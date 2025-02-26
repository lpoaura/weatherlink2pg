"""Script regroupant les différentes définitions utilisées pour l'application
click ainsi que les clés API et BDD."""

# 1 : Librairies et options
import datetime
import os
import urllib.parse
import click
import pandas as pd
import psycopg2
import requests
import tqdm
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import BIGINT, INTEGER, JSONB

# 2 : Clés API et BDD via .env + url API
# Informations API : https://weatherlink.github.io/v2-api/

load_dotenv()

# Clés API :
API_key = os.getenv("WEATHERLINK2PG_API_KEY")
API_secret = os.getenv("WEATHERLINK2PG_API_SECRET")
station_ID = os.getenv("WEATHERLINK2PG_API_STATIONID")

# Paramètres de connexion à la base de données PostgreSQL en local :
host = os.getenv("WEATHERLINK2PG_PG_HOST", default="localhost")
database = os.getenv("WEATHERLINK2PG_PG_DATABASE", default="weatherlink")
port = os.getenv("WEATHERLINK2PG_PG_PORT", default="5432")
user = os.getenv("WEATHERLINK2PG_PG_USER")
password = os.getenv("WEATHERLINK2PG_PG_PWD")
table_name = os.getenv("WEATHERLINK2PG_PG_TABLE", default="data")
schema_name = os.getenv("WEATHERLINK2PG_PG_SCHEMA", default=None)
relation = f"{schema_name}.{table_name}" if schema_name else table_name


# 3 : Définitions  :
def today_ts():
    """Récupération de la date du jour à 00h00 en TS pour utilisation comme
    date de fin avec l'API."""
    today = datetime.date.today()
    today_midnight = datetime.datetime.combine(today, datetime.time.min)
    end_date = int(today_midnight.timestamp())
    return end_date


def start_station(since: str):
    """Transformation de la date du début de la station en TS."""
    since = since if since else "2021-09-29"
    start_day = int(datetime.datetime.strptime(since, "%Y-%m-%d").timestamp())
    if_exists = "replace"  # informations pour la BDD
    return start_day, if_exists


def create_schema():
    """Create data schema if defined"""
    echo_success("CREATE SCHEMA")
    conn = psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    cur = conn.cursor()

    # Exécution d'une requête SQL et récupération de la TS :
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    conn.commit()
    # Fermeture du curseur et de la connexion
    cur.close()
    conn.close()


def last_ts_bdd():
    """Récupération de la dernière TS enregistrée dans la base de données."""
    # Connexion à la base de données
    conn = psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    cur = conn.cursor()
    # Exécution d'une requête SQL et récupération de la TS :
    cur.execute(f"SELECT ts FROM {relation} ORDER BY ts DESC LIMIT 1")
    data_extract = cur.fetchall()
    last_ts = pd.DataFrame(
        data_extract, columns=[desc[0] for desc in cur.description]
    ).values[0][0]
    if_exists = "append"  # informations pour la BDD

    # Fermeture du curseur et de la connexion
    cur.close()
    conn.close()

    return last_ts, if_exists


def one_day_data(start_date_api, end_date_api):
    """Récupération des données jour/jour via l'API et optention d'une DF."""
    # DataFrame historiques :
    df_ajout = pd.DataFrame()

    # Nb de jours à récupérer :
    nb_jours = int((end_date_api - start_date_api) / 86400)

    for i in tqdm.tqdm(range(nb_jours)):
        start_time = start_date_api + i * 86400
        end_time = start_time + 86400

        root_url = f"https://api.weatherlink.com/v2/historic/{station_ID}?"
        params = {'api-key': API_key, 'start-timestamp': start_time, 'end-timestamp': end_time}
        url = root_url + urllib.parse.urlencode(params)

        headers = {"X-Api-Secret": API_secret}

        # Requête :
        r = requests.get(url, headers=headers, timeout=60)

        # Si la requête a réussi :
        if r.status_code == 200:
            # Lecture de la request en json :
            data = r.json()

            # Transformation en DF :
            df_jour = pd.DataFrame(data)
            df_jour = df_jour[["station_id", "sensors"]]

            # Récupération des valeurs se trouvant dans sensors :
            df_sensors = pd.json_normalize(data["sensors"][0]["data"])

            # Récupération des json sur une colonne :
            df_jour = pd.DataFrame(
                {
                    "station_id": data["station_id"],
                    "infos_json": data["sensors"][0]["data"],
                }
            )

            # Concat des données :
            df_jour = pd.concat([df_jour, df_sensors], axis=1)

            # Concaténation des données :
            df_ajout = pd.concat([df_ajout, df_jour], ignore_index=True)
        else:
            echo_failure(
                f"La requête {url} a échoué, code erreur : {r.status_code} / {r.json()}"
            )

    return df_ajout


def up_to_bdd(df_ajout, if_exists):
    """Ajout des données dans la BDD."""
    # Connexion de la chaîne de connexion PostgreSQL :
    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn_str)

    # Définir les types de données pour chaque colonne :
    dtype = {"station_id": INTEGER, "ts": BIGINT, "infos_json": JSONB}
    if schema_name is not None:
        create_schema()
    # Insérer le DataFrame dans la base de données PostgreSQL :
    df_ajout.to_sql(
        table_name,
        engine,
        if_exists=if_exists,
        index=False,
        dtype=dtype,
        schema=schema_name,
    )

    # Fermeture de la connexion :
    engine.dispose()


def echo_success(message):
    """Decore pour le succes du programme click."""
    click.echo(
        click.style(
            message.replace("\n                     ", ""),
            fg="green",
        )
    )


def echo_failure(message):
    """Décore en cas d'échéc du programme click."""
    click.echo(
        click.style(
            message.replace("\n                     ", ""),
            fg="red",
        )
    )
