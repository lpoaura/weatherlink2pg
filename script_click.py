"""Cette application click est un script global qui permet la récupération
des données de la sonde météorologique soit depuis le début soit depuis
la dernière TS enregistrée. Et ce jusqu'à 00h00 du jour J"""

# 1 : Librairies et options
import datetime
import json
import os
import warnings

import click
import pandas as pd
import psycopg2
import requests
import tqdm
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.types import JSON, BigInteger, Integer

# Ignorer les avertissements FutureWarning : colonnes 100% NaN
warnings.filterwarnings("ignore", category=FutureWarning)


# 2 : Clés API et BDD via .env + url API
# Informations API : https://weatherlink.github.io/v2-api/

load_dotenv()

# Clés API :
API_key = os.getenv("API_key")
API_secret = os.getenv("API_secret")
station_ID = os.getenv("station_ID")

# Paramètres de connexion à la base de données PostgreSQL en local :
host = os.getenv("host")
database = os.getenv("database")
user = os.getenv("user")
password = os.getenv("password")
nom_table = os.getenv("nom_table")


# 3 : Définitions  :
def today_ts():
    """Récupération de la date du jour à 00h00 en TS pour utilisation comme
    date de fin avec l'API."""
    today = datetime.date.today()
    today_midnight = datetime.datetime.combine(today, datetime.time.min)
    end_date = int(today_midnight.timestamp())
    return end_date


def start_station():
    """Transformation de la date du début de la station en TS."""
    start_day = datetime.datetime(2021, 9, 29, 0, 0)
    start_day = int(start_day.timestamp())
    if_exists = "replace"  # informations pour la BDD
    return start_day, if_exists


def last_ts_bdd():
    """Récupération de la dernière TS enregistrée dans la base de données."""
    # Connexion à la base de données
    conn = psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
    )
    cur = conn.cursor()

    # Exécution d'une requête SQL et récupération de la TS :
    cur.execute(f"SELECT ts FROM {nom_table} ORDER BY ts DESC LIMIT 1")
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

        # Lien de la request :
        link = (
            f"https://api.weatherlink.com/v2/historic/{station_ID}?"  # URL
            f"api-key={API_key}&"  # Clé API
            f"start-timestamp={start_time}&"  # Timestamp de début
            f"end-timestamp={end_time}"  # Timestamp de fin
        )

        headers = {"X-Api-Secret": API_secret}

        # Requête :
        r = requests.get(link, headers=headers, timeout=60)

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

            # Convertir les objets JSON en chaînes de caractères JSON :
            df_jour["infos_json"] = df_jour["infos_json"].apply(json.dumps)

            # Concat des données :
            df_jour = pd.concat([df_jour, df_sensors], axis=1)

            # Concaténation des données :
            df_ajout = pd.concat([df_ajout, df_jour], ignore_index=True)
        else:
            print(f"La requête {link} a échoué, code erreur : {r.status_code}")

    return df_ajout


def up_to_bdd(df_ajout, if_exists):
    """Ajout des données dans la BDD."""
    # Connexion de la chaîne de connexion PostgreSQL :
    conn_str = f"postgresql://{user}:{password}@{host}/{database}"
    engine = create_engine(conn_str)

    # Définir les types de données pour chaque colonne :
    dtype = {"station_id": Integer(), "ts": BigInteger(), "infos_json": JSON}

    # Insérer le DataFrame dans la base de données PostgreSQL :
    df_ajout.to_sql(
        nom_table,
        engine,
        if_exists=if_exists,
        index=False,
        dtype=dtype,
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


# 4 : Utilisation de la routine de récupération des données via click :


@click.command()
@click.option(
    "--full",
    is_flag=True,
    help="""Option de récupération des donnes depuis le début de la sonde
    avec une réinitialisation de la table.""",
)
@click.option(
    "--update",
    is_flag=True,
    help="Option de mise à jour et d'ajout des données à la table.",
)
def main(full, update):
    """Permet à la fonction click de choisir entre récupérer toutes les données
    ou updater les nouvelles données."""

    if not (full or update):
        raise click.UsageError(
            """Vous devez spécifier une des options:
                               --full ou --update."""
        )

    if full:
        echo_success(
            """Le script de récupération totale des données
                     a été lancé avec succés."""
        )
        first_day_station, if_exists_bdd = start_station()
        end_api = today_ts()
        df_news = one_day_data(first_day_station, end_api)
        up_to_bdd(df_news, if_exists_bdd)
        echo_success("Le script s'est exécuté avec succès.")

    if update:
        echo_success(
            """Le script de mise à jour des données
                     a été lancé avec succès."""
        )
        last_ts, if_exists_bdd = last_ts_bdd()
        end_api = today_ts()
        df_news = one_day_data(last_ts, end_api)
        up_to_bdd(df_news, if_exists_bdd)
        echo_success("Le script s'est exécuté avec succès.")


if __name__ == "__main__":
    try:
        main()
    except click.UsageError as e:
        echo_failure(f"Erreur d'utilisation : {e}")
    except click.Abort:
        echo_failure("L'opération a été interrompue.")
