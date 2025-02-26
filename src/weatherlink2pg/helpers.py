"""Script regroupant les différentes définitions utilisées pour l'application
click ainsi que les clés API et BDD."""

# 1 : Librairies et options
import datetime
import os
import urllib.parse
from typing import Optional

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
# station_ID = os.getenv("WEATHERLINK2PG_API_STATIONID")

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
    echo_success(f"CREATE SCHEMA {schema_name}")
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
    cur.execute(f"""SELECT ts FROM {relation} """ "ORDER BY ts DESC LIMIT 1")
    data_extract = cur.fetchall()
    last_ts = pd.DataFrame(
        data_extract, columns=[desc[0] for desc in cur.description]
    ).values[0][0]
    if_exists = "append"  # informations pour la BDD

    # Fermeture du curseur et de la connexion
    cur.close()
    conn.close()

    return last_ts, if_exists


def list_stations() -> Optional[list[dict]]:
    """Download stations main informations"""
    root_url = "https://api.weatherlink.com/v2/stations?"
    params = {
        "api-key": API_key,
    }
    url = root_url + urllib.parse.urlencode(params)

    headers = {"X-Api-Secret": API_secret}

    # Requête :
    r = requests.get(url, headers=headers, timeout=60)
    if r.status_code == 200:
        data = r.json()
        return data["stations"]
    echo_failure(
        f"La requête {url} a échoué, code erreur :"
        f" {r.status_code} / {r.json()}"
    )
    return None


def get_stations_infos() -> None:
    "Display stations main informations"
    stations = list_stations()
    output = [
        f"""ID: {s["station_id"]} - NAME : {s['station_name']} """
        f"""({s['city']} - {s['longitude']}/{s['latitude']})"""
        for s in stations
    ]
    echo_info(str("\n".join(output)))


def get_station_ids() -> Optional[list[int]]:
    """Get available stations id"""

    stations = list_stations()

    # Requête :
    station_ids = [str(station["station_id"]) for station in stations]
    echo_info(f"Existing stations ids are {', '.join(station_ids)}")
    return station_ids


def fetch_station_data(
    station: int, start_time: int, end_time: int
) -> pd.DataFrame:
    """Fetch data for a specific station and time range."""
    root_url = f"https://api.weatherlink.com/v2/historic/{station}?"
    params = {
        "api-key": API_key,
        "start-timestamp": start_time,
        "end-timestamp": end_time,
    }
    url = root_url + urllib.parse.urlencode(params)
    headers = {"X-Api-Secret": API_secret}

    r = requests.get(url, headers=headers, timeout=60)
    if r.status_code == 200:
        return r.json()
    echo_failure(
        f"La requête {url} a échoué, code erreur :"
        f" {r.status_code} / {r.json()}"
    )
    return None


def process_station_data(data: dict) -> pd.DataFrame:
    """Process the JSON data for a station into a DataFrame."""
    df_jour = pd.DataFrame(
        {
            "station_id": data["station_id"],
            "infos_json": data["sensors"][0]["data"],
        }
    )
    df_sensors = pd.json_normalize(data["sensors"][0]["data"])
    return pd.concat([df_jour, df_sensors], axis=1)


def one_day_data(
    stations: list[int], start_date_api: int, end_date_api: int
) -> pd.DataFrame:
    """Récupération des données jour/jour via l'API et optention d'une DF."""
    df_ajout = pd.DataFrame()
    nb_jours = int((end_date_api - start_date_api) / 86400)

    for station in stations:
        echo_info(f"Downloading data from station {station}")
        for i in tqdm.tqdm(range(nb_jours)):
            start_time = start_date_api + i * 86400
            end_time = start_time + 86400

            data = fetch_station_data(station, start_time, end_time)
            if data:
                df_jour = process_station_data(data)
                df_ajout = pd.concat([df_ajout, df_jour], ignore_index=True)

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


def echo_info(message):
    """Decore pour le succes du programme click."""
    click.echo(
        click.style(
            message.replace("\n                     ", ""),
            fg="blue",
        )
    )


def echo_default(message):
    """Decore pour le succes du programme click."""
    click.echo(
        click.style(
            message.replace("\n                     ", ""),
            fg="white",
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
