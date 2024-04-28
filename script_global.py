"""Ce script est un script global qui permet la récupération des données de la
sonde météorologique soit depuis le début soit depuis la dernière TS
enregistrée. Et ce jusqu'à 00h00 du jour J"""

# 1 : Librairies et options
import datetime
import json
import os

import pandas as pd
import psycopg2
import requests
import tqdm
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.types import JSON, BigInteger, Integer

# 2 : Clés API et BDD via .env
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
    """Récupération de la date du jour à 00h00 en TS"""
    today = datetime.date.today()
    today_midnight = datetime.datetime.combine(today, datetime.time.min)
    end_date = int(today_midnight.timestamp())

    return end_date


def start_station():
    """Transformation de la date du début de la station en TS"""
    start_day = datetime.datetime(2021, 9, 29, 0, 0)
    start_day = int(start_day.timestamp())
    if_exists = "replace"  # informations pour la BDD

    return start_day, if_exists


def last_ts_bdd():
    """Récupération de la dernière TS dans la base de données"""
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


def start_api():
    """Choix de la TS de début à utiliser, en fonction de si la bdd est vide
    (historique) ou qu'elle contient déjà des données (routine)"""

    try:  # Présence d'une TS dans la table :
        start_date = last_ts_bdd()[0]
        if_exists = last_ts_bdd()[1]

    except psycopg2.ProgrammingError:  # Gérer l'erreur connexion BDD
        start_date = start_station()[0]
        if_exists = start_station()[1]

    return start_date, if_exists


# 4 : Ouverture de l'API
# DataFrame historiques :
df_ajout = pd.DataFrame()

# Start et End date :
start_date_api = start_api()[0]
end_date_api = today_ts()

# Nb de jours à récupérer :
nb_jours = int((end_date_api - start_date_api) / 86400)

for i in tqdm.tqdm(range(nb_jours)):
    start_time = start_date_api + i * 86400
    end_time = start_time + 86400

    # Lien de la request :
    link = (
        f"https://api.weatherlink.com/v2/historic/{station_ID}?"  # Base URL
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
        print(f"La requête {link} a échoué, code d'erreur : {r.status_code}")


# 5 : Transfert sur PostgreSQL
# Création de la chaîne de connexion PostgreSQL :
conn_str = f"postgresql://{user}:{password}@{host}/{database}"

# Création de la connexion à la base de données PostgreSQL :
engine = create_engine(conn_str)

# Définir les types de données pour chaque colonne :
dtype = {"station_id": Integer(), "ts": BigInteger(), "infos_json": JSON}

# Insérer le DataFrame dans la base de données PostgreSQL :
df_ajout.to_sql(
    nom_table,
    engine,
    if_exists=start_api()[1],
    index=False,
    dtype=dtype,
)

# Fermeture de la connexion :
engine.dispose()
