"""Script de récupération des données depuis le 1er jour de la sonde jusqu'a
00h00 du jour d'utilisation du script"""

# Librairies et options :
import datetime
import json
import os

import pandas as pd
import requests
import tqdm
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.types import JSON, BigInteger, Integer

# Clés API et BDD :
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


# Création des TS pour l'utilisation de l'option temps sur l'API :
# Date d'aujourd'hui à minuit :
today = datetime.date.today()
today_midnight = datetime.datetime.combine(today, datetime.time.min)

# Date du début de la station :
start_station = datetime.datetime(2021, 9, 29, 0, 0)

# Convertir la date en timestamp
start_station_timestamp = int(start_station.timestamp())
today_midnight_timestamp = int(today_midnight.timestamp())


# Récupération des données :
# DataFrame historiques :
df_historique = pd.DataFrame()

# Nb de jours à récupérer :
nb_jours = int((today_midnight_timestamp - start_station_timestamp) / 86400)

for i in tqdm.tqdm(range(nb_jours)):
    start_time = start_station_timestamp + i * 86400
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
        df_historique = pd.concat([df_historique, df_jour], ignore_index=True)
    else:
        print(f"La requête {link} a échoué, code d'erreur : {r.status_code}")


# Transfert sur PostgreSQL
# Création de la chaîne de connexion PostgreSQL :
conn_str = f"postgresql://{user}:{password}@{host}/{database}"

# Création de la connexion à la base de données PostgreSQL :
engine = create_engine(conn_str)

# Définir les types de données pour chaque colonne :
dtype = {"station_id": Integer(), "ts": BigInteger(), "infos_json": JSON}

# Insérer le DataFrame dans la base de données PostgreSQL :
df_historique.to_sql(
    nom_table,
    engine,
    if_exists="replace",
    index=False,
    dtype=dtype,
)

# Fermeture de la connexion :
engine.dispose()
