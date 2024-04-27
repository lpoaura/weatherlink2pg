"""Script de récupération des données depuis la dernière TS de la BDD/Table
 jusqu'a 00h00 du jour d'utilisation du script"""

# Librairies et options :
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


# Récupération de la TS la plus récente dans la table :
# Connexion à la base de données
conn = psycopg2.connect(
    dbname=database,
    user=user,
    password=password,
    host=host,
)

# Création d'un curseur : permet d'exécuter des commandes SQL sur BDD.
cur = conn.cursor()

# Exécution d'une requête SQL pour sélectionner les données de ma_table
cur.execute("SELECT ts FROM historiquemeteo ORDER BY ts DESC LIMIT 1")

# Récupération des données dans une liste de tuples
data = cur.fetchall()

# Création d'un DataFrame à partir des données
df = pd.DataFrame(data, columns=[desc[0] for desc in cur.description])

# Fermeture du curseur et de la connexion
cur.close()
conn.close()

# Récupération de la ts la plus récente de la BDD :
last_load_timestamp = df.values[0][0]


# Ouverture de l'API et récupération des données :
# Informations : https://weatherlink.github.io/v2-api/

# Création du TS de ce matin à 00h00 :
today = datetime.date.today()
today_midnight = datetime.datetime.combine(today, datetime.time.min)
today_midnight_timestamp = int(today_midnight.timestamp())

# DataFrame nouvelles données :
df_new = pd.DataFrame()

# Nb de jours à récupérer :
nb_jours = int((today_midnight_timestamp - last_load_timestamp) / 86400)

for i in tqdm.tqdm(range(nb_jours)):
    start_time = last_load_timestamp + i * 86400
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
        df_new = pd.concat([df_new, df_jour], ignore_index=True)
    else:
        print(f"La requête {link} a échoué, code d'erreur : {r.status_code}")


# Transfert nouvelles données sur PostgreSQL :
# Création de la chaîne de connexion PostgreSQL :
conn_str = f"postgresql://{user}:{password}@{host}/{database}"

# Création de la connexion à la base de données PostgreSQL :
engine = create_engine(conn_str)

# Définir les types de données pour chaque colonne :
dtype = {"station_id": Integer(), "ts": BigInteger(), "infos_json": JSON}

# Ajouter le DataFrame dans la base de données PostgreSQL :
df_new.to_sql(nom_table, engine, if_exists="append", index=False, dtype=dtype)

# Fermeture de la connexion :
engine.dispose()
