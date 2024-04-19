# 1 : Librairies et options
# Divers :
import pandas as pd
import datetime
import requests
import json
import tqdm

# Importer les codes depuis un fichier .env :
from dotenv import load_dotenv
import os
load_dotenv()

# Création et lecture des BDD postgresl :
from sqlalchemy import create_engine
import psycopg2
from sqlalchemy.types import Integer, BigInteger, JSON



# 2 : Clés API et BDD via .env
# Informations API : https://weatherlink.github.io/v2-api/
# Clés API :
APIKey = os.getenv("APIKey")
APISecret = os.getenv("APISecret")
stationID = os.getenv("stationID")

# Paramètres de connexion à la base de données PostgreSQL en local :
host = os.getenv("host")
database = os.getenv("database")
user = os.getenv("user")
password = os.getenv("password")
nomTable = os.getenv("nomTable")



# 3 : Définition pour la récupération du dernier TS de la table ou date du début de la sonde 
def startEndDateAPI () :
    
    # Enddate : aujourd'hui à minuit en TS :
    today = datetime.date.today()
    todayMidnight = datetime.datetime.combine(today, datetime.time.min)
    endDate = int(todayMidnight.timestamp())
    
    # Startdate : 1er jour de la sonde ou dernier TS enregistré dans la BDD :
    try : # Présence d'une TS dans la table :
        # Connexion à la base de données
        conn = psycopg2.connect(dbname = database, user = user, password = password , host = host)
        cur = conn.cursor()

        # Exécution d'une requête SQL et récupération de la TS : 
        cur.execute(f"SELECT ts FROM {nomTable} ORDER BY ts DESC LIMIT 1")
        data = cur.fetchall()
        startDate = pd.DataFrame(data, columns=[desc[0] for desc in cur.description]).values[0][0]
        ifExists = 'append' # informations pour la BDD

        # Fermeture du curseur et de la connexion
        cur.close()
        conn.close()
        
    except : # Pas de TS dans la table :
        # Date du début de la station en TS :
        startStation = datetime.datetime(2021, 9, 29, 0, 0)
        startDate = int(startStation.timestamp())
        ifExists = 'replace' # informations pour la BDD
        
    
    return startDate, endDate, ifExists


# 4 : Ouverture de l'API
# DataFrame historiques :
dfAjout = pd.DataFrame()

# Start et End date :
startDate = startEndDateAPI()[0]
endDate = startEndDateAPI()[1]

# Nb de jours à récupérer :
nbJours = int((endDate - startDate) / 86400)

for i in tqdm.tqdm(range(nbJours)):
    startTime = startDate + i * 86400
    endTime = startTime + 86400
    
    # Lien de la request : 
    link = 'https://api.weatherlink.com/v2/historic/{}?api-key={}&start-timestamp={}&end-timestamp={}'.format(stationID, APIKey, startTime, endTime)
    headers = {'X-Api-Secret' : APISecret}

    # Requête :
    r = requests.get(link, headers=headers)

    # Si la requête a réussi :
    if r.status_code == 200:
        # Lecture de la request en json :
        data = r.json()

        # Transformation en DF : 
        dfJour = pd.DataFrame(data)
        dfJour = dfJour[['station_id','sensors']]

        # Récupération des valeurs se trouvant dans sensors :
        dfSensors = pd.json_normalize(data['sensors'][0]['data'])
    
        # Récupération des json sur une colonne :
        dfJour = pd.DataFrame({'station_id': data['station_id'], 'infos_json' : data['sensors'][0]['data']})
    
        # Convertir les objets JSON en chaînes de caractères JSON :
        dfJour['infos_json'] = dfJour['infos_json'].apply(json.dumps)

        # Concat des données :
        dfJour = pd.concat([dfJour, dfSensors], axis=1)
        
        # Concaténation des données :
        dfAjout = pd.concat([dfAjout, dfJour], ignore_index=True)
    else:
        print("La requête {} a échoué avec le code d'erreur {}".format(link, r.status_code))



# 5 : Transfert sur PostgreSQL
# Création de la chaîne de connexion PostgreSQL :
connStr = f"postgresql://{user}:{password}@{host}/{database}"

# Création de la connexion à la base de données PostgreSQL :
engine = create_engine(connStr)

# Définir les types de données pour chaque colonne :
dtype = {'station_id': Integer(),
         'ts': BigInteger(),
         'infos_json': JSON}

# Insérer le DataFrame dans la base de données PostgreSQL :
dfAjout.to_sql(nomTable, engine, if_exists = startEndDateAPI()[2] , index=False, dtype=dtype)

# Fermeture de la connexion :
engine.dispose()