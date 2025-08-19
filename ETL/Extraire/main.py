from extraction_api import programme_principal 
import extractionneo4j as neo

programme_principal()
neo.programme_principal()
# # --------------------------
# # Paramètres de l'API
# # --------------------------
# base_url = "http://api:8000"     # URL de base de l'API
# username = "aboubacar14"            # Nom d'utilisateur
# password = "1410"                   # Mot de passe

# # --------------------------
# # 1. Authentification robuste avec retry
# # --------------------------
# token = authenticate_with_retry(base_url, username, password)

# # --------------------------
# # 2. Liste des endpoints à extraire (Extraction)
# # --------------------------
# endpoints = [
#     "eleves", "enseignants", "notes", "presences",
#     "matiere", "regions", "etablissements", "cours"
# ]

# print("[INFO] Récupération des données en cours...")

# # Récupérer toutes les données et les stocker dans un dictionnaire
# # data_map["eleves"] => liste des élèves
# data_map = {endpoint: get_data(endpoint, token, base_url) for endpoint in endpoints}

# # --------------------------
# # 3. Connexion à HDFS
# # --------------------------
# client = InsecureClient(url="http://namenode:9870", user="hadoop")
# chemin_principal = "/data/datalake/"   # Dossier principal dans HDFS

# # --------------------------
# # 4. Stockage des données dans HDFS (Loading)
# # --------------------------
# for key, data in data_map.items():
#     stocke_data_datalake(client, chemin_principal + f"{key}_api.csv", data)

# print("[INFO] ETL terminé ")