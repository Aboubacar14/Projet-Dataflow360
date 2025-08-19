# import requests
# from hdfs import InsecureClient
# import csv
# import time
# import sys

# # --------------------------
# # Constantes globales
# # --------------------------
# MAX_RETRIES = 10
# RETRY_DELAY = 10
# TIMEOUT_API = 30  # Timeout par défaut pour les requêtes
# HDFS_URL = "http://namenode:9870"
# HDFS_USER = "hadoop"
# BASE_URL = "http://api:8000"
# USERNAME = "aboubacar14"
# PASSWORD = "1410"
# ENDPOINTS = [
#     "eleves", "enseignants", "notes", "presences",
#     "matiere", "regions", "etablissements", "cours"
# ]
# HDFS_PATH = "/data/datalake/"


# def log(level, message):
#     """Affiche un log formaté."""
#     print(f"[{level.upper()}] {message}")


# def request_with_retry(method, url, **kwargs):
#     """Envoie une requête HTTP avec gestion des erreurs."""
#     try:
#         response = requests.request(method, url, timeout=TIMEOUT_API, **kwargs)
#         response.raise_for_status()
#         return response
#     except requests.exceptions.RequestException as e:
#         log("error", f"Requête échouée vers {url} : {e}")
#         return None


# def authenticate():
#     """Authentifie l'utilisateur et retourne le token."""
#     for attempt in range(1, MAX_RETRIES + 1):
#         log("info", f"Tentative {attempt}/{MAX_RETRIES} d'authentification...")
#         response = request_with_retry(
#             "POST",
#             f"{BASE_URL}/utilisateurs/login",
#             params={"username": USERNAME, "password": PASSWORD}
#         )
#         if response and response.status_code == 200:
#             token = response.json().get("access_token")
#             if token:
#                 log("info", "Authentification réussie ✅")
#                 return token
#         time.sleep(RETRY_DELAY)

#     log("fatal", "Impossible de s'authentifier après plusieurs tentatives.")
#     sys.exit(1)


# def get_data(endpoint, token):
#     """Récupère les données JSON depuis l'API."""
#     response = request_with_retry(
#         "GET",
#         f"{BASE_URL}/{endpoint}",
#         headers={"Authorization": f"Bearer {token}"}
#     )
#     if response:
#         data = response.json()
#         log("info", f"{endpoint} : {len(data)} enregistrements récupérés.")
#         return data
#     return []


# def write_to_hdfs(client, path, records):
#     """Écrit une liste de dictionnaires en CSV dans HDFS."""
#     if not records:
#         log("warn", f"Aucune donnée à écrire dans {path}")
#         return
#     try:
#         with client.write(path, overwrite=True, encoding="utf-8") as writer:
#             writer_csv = csv.DictWriter(writer, fieldnames=records[0].keys())
#             writer_csv.writeheader()
#             writer_csv.writerows(records)
#         log("info", f"{path} écrit avec succès.")
#     except Exception as e:
#         log("error", f"Erreur écriture HDFS {path} : {e}")


# def main():
#     token = authenticate()

#     log("info", "Extraction des données en cours...")
#     data_map = {ep: get_data(ep, token) for ep in ENDPOINTS}

#     client = InsecureClient(url=HDFS_URL, user=HDFS_USER)

#     for key, data in data_map.items():
#         write_to_hdfs(client, f"{HDFS_PATH}{key}_api.csv", data)

#     log("info", "ETL terminé ✅")


# if __name__ == "__main__":
#     main()

# =============================================================================
# SCRIPT EXTRACTION API - VERSION FRANÇAISE
# Fichier: extraire_api.py
# =============================================================================

import requests
from hdfs import InsecureClient
import csv
import time
import sys
import os
from datetime import datetime

# --------------------------
# Constantes globales
# --------------------------
NOMBRE_MAX_TENTATIVES = 10
DELAI_ENTRE_TENTATIVES = 10
TIMEOUT_API = 30  # Timeout par défaut pour les requêtes
URL_HDFS = "http://namenode:9870"
UTILISATEUR_HDFS = "hadoop"
URL_BASE_API = "http://api:8000"
NOM_UTILISATEUR = "aboubacar14"
MOT_DE_PASSE = "1410"
POINTS_TERMINAISON = [
    "eleves", "enseignants", "notes", "presences",
    "matiere", "regions", "etablissements", "cours"
]
CHEMIN_HDFS_API = "/data/datalake/donnees_api"

# Variable globale pour stocker les données API
donnees_api = {}

def journaliser(niveau, message):
    """Affiche un log formaté."""
    print(f"[{niveau.upper()}] {message}")

def requete_avec_tentatives(methode, url, **kwargs):
    """Envoie une requête HTTP avec gestion des erreurs."""
    try:
        reponse = requests.request(methode, url, timeout=TIMEOUT_API, **kwargs)
        reponse.raise_for_status()
        return reponse
    except requests.exceptions.RequestException as e:
        journaliser("error", f"Requête échouée vers {url} : {e}")
        return None

def s_authentifier():
    """Authentifie l'utilisateur et retourne le token."""
    for tentative in range(1, NOMBRE_MAX_TENTATIVES + 1):
        journaliser("info", f"Tentative {tentative}/{NOMBRE_MAX_TENTATIVES} d'authentification...")
        reponse = requete_avec_tentatives(
            "POST",
            f"{URL_BASE_API}/utilisateurs/login",
            params={"username": NOM_UTILISATEUR, "password": MOT_DE_PASSE}
        )
        if reponse and reponse.status_code == 200:
            token = reponse.json().get("access_token")
            if token:
                journaliser("info", "Authentification réussie ✅")
                return token
        time.sleep(DELAI_ENTRE_TENTATIVES)

    journaliser("fatal", "Impossible de s'authentifier après plusieurs tentatives.")
    sys.exit(1)

def obtenir_donnees(point_terminaison, token):
    """Récupère les données JSON depuis l'API."""
    reponse = requete_avec_tentatives(
        "GET",
        f"{URL_BASE_API}/{point_terminaison}",
        headers={"Authorization": f"Bearer {token}"}
    )
    if reponse:
        donnees = reponse.json()
        journaliser("info", f"{point_terminaison} : {len(donnees)} enregistrements récupérés.")
        return donnees
    return []

def connecter_hdfs_avec_tentatives():
    """Connexion à HDFS avec tentatives multiples."""
    for tentative in range(1, NOMBRE_MAX_TENTATIVES + 1):
        try:
            journaliser("info", f"Tentative {tentative}/{NOMBRE_MAX_TENTATIVES} de connexion à HDFS...")
            client_hdfs = InsecureClient(URL_HDFS, user=UTILISATEUR_HDFS)
            # Test de connexion
            client_hdfs.list("/")
            journaliser("info", "Connexion à HDFS réussie ✅")
            return client_hdfs
        except Exception as e:
            journaliser("error", f"Connexion HDFS échouée : {e}")
        
        if tentative < NOMBRE_MAX_TENTATIVES:
            journaliser("info", f"Nouvelle tentative dans {DELAI_ENTRE_TENTATIVES}s...")
            time.sleep(DELAI_ENTRE_TENTATIVES)
    
    journaliser("warn", "Impossible de se connecter à HDFS après plusieurs tentatives.")
    return None

def ecrire_vers_hdfs(client, chemin, enregistrements):
    """Écrit une liste de dictionnaires en CSV dans HDFS."""
    if not enregistrements:
        journaliser("warn", f"Aucune donnée à écrire dans {chemin}")
        return
    try:
        with client.write(chemin, overwrite=True, encoding="utf-8") as writer:
            writer_csv = csv.DictWriter(writer, fieldnames=enregistrements[0].keys())
            writer_csv.writeheader()
            writer_csv.writerows(enregistrements)
        journaliser("info", f"{chemin} écrit avec succès.")
    except Exception as e:
        journaliser("error", f"Erreur écriture HDFS {chemin} : {e}")

# =============================================================================
# FONCTIONS PRINCIPALES D'EXTRACTION
# =============================================================================

def extraire_donnees_api():
    """Fonction principale pour extraire les données de l'API et les stocker dans donnees_api."""
    global donnees_api
    
    journaliser("info", "Démarrage de l'extraction API")
    horodatage = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Authentification
    token = s_authentifier()

    # Extraction des données et stockage dans la variable globale
    journaliser("info", "Extraction des données API en cours...")
    for point_terminaison in POINTS_TERMINAISON:
        donnees_api[point_terminaison] = obtenir_donnees(point_terminaison, token)

    # Résumé
    journaliser("info", f"\n=== RÉSUMÉ EXTRACTION API ===")
    journaliser("info", f"Horodatage: {horodatage}")
    journaliser("info", f"Données stockées dans la variable 'donnees_api':")
    
    for cle, donnees in donnees_api.items():
        journaliser("info", f"- {cle}: {len(donnees)} enregistrements")
    
    journaliser("info", "✅ Extraction API terminée avec succès")
    return donnees_api

def sauvegarder_donnees_api_vers_hdfs():
    """Sauvegarde les données API vers HDFS."""
    if not donnees_api:
        journaliser("warn", "Aucune donnée API à sauvegarder vers HDFS")
        return
    
    # Connexion HDFS
    client_hdfs = connecter_hdfs_avec_tentatives()
    if not client_hdfs:
        journaliser("error", "Impossible de sauvegarder vers HDFS - pas de connexion")
        return
    
    # Créer le répertoire de base s'il n'existe pas
    try:
        client_hdfs.makedirs(CHEMIN_HDFS_API)
        journaliser("info", f"Répertoire HDFS créé : {CHEMIN_HDFS_API}")
    except Exception as e:
        if "already exists" in str(e).lower():
            journaliser("info", f"Répertoire HDFS existe déjà : {CHEMIN_HDFS_API}")
        else:
            journaliser("warn", f"Erreur création répertoire HDFS : {e}")
    
    horodatage = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Sauvegarder chaque table
    for nom_table, donnees in donnees_api.items():
        nom_fichier = f"{nom_table}_api_{horodatage}.csv"
        chemin_hdfs_fichier = f"{CHEMIN_HDFS_API}/{nom_fichier}"
        ecrire_vers_hdfs(client_hdfs, chemin_hdfs_fichier, donnees)
    
    journaliser("info", f"✅ Sauvegarde HDFS terminée vers {CHEMIN_HDFS_API}")

# =============================================================================
# FONCTIONS UTILITAIRES POUR API
# =============================================================================

def obtenir_table_api(nom_table):
    """Récupère une table spécifique depuis les données API."""
    return donnees_api.get(nom_table, [])

def sauvegarder_api_vers_csv(prefixe_fichier="api"):
    """Sauvegarde les données API en fichiers CSV locaux."""
    if not donnees_api:
        journaliser("warn", "Aucune donnée API à sauvegarder")
        return
    
    horodatage = datetime.now().strftime("%Y%m%d_%H%M%S")
    repertoire_sortie = f"./sortie_{prefixe_fichier}_{horodatage}"
    os.makedirs(repertoire_sortie, exist_ok=True)
    
    for nom_table, enregistrements in donnees_api.items():
        if enregistrements:
            chemin_fichier = os.path.join(repertoire_sortie, f"{nom_table}_{prefixe_fichier}.csv")
            with open(chemin_fichier, 'w', newline='', encoding='utf-8') as fichier_csv:
                writer = csv.DictWriter(fichier_csv, fieldnames=enregistrements[0].keys())
                writer.writeheader()
                writer.writerows(enregistrements)
            journaliser("info", f"Sauvegardé: {chemin_fichier}")

def afficher_resume_api():
    """Affiche un résumé des données API chargées."""
    journaliser("info", "\n=== RÉSUMÉ DES DONNÉES API ===")
    if not donnees_api:
        journaliser("info", "Aucune donnée API chargée")
        return
    
    for table, donnees in donnees_api.items():
        journaliser("info", f"- {table}: {len(donnees)} enregistrements")

def vider_donnees_api():
    """Vide les données API de la mémoire."""
    global donnees_api
    donnees_api.clear()
    journaliser("info", "Données API effacées de la mémoire")

def obtenir_liste_points_terminaison():
    """Retourne la liste des points de terminaison disponibles."""
    return POINTS_TERMINAISON.copy()

def recharger_point_terminaison(nom_point_terminaison, token=None):
    """Recharge un point de terminaison spécifique."""
    if nom_point_terminaison not in POINTS_TERMINAISON:
        journaliser("error", f"Point de terminaison '{nom_point_terminaison}' non reconnu")
        return False
    
    if not token:
        token = s_authentifier()
    
    journaliser("info", f"Rechargement du point de terminaison: {nom_point_terminaison}")
    donnees_api[nom_point_terminaison] = obtenir_donnees(nom_point_terminaison, token)
    return True

def valider_donnees_api():
    """Valide la cohérence des données extraites."""
    journaliser("info", "\n=== VALIDATION DES DONNÉES API ===")
    
    if not donnees_api:
        journaliser("warn", "Aucune donnée à valider")
        return False
    
    validation_ok = True
    
    for nom_table, donnees in donnees_api.items():
        if not donnees:
            journaliser("warn", f"Table '{nom_table}' est vide")
            continue
            
        # Vérifier que tous les enregistrements ont les mêmes clés
        if len(donnees) > 1:
            premieres_cles = set(donnees[0].keys())
            for i, enregistrement in enumerate(donnees[1:], 1):
                if set(enregistrement.keys()) != premieres_cles:
                    journaliser("error", f"Table '{nom_table}': structure incohérente à l'enregistrement {i}")
                    validation_ok = False
        
        journaliser("info", f"Table '{nom_table}': {len(donnees)} enregistrements, {len(donnees[0].keys()) if donnees else 0} colonnes")
    
    if validation_ok:
        journaliser("info", "✅ Validation réussie")
    else:
        journaliser("error", "❌ Erreurs de validation détectées")
    
    return validation_ok

def obtenir_toutes_donnees_api():
    """Retourne toutes les données API."""
    return donnees_api.copy()

def programme_principal():
    """Programme principal avec sauvegarde HDFS."""
    # Extraction des données
    extraire_donnees_api()
    
    # Validation des données
    valider_donnees_api()
    
    # Sauvegarde vers HDFS
    sauvegarder_donnees_api_vers_hdfs()
    
    # Affichage du résumé
    afficher_resume_api()

def programme_principal_ancien():
    """Ancienne fonction principale - maintient la compatibilité."""
    token = s_authentifier()

    journaliser("info", "Extraction des données en cours...")
    carte_donnees = {pt: obtenir_donnees(pt, token) for pt in POINTS_TERMINAISON}

    client = connecter_hdfs_avec_tentatives()
    if client:
        for cle, donnees in carte_donnees.items():
            ecrire_vers_hdfs(client, f"/data/datalake/{cle}_api.csv", donnees)

    journaliser("info", "ETL terminé ✅")

# =============================================================================
# EXEMPLE D'UTILISATION
# =============================================================================

if __name__ == "__main__":
    # Programme principal avec variables et sauvegarde HDFS
    programme_principal()
    
    # Exemples d'accès aux données après extraction
    journaliser("info", "\n=== EXEMPLES D'ACCÈS AUX DONNÉES API ===")
    
    # Accéder à une table spécifique
    eleves = obtenir_table_api("eleves")
    journaliser("info", f"Élèves: {len(eleves)} enregistrements")
    
    if eleves and len(eleves) > 0:
        journaliser("info", f"Premier élève: {eleves[0]}")
    
    # Lister tous les points de terminaison disponibles
    points_terminaison_disponibles = obtenir_liste_points_terminaison()
    journaliser("info", f"Points de terminaison disponibles: {points_terminaison_disponibles}")
    
    # Accéder à toutes les données
    journaliser("info", f"Tables chargées: {list(donnees_api.keys())}")
    
    # Exemple de traitement des données
    for nom_table, enregistrements in donnees_api.items():
        if enregistrements:
            journaliser("info", f"Table {nom_table}: {len(enregistrements)} enregistrements, {len(enregistrements[0].keys())} colonnes")
            # Afficher les colonnes disponibles
            colonnes = list(enregistrements[0].keys())
            colonnes_affichees = ', '.join(colonnes[:5]) + ('...' if len(colonnes) > 5 else '')
            journaliser("info", f"  Colonnes: {colonnes_affichees}")
    
    # Sauvegarder en CSV local si nécessaire (optionnel)
    # sauvegarder_api_vers_csv()