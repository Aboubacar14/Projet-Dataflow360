# # --------------------------
# # Import des librairies
# # --------------------------
# from py2neo import Graph, Node, Relationship
# from hdfs import InsecureClient
# import csv
# import time
# import os
# from datetime import datetime
# import random

# # ------------------------------------
# # Paramètres globaux pour le retry
# # ----------------------------------
# MAX_RETRIES = 10
# RETRY_DELAY = 10

# # --------------------------
# # Configuration Neo4j
# # --------------------------
# NEO4J_URI = "bolt://neo4j:7687"
# NEO4J_USER = "neo4j"
# NEO4J_PASSWORD = "12345678"

# # --------------------------
# # Configuration HDFS
# # --------------------------
# HDFS_URL = "http://namenode:9870"
#   # Port NameNode Web UI
# HDFS_USER = "hadoop"

# # ----------------------------------------------------------
# # Fonction : Connexion à Neo4j avec retry
# # ----------------------------------------------------------
# def connect_neo4j_with_retry():
#     for attempt in range(1, MAX_RETRIES + 1):
#         try:
#             print(f"[INFO] Tentative {attempt}/{MAX_RETRIES} de connexion à Neo4j...")
#             graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
#             if graph.run("RETURN 1 as test").data()[0]["test"] == 1:
#                 print("[INFO] Connexion à Neo4j réussie ✅")
#                 return graph
#         except Exception as e:
#             print(f"[ERROR] Connexion Neo4j échouée : {e}")
        
#         if attempt < MAX_RETRIES:
#             print(f"[INFO] Nouvelle tentative dans {RETRY_DELAY}s...")
#             time.sleep(RETRY_DELAY)
    
#     print("[FATAL] Impossible de se connecter à Neo4j après plusieurs tentatives.")
#     exit(1)

# # ----------------------------------------------------------
# # Fonction : Connexion à HDFS avec retry
# # ----------------------------------------------------------
# def connect_hdfs_with_retry():
#     for attempt in range(1, MAX_RETRIES + 1):
#         try:
#             print(f"[INFO] Tentative {attempt}/{MAX_RETRIES} de connexion à HDFS...")
#             hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)
#             # Test de connexion
#             hdfs_client.list("/")
#             print("[INFO] Connexion à HDFS réussie ✅")
#             return hdfs_client
#         except Exception as e:
#             print(f"[ERROR] Connexion HDFS échouée : {e}")
        
#         if attempt < MAX_RETRIES:
#             print(f"[INFO] Nouvelle tentative dans {RETRY_DELAY}s...")
#             time.sleep(RETRY_DELAY)
    
#     print("[WARN] Impossible de se connecter à HDFS après plusieurs tentatives.")
#     return None

# # ----------------------------------------------------------
# # Fonction : Exécuter une requête Cypher et récupérer les données
# # ----------------------------------------------------------
# def get_neo4j_data(graph, query, query_name):
#     try:
#         print(f"[INFO] Exécution de la requête : {query_name}")
#         result = graph.run(query)
#         data = result.data()
#         print(f"[INFO] Données récupérées pour {query_name} : {len(data)} éléments.")
#         return data
#     except Exception as e:
#         print(f"[ERROR] Erreur lors de l'exécution de {query_name} : {e}")
#         return []

# # --------------------------
# # Fonction : Sauvegarde locale en CSV
# # --------------------------
# def save_data_locally(output_dir, filename, contenu):
#     if not contenu:
#         print(f"[WARN] Aucune donnée à écrire pour {filename}")
#         return False, None
#     try:
#         os.makedirs(output_dir, exist_ok=True)
#         filepath = os.path.join(output_dir, filename)
#         with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
#             writer_csv = csv.DictWriter(csvfile, fieldnames=contenu[0].keys())
#             writer_csv.writeheader()
#             writer_csv.writerows(contenu)
#         print(f"[INFO] Données sauvegardées localement : {filepath}")
#         return True, filepath
#     except Exception as e:
#         print(f"[ERROR] Erreur sauvegarde locale {filename} : {e}")
#         return False, None

# # --------------------------
# # Fonction : Créer le répertoire HDFS s'il n'existe pas
# # --------------------------
# def create_hdfs_directory(hdfs_client, path):
#     try:
#         hdfs_client.makedirs(path)
#         print(f"[INFO] Répertoire HDFS créé : {path}")
#     except Exception as e:
#         if "already exists" in str(e).lower():
#             print(f"[INFO] Répertoire HDFS existe déjà : {path}")
#         else:
#             print(f"[WARN] Erreur création répertoire HDFS {path} : {e}")

# # --------------------------
# # Fonction : Upload vers HDFS
# # --------------------------
# def upload_to_hdfs(hdfs_client, local_file, hdfs_path):
#     try:
#         # Créer le répertoire parent s'il n'existe pas
#         parent_dir = os.path.dirname(hdfs_path)
#         if parent_dir != "/":
#             create_hdfs_directory(hdfs_client, parent_dir)
        
#         # Upload du fichier
#         hdfs_client.upload(hdfs_path, local_file, overwrite=True)
#         print(f"[INFO] Fichier uploadé vers HDFS : {hdfs_path}")
#         return True
#     except Exception as e:
#         print(f"[ERROR] Erreur upload HDFS {hdfs_path} : {e}")
#         return False

# # --------------------------
# # Définition des requêtes Cypher
# # --------------------------
# def get_cypher_queries():
#     return {
#         "regions": """
#             MATCH (r:Region)
#             RETURN r.id_region as id_region, 
#                    r.nom_region as nom_region, 
#                    r.ville as ville
#             ORDER BY r.id_region
#         """,
#         "etablissements": """
#             MATCH (e:Etablissement)-[:APPARTIENT_A]->(r:Region)
#             RETURN e.id_etablissement as id_etablissement,
#                    e.nom_etablissement as nom_etablissement,
#                    e.type as type,
#                    e.statut as statut,
#                    e.id_region as id_region,
#                    r.nom_region as nom_region,
#                    r.ville as ville
#             ORDER BY e.id_etablissement
#         """,
#         "eleves": """
#             MATCH (eleve:Eleve)-[:INSCRIT_A]->(etab:Etablissement)
#             RETURN eleve.id_eleve as id_eleve,
#                    eleve.nom as nom,
#                    eleve.prenom as prenom,
#                    eleve.date_naissance as date_naissance,
#                    eleve.sexe as sexe,
#                    eleve.id_etablissement as id_etablissement,
#                    etab.nom_etablissement as nom_etablissement
#             ORDER BY eleve.id_eleve
#         """,
#         "vue_complete": """
#             MATCH (eleve:Eleve)-[:INSCRIT_A]->(etab:Etablissement)-[:APPARTIENT_A]->(region:Region)
#             RETURN eleve.id_eleve as id_eleve,
#                    eleve.nom as nom_eleve,
#                    eleve.prenom as prenom_eleve,
#                    eleve.date_naissance as date_naissance,
#                    eleve.sexe as sexe,
#                    etab.id_etablissement as id_etablissement,
#                    etab.nom_etablissement as nom_etablissement,
#                    etab.type as type_etablissement,
#                    etab.statut as statut_etablissement,
#                    region.id_region as id_region,
#                    region.nom_region as nom_region,
#                    region.ville as ville
#             ORDER BY eleve.id_eleve
#         """
#     }

# # --------------------
# # Programme principal
# # --------------------
# def main():
#     print("[INFO] Démarrage de l'extraction Neo4j vers HDFS")
#     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
#     # 1. Connexion à Neo4j
#     graph = connect_neo4j_with_retry()
    
#     # 2. Récupération des données
#     queries = get_cypher_queries()
#     data_map = {}
#     for query_name, query in queries.items():
#         data_map[query_name] = get_neo4j_data(graph, query, query_name)
    
#     # 3. Sauvegarde locale
#     local_output_dir = f"./output_neo4j_{timestamp}"
#     local_files = {}
#     local_success_count = 0
    
#     for key, data in data_map.items():
#         filename = f"{key}_neo4j.csv"
#         success, filepath = save_data_locally(local_output_dir, filename, data)
#         if success:
#             local_files[key] = filepath
#             local_success_count += 1
    
#     print(f"[INFO] Sauvegarde locale terminée : {local_success_count}/{len(data_map)} fichiers")
    
#     # 4. Connexion et upload vers HDFS
#     hdfs_client = connect_hdfs_with_retry()
#     hdfs_success_count = 0
    
#     if hdfs_client:
#         hdfs_base_path = "/data/datalake"
        
#         for key, local_file in local_files.items():
#             filename = f"{key}_neo4j_{timestamp}.csv"
#             hdfs_path = f"{hdfs_base_path}/{filename}"
            
#             if upload_to_hdfs(hdfs_client, local_file, hdfs_path):
#                 hdfs_success_count += 1
        
#         print(f"[INFO] Upload HDFS terminé : {hdfs_success_count}/{len(local_files)} fichiers")
#     else:
#         print("[WARN] Upload HDFS impossible, données disponibles uniquement localement")
    
#     # 5. Résumé final
#     print(f"\n=== RÉSUMÉ EXTRACTION ===")
#     print(f"Timestamp: {timestamp}")
#     print(f"Dossier local: {local_output_dir}")
#     print(f"Fichiers sauvegardés localement: {local_success_count}/{len(data_map)}")
#     if hdfs_client:
#         print(f"Fichiers uploadés vers HDFS: {hdfs_success_count}/{len(local_files)}")
#         print(f"Répertoire HDFS: {hdfs_base_path}")
    
#     for key, data in data_map.items():
#         print(f"- {key}: {len(data)} enregistrements")
    
#     print(f"\n✅ ETL Neo4j terminé avec succès")

# if __name__ == "__main__":
#     main()

# =============================================================================
# SCRIPT EXTRACTION NEO4J - VERSION FRANÇAISE
# Fichier: extraire_neo4j.py
# =============================================================================

from py2neo import Graph, Node, Relationship
from hdfs import InsecureClient
import time
import os
import csv
from datetime import datetime

# ------------------------------------
# Paramètres globaux pour les tentatives
# ------------------------------------
NOMBRE_MAX_TENTATIVES = 10
DELAI_ENTRE_TENTATIVES = 10

# --------------------------
# Configuration Neo4j
# --------------------------
URI_NEO4J = "bolt://neo4j:7687"
UTILISATEUR_NEO4J = "neo4j"
MOT_DE_PASSE_NEO4J = "12345678"

# --------------------------
# Configuration HDFS
# --------------------------
URL_HDFS = "http://namenode:9870"
UTILISATEUR_HDFS = "hadoop"
CHEMIN_HDFS_NEO4J = "/data/datalake/donnees_neo4j"

# Variable globale pour stocker les données Neo4j
donnees_neo4j = {}

# ----------------------------------------------------------
# Fonction : Connexion à Neo4j avec tentatives multiples
# ----------------------------------------------------------
def connecter_neo4j_avec_tentatives():
    for tentative in range(1, NOMBRE_MAX_TENTATIVES + 1):
        try:
            print(f"[INFO] Tentative {tentative}/{NOMBRE_MAX_TENTATIVES} de connexion à Neo4j...")
            graphe = Graph(URI_NEO4J, auth=(UTILISATEUR_NEO4J, MOT_DE_PASSE_NEO4J))
            if graphe.run("RETURN 1 as test").data()[0]["test"] == 1:
                print("[INFO] Connexion à Neo4j réussie ✅")
                return graphe
        except Exception as e:
            print(f"[ERROR] Connexion Neo4j échouée : {e}")
        
        if tentative < NOMBRE_MAX_TENTATIVES:
            print(f"[INFO] Nouvelle tentative dans {DELAI_ENTRE_TENTATIVES}s...")
            time.sleep(DELAI_ENTRE_TENTATIVES)
    
    print("[FATAL] Impossible de se connecter à Neo4j après plusieurs tentatives.")
    exit(1)

# ----------------------------------------------------------
# Fonction : Connexion à HDFS avec tentatives multiples
# ----------------------------------------------------------
def connecter_hdfs_avec_tentatives():
    for tentative in range(1, NOMBRE_MAX_TENTATIVES + 1):
        try:
            print(f"[INFO] Tentative {tentative}/{NOMBRE_MAX_TENTATIVES} de connexion à HDFS...")
            client_hdfs = InsecureClient(URL_HDFS, user=UTILISATEUR_HDFS)
            # Test de connexion
            client_hdfs.list("/")
            print("[INFO] Connexion à HDFS réussie ✅")
            return client_hdfs
        except Exception as e:
            print(f"[ERROR] Connexion HDFS échouée : {e}")
        
        if tentative < NOMBRE_MAX_TENTATIVES:
            print(f"[INFO] Nouvelle tentative dans {DELAI_ENTRE_TENTATIVES}s...")
            time.sleep(DELAI_ENTRE_TENTATIVES)
    
    print("[WARN] Impossible de se connecter à HDFS après plusieurs tentatives.")
    return None

# ----------------------------------------------------------
# Fonction : Exécuter une requête Cypher et récupérer les données
# ----------------------------------------------------------
def obtenir_donnees_neo4j(graphe, requete, nom_requete):
    try:
        print(f"[INFO] Exécution de la requête : {nom_requete}")
        resultat = graphe.run(requete)
        donnees = resultat.data()
        print(f"[INFO] Données récupérées pour {nom_requete} : {len(donnees)} éléments.")
        return donnees
    except Exception as e:
        print(f"[ERROR] Erreur lors de l'exécution de {nom_requete} : {e}")
        return []

# ----------------------------------------------------------
# Fonction : Écrire des données en CSV dans HDFS
# ----------------------------------------------------------
def ecrire_vers_hdfs(client_hdfs, chemin, enregistrements):
    """Écrit une liste de dictionnaires en CSV dans HDFS."""
    if not enregistrements:
        print(f"[WARN] Aucune donnée à écrire dans {chemin}")
        return
    try:
        with client_hdfs.write(chemin, overwrite=True, encoding="utf-8") as writer:
            writer_csv = csv.DictWriter(writer, fieldnames=enregistrements[0].keys())
            writer_csv.writeheader()
            writer_csv.writerows(enregistrements)
        print(f"[INFO] {chemin} écrit avec succès.")
    except Exception as e:
        print(f"[ERROR] Erreur écriture HDFS {chemin} : {e}")

# --------------------------
# Définition des requêtes Cypher
# --------------------------
def obtenir_requetes_cypher():
    return {
        "regions": """
            MATCH (r:Region)
            RETURN r.id_region as id_region, 
                   r.nom_region as nom_region, 
                   r.ville as ville
            ORDER BY r.id_region
        """,
        "etablissements": """
            MATCH (e:Etablissement)-[:APPARTIENT_A]->(r:Region)
            RETURN e.id_etablissement as id_etablissement,
                   e.nom_etablissement as nom_etablissement,
                   e.type as type,
                   e.statut as statut,
                   e.id_region as id_region,
                   r.nom_region as nom_region,
                   r.ville as ville
            ORDER BY e.id_etablissement
        """,
        "eleves": """
            MATCH (eleve:Eleve)-[:INSCRIT_A]->(etab:Etablissement)
            RETURN eleve.id_eleve as id_eleve,
                   eleve.nom as nom,
                   eleve.prenom as prenom,
                   eleve.date_naissance as date_naissance,
                   eleve.sexe as sexe,
                   eleve.id_etablissement as id_etablissement,
                   etab.nom_etablissement as nom_etablissement
            ORDER BY eleve.id_eleve
        """,
        "vue_complete": """
            MATCH (eleve:Eleve)-[:INSCRIT_A]->(etab:Etablissement)-[:APPARTIENT_A]->(region:Region)
            RETURN eleve.id_eleve as id_eleve,
                   eleve.nom as nom_eleve,
                   eleve.prenom as prenom_eleve,
                   eleve.date_naissance as date_naissance,
                   eleve.sexe as sexe,
                   etab.id_etablissement as id_etablissement,
                   etab.nom_etablissement as nom_etablissement,
                   etab.type as type_etablissement,
                   etab.statut as statut_etablissement,
                   region.id_region as id_region,
                   region.nom_region as nom_region,
                   region.ville as ville
            ORDER BY eleve.id_eleve
        """
    }

# --------------------
# Programme principal Neo4j
# --------------------
def extraire_donnees_neo4j():
    """Fonction principale pour extraire les données de Neo4j."""
    global donnees_neo4j
    
    print("[INFO] Démarrage de l'extraction Neo4j")
    horodatage = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 1. Connexion à Neo4j
    graphe = connecter_neo4j_avec_tentatives()
    
    # 2. Récupération des données et stockage dans la variable globale
    requetes = obtenir_requetes_cypher()
    for nom_requete, requete in requetes.items():
        donnees_neo4j[nom_requete] = obtenir_donnees_neo4j(graphe, requete, nom_requete)
    
    # 3. Résumé
    print(f"\n=== RÉSUMÉ EXTRACTION NEO4J ===")
    print(f"Horodatage: {horodatage}")
    print(f"Données stockées dans la variable 'donnees_neo4j':")
    
    for cle, donnees in donnees_neo4j.items():
        print(f"- {cle}: {len(donnees)} enregistrements")
    
    print(f"\n✅ Extraction Neo4j terminée avec succès")
    return donnees_neo4j

def sauvegarder_donnees_neo4j_vers_hdfs():
    """Sauvegarde les données Neo4j vers HDFS."""
    if not donnees_neo4j:
        print("[WARN] Aucune donnée Neo4j à sauvegarder vers HDFS")
        return
    
    # Connexion HDFS
    client_hdfs = connecter_hdfs_avec_tentatives()
    if not client_hdfs:
        print("[ERROR] Impossible de sauvegarder vers HDFS - pas de connexion")
        return
    
    # Créer le répertoire de base s'il n'existe pas
    try:
        client_hdfs.makedirs(CHEMIN_HDFS_NEO4J)
        print(f"[INFO] Répertoire HDFS créé : {CHEMIN_HDFS_NEO4J}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"[INFO] Répertoire HDFS existe déjà : {CHEMIN_HDFS_NEO4J}")
        else:
            print(f"[WARN] Erreur création répertoire HDFS : {e}")
    
    horodatage = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Sauvegarder chaque table
    for nom_table, donnees in donnees_neo4j.items():
        nom_fichier = f"{nom_table}_neo4j_{horodatage}.csv"
        chemin_hdfs_fichier = f"{CHEMIN_HDFS_NEO4J}/{nom_fichier}"
        ecrire_vers_hdfs(client_hdfs, chemin_hdfs_fichier, donnees)
    
    print(f"[INFO] ✅ Sauvegarde HDFS terminée vers {CHEMIN_HDFS_NEO4J}")

# =============================================================================
# FONCTIONS UTILITAIRES POUR NEO4J
# =============================================================================

def obtenir_table_neo4j(nom_table):
    """Récupère une table spécifique depuis les données Neo4j."""
    return donnees_neo4j.get(nom_table, [])

def sauvegarder_neo4j_vers_csv(prefixe_fichier="neo4j"):
    """Sauvegarde les données Neo4j en fichiers CSV locaux."""
    if not donnees_neo4j:
        print("[WARN] Aucune donnée Neo4j à sauvegarder")
        return
    
    horodatage = datetime.now().strftime("%Y%m%d_%H%M%S")
    repertoire_sortie = f"./sortie_{prefixe_fichier}_{horodatage}"
    os.makedirs(repertoire_sortie, exist_ok=True)
    
    for nom_table, enregistrements in donnees_neo4j.items():
        if enregistrements:
            chemin_fichier = os.path.join(repertoire_sortie, f"{nom_table}_{prefixe_fichier}.csv")
            with open(chemin_fichier, 'w', newline='', encoding='utf-8') as fichier_csv:
                writer = csv.DictWriter(fichier_csv, fieldnames=enregistrements[0].keys())
                writer.writeheader()
                writer.writerows(enregistrements)
            print(f"[INFO] Sauvegardé: {chemin_fichier}")

def afficher_resume_neo4j():
    """Affiche un résumé des données Neo4j chargées."""
    print("\n=== RÉSUMÉ DES DONNÉES NEO4J ===")
    if not donnees_neo4j:
        print("Aucune donnée Neo4j chargée")
        return
    
    for table, donnees in donnees_neo4j.items():
        print(f"- {table}: {len(donnees)} enregistrements")

def vider_donnees_neo4j():
    """Vide les données Neo4j de la mémoire."""
    global donnees_neo4j
    donnees_neo4j.clear()
    print("[INFO] Données Neo4j effacées de la mémoire")

def programme_principal():
    """Programme principal avec sauvegarde HDFS."""
    # Extraction des données
    extraire_donnees_neo4j()
    
    # Sauvegarde vers HDFS
    sauvegarder_donnees_neo4j_vers_hdfs()
    
    # Affichage du résumé
    afficher_resume_neo4j()

# =============================================================================
# EXEMPLE D'UTILISATION
# =============================================================================

if __name__ == "__main__":
    # Programme principal avec sauvegarde HDFS
    programme_principal()
    
    # Exemples d'accès aux données
    print("\n=== EXEMPLES D'ACCÈS AUX DONNÉES NEO4J ===")
    
    # Accéder à une table spécifique
    regions = obtenir_table_neo4j("regions")
    print(f"Régions: {len(regions)} enregistrements")
    
    if regions:
        print("Première région:", regions[0])
    
    # Accéder à toutes les données
    print(f"Tables disponibles: {list(donnees_neo4j.keys())}")
    
    # Sauvegarder en CSV local si nécessaire (optionnel)
    # sauvegarder_neo4j_vers_csv()