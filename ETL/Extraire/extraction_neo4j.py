# script extraction_neo4j.py

# --------------------------
# Import des librairies
# --------------------------
from py2neo import Graph                   # Driver py2neo pour Neo4j
from hdfs import InsecureClient            # Pour interagir avec HDFS via WebHDFS
import csv                                 # Pour écrire les données sous forme de fichiers CSV
import time                                # Pour gérer les délais entre les tentatives (retry)

# ------------------------------------
# Paramètres globaux pour le retry
# ----------------------------------
MAX_RETRIES = 10       # Nombre maximum de tentatives de connexion
RETRY_DELAY = 10       # Délai (en secondes) entre chaque tentative

# --------------------------
# Configuration Neo4j
# --------------------------
NEO4J_URI = "bolt://localhost:7687"        # URI de connexion Neo4j
NEO4J_USER = "neo4j"                       # Utilisateur Neo4j
NEO4J_PASSWORD = "12345678"                # Mot de passe Neo4j

# ----------------------------------------------------------
# Fonction : Connexion à Neo4j avec retry
# ----------------------------------------------------------
def connect_neo4j_with_retry():
    """
    Établit une connexion à Neo4j avec plusieurs tentatives.
    Retourne l'objet Graph si succès, sinon quitte le programme.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"[INFO] Tentative {attempt}/{MAX_RETRIES} de connexion à Neo4j...")
            
            # Création de la connexion Graph avec py2neo
            graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            
            # Test de la connexion avec une requête simple
            result = graph.run("RETURN 1 as test").data()
            if result and result[0]["test"] == 1:
                print("[INFO] Connexion à Neo4j réussie ✅")
                return graph
                    
        except Exception as e:
            print(f"[ERROR] Connexion Neo4j échouée : {e}")
        
        # Attente avant la prochaine tentative
        if attempt < MAX_RETRIES:
            print(f"[INFO] Nouvelle tentative dans {RETRY_DELAY}s...")
            time.sleep(RETRY_DELAY)
    
    # Si toutes les tentatives échouent
    print("[FATAL] Impossible de se connecter à Neo4j après plusieurs tentatives.")
    exit(1)

# ----------------------------------------------------------
# Fonction : Exécuter une requête Cypher et récupérer les données
# ----------------------------------------------------------
def get_neo4j_data(graph, query, query_name):
    """
    Exécute une requête Cypher et retourne les résultats sous forme de liste de dictionnaires.
    
    Args:
        graph: Objet Graph py2neo
        query: Requête Cypher à exécuter
        query_name: Nom descriptif de la requête (pour les logs)
    
    Returns:
        Liste de dictionnaires contenant les résultats
    """
    try:
        print(f"[INFO] Exécution de la requête : {query_name}")
        
        # Exécution de la requête Cypher
        result = graph.run(query)
        
        # Conversion des résultats en liste de dictionnaires
        data = result.data()
        
        print(f"[INFO] Données récupérées pour {query_name} : {len(data)} éléments.")
        return data
            
    except Exception as e:
        print(f"[ERROR] Erreur lors de l'exécution de {query_name} : {e}")
        return []

# --------------------------
# Fonction : Stocker les données dans HDFS en CSV
# --------------------------
def stocke_data_datalake(client, chemin, contenu):
    """
    Écrit les données dans HDFS sous forme CSV.
    - client : connexion HDFS
    - chemin : chemin HDFS où stocker le fichier
    - contenu : liste de dictionnaires (les données)
    """
    if not contenu:
        print(f"[WARN] Aucune donnée à écrire dans {chemin}")
        return

    try:
        with client.write(chemin, overwrite=True, encoding="utf-8") as writer:
            if contenu:  # Vérification supplémentaire
                writer_csv = csv.DictWriter(writer, fieldnames=contenu[0].keys())
                writer_csv.writeheader()
                writer_csv.writerows(contenu)
        
        print(f"[INFO] Données écrites dans {chemin}")

    except Exception as e:
        print(f"[ERROR] Erreur écriture HDFS : {e}")

# --------------------------
# Définition des requêtes Cypher adaptées à votre modèle
# --------------------------
def get_cypher_queries():
    """
    Retourne un dictionnaire des requêtes Cypher basées sur votre modèle de données.
    """
    queries = {
        # 1. Extraction des régions
        "regions": """
            MATCH (r:Region)
            RETURN r.id_region as id_region, 
                   r.nom_region as nom_region, 
                   r.ville as ville
            ORDER BY r.id_region
        """,
        
        # 2. Extraction des établissements avec informations région
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
        
        # 3. Extraction des élèves avec informations établissement
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
        
        # 4. Extraction des relations élève-établissement
        "inscriptions": """
            MATCH (eleve:Eleve)-[rel:INSCRIT_A]->(etab:Etablissement)
            RETURN eleve.id_eleve as id_eleve,
                   etab.id_etablissement as id_etablissement,
                   type(rel) as type_relation
            ORDER BY eleve.id_eleve
        """,
        
        # 5. Extraction des relations établissement-région
        "etablissement_regions": """
            MATCH (etab:Etablissement)-[rel:APPARTIENT_A]->(region:Region)
            RETURN etab.id_etablissement as id_etablissement,
                   region.id_region as id_region,
                   type(rel) as type_relation
            ORDER BY etab.id_etablissement
        """,
        
        # 6. Statistiques par établissement
        "stats_etablissements": """
            MATCH (etab:Etablissement)
            OPTIONAL MATCH (eleve:Eleve)-[:INSCRIT_A]->(etab)
            RETURN etab.id_etablissement as id_etablissement,
                   etab.nom_etablissement as nom_etablissement,
                   count(eleve) as nombre_eleves
            ORDER BY etab.id_etablissement
        """,
        
        # 7. Statistiques par région
        "stats_regions": """
            MATCH (region:Region)
            OPTIONAL MATCH (etab:Etablissement)-[:APPARTIENT_A]->(region)
            OPTIONAL MATCH (eleve:Eleve)-[:INSCRIT_A]->(etab)
            RETURN region.id_region as id_region,
                   region.nom_region as nom_region,
                   region.ville as ville,
                   count(DISTINCT etab) as nombre_etablissements,
                   count(eleve) as nombre_eleves_total
            ORDER BY region.id_region
        """,
        
        # 8. Répartition par sexe
        "repartition_sexe": """
            MATCH (eleve:Eleve)-[:INSCRIT_A]->(etab:Etablissement)-[:APPARTIENT_A]->(region:Region)
            RETURN region.nom_region as nom_region,
                   etab.nom_etablissement as nom_etablissement,
                   eleve.sexe as sexe,
                   count(eleve) as nombre
            ORDER BY region.nom_region, etab.nom_etablissement, eleve.sexe
        """,
        
        # 9. Vue complète (élève + établissement + région)
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
    
    return queries

# --------------------
# Programme principal
# --------------------
if __name__ == "__main__":
    print("[INFO] Démarrage de l'extraction Neo4j vers HDFS")
    
    # --------------------------
    # 1. Connexion à Neo4j
    # --------------------------
    graph = connect_neo4j_with_retry()
    
    try:
        # --------------------------
        # 2. Définition des requêtes à exécuter
        # --------------------------
        queries = get_cypher_queries()
        
        print("[INFO] Récupération des données Neo4j en cours...")
        
        # --------------------------
        # 3. Exécution des requêtes et collecte des données
        # --------------------------
        data_map = {}
        for query_name, query in queries.items():
            data_map[query_name] = get_neo4j_data(graph, query, query_name)
        
        # --------------------------
        # 4. Connexion à HDFS
        # --------------------------
        client = InsecureClient('http://localhost:9870', user='hadoop')
        chemin_principal = "/data/datalake/"
        
        # --------------------------
        # 5. Stockage des données dans HDFS
        # --------------------------
        print("[INFO] Stockage des données dans HDFS...")
        for key, data in data_map.items():
            fichier_name = f"{key}_neo4j.csv"
            stocke_data_datalake(client, chemin_principal + fichier_name, data)
        
        print("[INFO] ETL Neo4j terminé avec succès ✅")
        
        # --------------------------
        # 6. Affichage des statistiques
        # --------------------------
        print("\n=== STATISTIQUES D'EXTRACTION ===")
        for key, data in data_map.items():
            print(f"- {key}: {len(data)} enregistrements")
        
    except Exception as e:
        print(f"[FATAL] Erreur lors de l'ETL : {e}")
    
    finally:
        print("[INFO] Extraction terminée")