from py2neo import Graph

# Connexion à Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j", "siraketa"))

# 🔹 Récupérer tous les nœuds
nodes_query = "MATCH (n) RETURN n"
nodes = graph.run(nodes_query).data()

# 🔹 Récupérer toutes les relations
rels_query = "MATCH ()-[r]->() RETURN r"
relations = graph.run(rels_query).data()

# 🔹 Exemple d'accès
print(f"Nombre de nœuds : {len(nodes)}")
print(f"Nombre de relations : {len(relations)}")

# Stocker dans une seule variable Python
neo4j_data = {
    "nodes": nodes,
    "relations": relations
}
