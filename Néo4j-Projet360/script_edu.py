# Importation des librairies nécessaires
from py2neo import Graph, Node, Relationship  # Pour gérer Neo4j
import random                                # Pour la génération aléatoire
from datetime import datetime, timedelta     # Pour générer des dates aléatoires

# Connexion à Neo4j (modifie le mot de passe si nécessaire)
graph = Graph("bolt://localhost:7687", auth=("neo4j", "siraketa"))

# ---------------------------------------------------------------
# 1️⃣ Fonction pour générer une date aléatoire entre 2020 et 2024
# ---------------------------------------------------------------
def random_date(start_year=2020, end_year=2024):
    # Date de début : 1er janvier de l'année start_year
    start = datetime(start_year, 1, 1)
    # Date de fin : 31 décembre de l'année end_year
    end = datetime(end_year, 12, 31)
    # Calcul de la différence en jours entre les deux dates
    delta = end - start  
    # Nombre aléatoire de jours à ajouter à la date de début
    random_days = random.randint(0, delta.days)
    # On ajoute les jours aléatoires à la date de départ et formate au format YYYY-MM-DD
    return (start + timedelta(days=random_days)).strftime("%Y-%m-%d")

# ------------------------------------------
# 2️⃣ Création des Régions et Villes (zones)
# ------------------------------------------
def create_regions():
    # Liste complète des régions avec id unique, nom de la région et ville (commune ou département)
    regions = [
        # Zones de Kaolack
        {"id_region": "R1", "nom_region": "Kaolack", "ville": "Kaolack Commune"},
        {"id_region": "R2", "nom_region": "Kaolack", "ville": "Kaolack Département"},
        {"id_region": "R3", "nom_region": "Kaolack", "ville": "Guinguinéo"},
        {"id_region": "R4", "nom_region": "Kaolack", "ville": "Nioro du Rip"},

        # Zones de Kaffrine
        {"id_region": "R5", "nom_region": "Kaffrine", "ville": "Birkelane"},
        {"id_region": "R6", "nom_region": "Kaffrine", "ville": "Kaffrine"},
        {"id_region": "R7", "nom_region": "Kaffrine", "ville": "Koungheul"},
        {"id_region": "R8", "nom_region": "Kaffrine", "ville": "Malem Hoddar"},

        # Zone Guinguinéo (ajoutée)
        {"id_region": "R9", "nom_region": "Kaolack", "ville": "Guinguinéo"},

        # Zone Nioro du Rip (ajoutée)
        {"id_region": "R10", "nom_region": "Kaolack", "ville": "Nioro du Rip"},
    ]

    # Pour chaque région dans la liste
    for r in regions:
        # Création du nœud Neo4j avec les propriétés id_region, nom_region et ville
        node_r = Node("Region",
                      id_region=r["id_region"],
                      nom_region=r["nom_region"],
                      ville=r["ville"])
        # Fusion dans Neo4j : crée si absent, sinon met à jour selon id_region
        graph.merge(node_r, "Region", "id_region")

# ---------------------------------------------------------------
# 3️⃣ Création des établissements avec association à la région
# ---------------------------------------------------------------
def create_etablissements():
    # Liste exhaustive des établissements pour Kaolack, Kaffrine, Guinguinéo, Nioro du Rip
    etablissements = [
        # Kaolack Commune (R1)
        {"id_etablissement": "E1", "nom_etablissement": "LYCEE ABDOU HAMID KANE", "type": "Lycée", "statut": "Public", "id_region": "R1"},
        {"id_etablissement": "E2", "nom_etablissement": "LYCEE DE KAOLACK (NOUVEAU LYCEE)", "type": "Lycée", "statut": "Public", "id_region": "R1"},
        {"id_etablissement": "E3", "nom_etablissement": "LYCEE EX CEM DE NGANE SAER", "type": "Lycée", "statut": "Public", "id_region": "R1"},
        {"id_etablissement": "E4", "nom_etablissement": "LYCEE FRANCO ARABE", "type": "Lycée", "statut": "Public", "id_region": "R1"},
        {"id_etablissement": "E5", "nom_etablissement": "LYCEE DE NGANE ALASSANE", "type": "Lycée", "statut": "Public", "id_region": "R1"},
        {"id_etablissement": "E6", "nom_etablissement": "LYCEE DE DYA", "type": "Lycée", "statut": "Public", "id_region": "R1"},
        {"id_etablissement": "E7", "nom_etablissement": "LYCEE DE KAHONE (EX CEM)", "type": "Lycée", "statut": "Public", "id_region": "R1"},
        {"id_etablissement": "E8", "nom_etablissement": "LYCEE DE KEUR SOCE", "type": "Lycée", "statut": "Public", "id_region": "R1"},
        {"id_etablissement": "E9", "nom_etablissement": "LYCEE IBRAHIMA DIOUF", "type": "Lycée", "statut": "Public", "id_region": "R1"},
        {"id_etablissement": "E10", "nom_etablissement": "LYCEE VALDIODIO NDIAYE", "type": "Lycée", "statut": "Public", "id_region": "R1"},

        # Kaolack Département (R2)
        {"id_etablissement": "E11", "nom_etablissement": "LYCEE DE KOUTAL (EX CEM)", "type": "Lycée", "statut": "Public", "id_region": "R2"},
        {"id_etablissement": "E12", "nom_etablissement": "LYCEE DE NDIAFFATE (EX CEM)", "type": "Lycée", "statut": "Public", "id_region": "R2"},
        {"id_etablissement": "E13", "nom_etablissement": "LYCEE DE NDIEBEL", "type": "Lycée", "statut": "Public", "id_region": "R2"},
        {"id_etablissement": "E14", "nom_etablissement": "LYCEE DE NDIEDIENG (EX CEM)", "type": "Lycée", "statut": "Public", "id_region": "R2"},
        {"id_etablissement": "E15", "nom_etablissement": "LYCEE DE NGOTHIE", "type": "Lycée", "statut": "Public", "id_region": "R2"},
        {"id_etablissement": "E16", "nom_etablissement": "LYCEE DE THIARE", "type": "Lycée", "statut": "Public", "id_region": "R2"},
        {"id_etablissement": "E17", "nom_etablissement": "LYCEE DE THIOFIOR", "type": "Lycée", "statut": "Public", "id_region": "R2"},
        {"id_etablissement": "E18", "nom_etablissement": "LYCEE EX CEM DE KOUMBAL", "type": "Lycée", "statut": "Public", "id_region": "R2"},
        {"id_etablissement": "E19", "nom_etablissement": "LYCEE EX CEM DE LATMINGUE", "type": "Lycée", "statut": "Public", "id_region": "R2"},
        {"id_etablissement": "E20", "nom_etablissement": "LYCEE EX CEM DE SIBASSOR", "type": "Lycée", "statut": "Public", "id_region": "R2"},
        {"id_etablissement": "E21", "nom_etablissement": "LYCEE EX CEM DE THIOMBY", "type": "Lycée", "statut": "Public", "id_region": "R2"},
        {"id_etablissement": "E22", "nom_etablissement": "LYCEE NDOFFANE", "type": "Lycée", "statut": "Public", "id_region": "R2"},
        {"id_etablissement": "E23", "nom_etablissement": "LYCEE SAMBA DIONE (EX CEM)", "type": "Lycée", "statut": "Public", "id_region": "R2"},

        # Guinguinéo (R9) - lycées listés
        {"id_etablissement": "E24", "nom_etablissement": "LYCEE DE MAKA KAHONE", "type": "Lycée", "statut": "Public", "id_region": "R9"},
        {"id_etablissement": "E25", "nom_etablissement": "LYCEE DE OUROUR", "type": "Lycée", "statut": "Public", "id_region": "R9"},
        {"id_etablissement": "E26", "nom_etablissement": "LYCEE EX CEM DE FASS", "type": "Lycée", "statut": "Public", "id_region": "R9"},
        {"id_etablissement": "E27", "nom_etablissement": "LYCEE EX CEM DE MBADAKHOUNE", "type": "Lycée", "statut": "Public", "id_region": "R9"},
        {"id_etablissement": "E28", "nom_etablissement": "LYCEE DE KHELCOM", "type": "Lycée", "statut": "Public", "id_region": "R9"},
        {"id_etablissement": "E29", "nom_etablissement": "LYCEE EX CEM DE MBOSS", "type": "Lycée", "statut": "Public", "id_region": "R9"},
        {"id_etablissement": "E30", "nom_etablissement": "LYCEE EX CEM DE NGATHIE NAOUDE", "type": "Lycée", "statut": "Public", "id_region": "R9"},

        # Nioro du Rip (R10) - ajout lycées listés
        {"id_etablissement": "E31", "nom_etablissement": "LYCEE EX CEM DE TAÏBA NIASSENE", "type": "Lycée", "statut": "Public", "id_region": "R10"},
        {"id_etablissement": "E32", "nom_etablissement": "LYCEE EX CEM DE WACK NGOUNA", "type": "Lycée", "statut": "Public", "id_region": "R10"},
        {"id_etablissement": "E33", "nom_etablissement": "LYCEE EX CEM FRANCO ARABE KEUR MADIABEL", "type": "Lycée", "statut": "Public", "id_region": "R10"},
        {"id_etablissement": "E34", "nom_etablissement": "LYCEE EX CEM SÉNY SAMB", "type": "Lycée", "statut": "Public", "id_region": "R10"},
        {"id_etablissement": "E35", "nom_etablissement": "LYCEE KEUR MADIABEL", "type": "Lycée", "statut": "Public", "id_region": "R10"},
        {"id_etablissement": "E36", "nom_etablissement": "LYCEE MABA DIAKHOU BA", "type": "Lycée", "statut": "Public", "id_region": "R10"},
        {"id_etablissement": "E37", "nom_etablissement": "LYCÉE SAMBOU OUMANI TOURE", "type": "Lycée", "statut": "Public", "id_region": "R10"},
        {"id_etablissement": "E38", "nom_etablissement": "LYCEE EX CEM DE POROKHANE", "type": "Lycée", "statut": "Public", "id_region": "R10"},
        {"id_etablissement": "E39", "nom_etablissement": "LYCEE DE KEUR AYIB POSTE", "type": "Lycée", "statut": "Public", "id_region": "R10"},
        {"id_etablissement": "E40", "nom_etablissement": "LYCEE DE NGAYENE SABAKH", "type": "Lycée", "statut": "Public", "id_region": "R10"},
        {"id_etablissement": "E41", "nom_etablissement": "LYCEE DE VELINGARA WALO", "type": "Lycée", "statut": "Public", "id_region": "R10"},
        {"id_etablissement": "E42", "nom_etablissement": "LYCEE EX CEM DE KEUR MABA DIAKHOU", "type": "Lycée", "statut": "Public", "id_region": "R10"},
        {"id_etablissement": "E43", "nom_etablissement": "LYCEE EX CEM DE PAOSKOTO", "type": "Lycée", "statut": "Public", "id_region": "R10"},
        {"id_etablissement": "E44", "nom_etablissement": "LYCEE SAMBA DIONE (EX CEM)", "type": "Lycée", "statut": "Public", "id_region": "R10"},

        # Kaffrine - Birkelane (R5)
        {"id_etablissement": "E45", "nom_etablissement": "LYCEE DE BIRKILANE", "type": "Lycée", "statut": "Public", "id_region": "R5"},
        {"id_etablissement": "E46", "nom_etablissement": "LYCEE MABO EX CEM", "type": "Lycée", "statut": "Public", "id_region": "R5"},

        # Kaffrine - Kaffrine (R6)
        {"id_etablissement": "E47", "nom_etablissement": "LYCEE DE DIAKHAO SALOUM", "type": "Lycée", "statut": "Public", "id_region": "R6"},
        {"id_etablissement": "E48", "nom_etablissement": "LYCEE DE DIOKOUL MBELBOUCK", "type": "Lycée", "statut": "Public", "id_region": "R6"},
        {"id_etablissement": "E49", "nom_etablissement": "LYCEE EX CEM DE BOULEL030", "type": "Lycée", "statut": "Public", "id_region": "R6"},
        {"id_etablissement": "E50", "nom_etablissement": "LYCEE EX CEM DIAMAGUENE TP111", "type": "Lycée", "statut": "Public", "id_region": "R6"},
        {"id_etablissement": "E51", "nom_etablissement": "LYCEE DE KAFFRINE", "type": "Lycée", "statut": "Public", "id_region": "R6"},
        {"id_etablissement": "E52", "nom_etablissement": "LYCEE FRANCO ARABE DE KAFFRINE", "type": "Lycée", "statut": "Public", "id_region": "R6"},
        {"id_etablissement": "E53", "nom_etablissement": "LYCEE GNIBY EX CEM", "type": "Lycée", "statut": "Public", "id_region": "R6"},
        {"id_etablissement": "E54", "nom_etablissement": "LYCEE YORO SY PENDA MBAYE", "type": "Lycée", "statut": "Public", "id_region": "R6"},

        # Kaffrine - Koungheul (R7)
        {"id_etablissement": "E55", "nom_etablissement": "LYCEE DE KOUNGHEUL COMMUNE", "type": "Lycée", "statut": "Public", "id_region": "R7"},
        {"id_etablissement": "E56", "nom_etablissement": "LYCEE DE MISSIRAH WADENE", "type": "Lycée", "statut": "Public", "id_region": "R7"},
        {"id_etablissement": "E57", "nom_etablissement": "LYCEE EL HADJI IBRAHIMA BA (KOUNGHEUL)", "type": "Lycée", "statut": "Public", "id_region": "R7"},

        # Kaffrine - Malem Hoddar (R8)
        {"id_etablissement": "E58", "nom_etablissement": "LYCÉE EL HADJI IBRAHIMA KA DIT IBOULAYE", "type": "Lycée", "statut": "Public", "id_region": "R8"},
        {"id_etablissement": "E59", "nom_etablissement": "LYCEE EX CEM NDIOUM NGAINTH", "type": "Lycée", "statut": "Public", "id_region": "R8"},
    ]

    # Création des nœuds Etablissement et création des relations vers leur Région
    for e in etablissements:
        # Création du nœud établissement avec ses propriétés
        node_e = Node("Etablissement",
                      id_etablissement=e["id_etablissement"],
                      nom_etablissement=e["nom_etablissement"],
                      type=e["type"],
                      statut=e["statut"],
                      id_region=e["id_region"])
        # Fusion ou mise à jour du nœud selon id_etablissement
        graph.merge(node_e, "Etablissement", "id_etablissement")

        # Recherche du nœud Région correspondant à l'établissement
        region_node = graph.nodes.match("Region", id_region=e["id_region"]).first()
        # Si trouvé, création de la relation APPARTIENT_A entre établissement et région
        if region_node:
            graph.merge(Relationship(node_e, "APPARTIENT_A", region_node))

# -----------------------------------------------
# 4️⃣ Création des élèves (100 par établissement)
# -----------------------------------------------
def create_eleves():
    # Listes pour générer aléatoirement les attributs des élèves
    sexes = ['M', 'F']  # Sexe masculin ou féminin
    noms = ["Diop", "Ndoye", "Sarr", "Fall", "Gueye", "Ba", "Dia", "Diallo", "Lo", "Seck"]
    prenoms = ["Mamadou", "Fatou", "Abdoulaye", "Aminata", "Cheikh", "Coumba", "Ousmane", "Sokhna", "Moussa", "Awa"]

    compteur_eleve = 1  # Compteur global pour générer un ID unique pour chaque élève

    # Récupération de tous les établissements créés dans la base Neo4j
    etablissements = list(graph.nodes.match("Etablissement"))

    # Pour chaque établissement, création de 100 élèves fictifs
    for etab in etablissements:
        print(f"Création de 100 élèves pour l'établissement {etab['nom_etablissement']}...")
        for _ in range(100):
            # Choix aléatoire des infos de l'élève
            nom = random.choice(noms)
            prenom = random.choice(prenoms)
            sexe = random.choice(sexes)
            date_naissance = random_date()  # Date entre 2020 et 2024

            # Génération d'un ID unique : ex S00001
            id_eleve = f"S{compteur_eleve:05d}"

            # Création du nœud Eleve avec ses propriétés
            eleve_node = Node("Eleve",
                              id_eleve=id_eleve,
                              nom=nom,
                              prenom=prenom,
                              date_naissance=date_naissance,
                              sexe=sexe,
                              id_etablissement=etab["id_etablissement"])
            # Fusion selon id_eleve
            graph.merge(eleve_node, "Eleve", "id_eleve")

            # Création de la relation Eleve INSCRIT_A Etablissement
            graph.merge(Relationship(eleve_node, "INSCRIT_A", etab))

            compteur_eleve += 1  # Incrémentation pour le prochain élève

    print("✅ 100 élèves créés pour chaque établissement.")

# --------------------------------------
# 5️⃣ Fonction principale qui lance tout
# --------------------------------------
def main():
    create_regions()        # Création des régions et villes
    create_etablissements() # Création des établissements et liens avec les régions
    create_eleves()         # Création des élèves pour chaque établissement

# Point d'entrée du script
if __name__ == "__main__":
    main()