from hdfs import InsecureClient

# Remplacez par l'URL de votre HDFS
hdfs_url = 'http://namenode:9870'
client = InsecureClient(hdfs_url, user='hadoop')

# Test de la connexion
try:
    print(client.list('/'))  # Liste les fichiers à la racine
except Exception as e:
    print(f"[ERROR] Erreur de connexion HDFS : {e}")
