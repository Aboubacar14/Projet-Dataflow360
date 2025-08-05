import requests


base_url = "http://192.168.3.51:8000"

def recevoir_donnees(endpoint, nom_utilisateus, mot_de_passe):
    response_login = requests.post(f"{endpoint}",params = {"username": f"{nom_utilisateus}", "password":f"{mot_de_passe}"})

    token = response_login.json()["access_token"]

    response = requests.get(f"{base_url}/cours", headers = {"Authorization":f"Bearer {token}"})
    cours = response.json()
    return cours

donnees = recevoir_donnees(f"{base_url}/utilisateurs/login", "Aboubacar Konaré", "siraketa")

print(donnees)