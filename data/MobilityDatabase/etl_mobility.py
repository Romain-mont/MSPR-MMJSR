import requests
import os
import re
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()
# === CONFIGURATION ===
API_URL = "https://api.mobilitydatabase.org/v1"
REFRESH_TOKEN = os.getenv("REFRESH_TOKEN")

if not REFRESH_TOKEN:
    raise ValueError("❌ REFRESH_TOKEN non trouvé dans le fichier .env")

# Pays cibles (Codes ISO 2 lettres)
TARGET_COUNTRIES = ["FR", "CH", "DE", "BE", "ES", "IT", "NL"]

# Filtre optionnel : Mettre à False pour tout télécharger (Bus urbains inclus)
# Mettre à True pour essayer de cibler le ferroviaire/national (filtre heuristique)
ONLY_RAIL_AND_MAJOR = True 

# Mots-clés pour exclure les réseaux locaux si le filtre est actif
EXCLUDE_KEYWORDS = ["bus", "shuttle", "tram", "trolley", "urbain", "municipal"]
# Mots-clés pour forcer l'inclusion même si "bus" est présent (ex: "Bus replacement service")
INCLUDE_KEYWORDS = ["sncf", "db", "sbb", "cff", "rail", "train", "ferro", "national", "regional", "nmbs", "renfe", "trenitalia"]

def get_token():
    r = requests.post(f"{API_URL}/tokens", json={"refresh_token": REFRESH_TOKEN})
    r.raise_for_status()
    return r.json()["access_token"]

def sanitize_filename(name):
    """Nettoie le nom pour qu'il soit valide comme nom de fichier"""
    return re.sub(r'[\\/*?:"<>|]', "", name).replace(" ", "_").strip()

def is_interesting_feed(feed):
    """Filtre heuristique pour garder les gros réseaux"""
    if not ONLY_RAIL_AND_MAJOR:
        return True
    
    provider = (feed.get("provider") or "").lower()
    name = (feed.get("feed_name") or "").lower()
    full_text = f"{provider} {name}"

    # 1. Si c'est un mot-clé prioritaire (SNCF, DB, Rail...), on garde
    if any(kw in full_text for kw in INCLUDE_KEYWORDS):
        return True
        
    # 2. Si ça contient "Bus" ou "Tram" sans être un mot-clé prioritaire, on jette
    if any(kw in full_text for kw in EXCLUDE_KEYWORDS):
        return False
        
    # 3. Par défaut, on garde (dans le doute)
    return True

def download_country_feeds(country_code, token):
    headers = {"Authorization": f"Bearer {token}"}
    print(f"\n🌍 SCAN DU PAYS : {country_code}")
    
    # Récupérer tous les feeds du pays
    try:
        r = requests.get(f"{API_URL}/gtfs_feeds", headers=headers, params={"country_code": country_code, "limit": 100})
        r.raise_for_status()
        feeds = r.json()
    except Exception as e:
        print(f"❌ Erreur API pour {country_code}: {e}")
        return

    print(f"   -> {len(feeds)} feeds trouvés au total.")
    
    count_interesting = 0
    
    for feed in feeds:
        feed_id = feed.get("id")
        provider = sanitize_filename(feed.get("provider", "Unknown"))
        
        # Filtrage
        if not is_interesting_feed(feed):
            continue

        # Récupération URL
        dataset = feed.get("latest_dataset")
        if not dataset:
            continue
            
        url = dataset.get("hosted_url")
        if not url:
            continue

        count_interesting += 1
        print(f"   📄 [{feed_id}] {provider}")
        print(f"      URL: {url}")

    print(f"✅ {country_code} terminé : {count_interesting} feeds intéressants trouvés.")

if __name__ == "__main__":
    print("=== DÉMARRAGE AUTOMATISATION ETL EUROPE ===")
    token = get_token()
    
    for country in TARGET_COUNTRIES:
        download_country_feeds(country, token)
        
    print("\n🎉 Extraction terminée pour tous les pays cibles.")