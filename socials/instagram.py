import os
import sys
from yt_dlp import YoutubeDL

# === CONFIG ===
USERNAME = "user010305victoire"   # <-- remplacez par le nom d'utilisateur TikTok sans @
OUTPUT_DIR = os.path.join(os.getcwd(), "tiktok_downloads")
USER_DIR = os.path.join(OUTPUT_DIR, USERNAME)
# Optionnel : limitez le nombre de vidéos si vous testez
MAX_VIDEOS = None  # int ou None pour toutes

# === Vérification d'entrée ===
if not USERNAME:
    print("Erreur : définissez USERNAME dans le script.")
    sys.exit(1)

os.makedirs(USER_DIR, exist_ok=True)

# === URL cible (profil public) ===
profile_url = f"https://www.tiktok.com/@{USERNAME}"

# === Options yt-dlp ===
ydl_opts = {
    # modèle de nom de fichier : dossier_utilisateur/<id> - <title>.ext
    "outtmpl": os.path.join(USER_DIR, "%(id)s - %(title).100s.%(ext)s"),
    "format": "bestvideo+bestaudio/best",
    "merge_output_format": "mp4",
    "ignoreerrors": True,     # continue si une vidéo pose problème
    "continuedl": True,       # reprendre téléchargements partiels
    "noplaylist": False,      # autoriser l'extraction de la "playlist" du profil
    "quiet": False,
    "no_warnings": True,
    "writesubtitles": False,
    "writeinfojson": True,    # utile pour métadonnées
    "sleep_interval_requests": 1.0,  # minimise les risques de throttling
    # "ratelimit": "1M",      # décommentez si vous voulez limiter la bande passante
    # "proxy": "socks5://127.0.0.1:9050", # si vous avez besoin d'un proxy
}

# Si vous voulez limiter le nombre de vidéos :
if MAX_VIDEOS is not None:
    ydl_opts["playlistend"] = MAX_VIDEOS

# === Exécution ===
def download_profile(url):
    with YoutubeDL(ydl_opts) as ydl:
        # Extraire d'abord les entrées (méta) pour voir combien il y a, puis lancer download
        try:
            info = ydl.extract_info(url, download=False)
        except Exception as e:
            print(f"Extraction initiale échouée : {e}")
            # Tentative directe de téléchargement (parfois utile)
            try:
                ydl.download([url])
                return
            except Exception as e2:
                print(f"Tentative de téléchargement directe échouée : {e2}")
                raise

        # info peut être un dict représentant une playlist / profil
        if info is None:
            print("Aucune information récupérée (profil introuvable ou privé).")
            return

        # Affiche résumé
        if "entries" in info:
            total = len([e for e in info["entries"] if e])
            print(f"Profil {USERNAME} : {total} vidéos détectées (peut être partiel).")
        else:
            print("Structure d'information inattendue, on lancera le téléchargement direct.")

        # Lancer le téléchargement (yt-dlp gère les pages)
        try:
            print("Démarrage du téléchargement. Consultez la sortie pour le progrès.")
            ydl.download([url])
        except Exception as e:
            print(f"Erreur pendant le téléchargement : {e}")

if __name__ == "__main__":
    print(f"Téléchargement des vidéos de @{USERNAME} dans : {USER_DIR}")
    download_profile(profile_url)
    print("Terminé.")
