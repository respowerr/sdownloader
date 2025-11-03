# tiktok_multi_async_daemon.py
import os
import sys
import asyncio
import random
import signal
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
from yt_dlp import YoutubeDL

# ========= CONFIG =========
USERNAMES = [
    "kassylbt",
    "leasantozz",
    "user010305victoire",
]

OUTPUT_ROOT = os.path.join(os.getcwd(), "tiktok_downloads")

# Limites de concurrence
MAX_CONCURRENCY_USERS = 3             # nb de profils traités en parallèle
MAX_CONCURRENCY_VIDEOS_PER_USER = 4   # téléchargements simultanés par profil
MAX_TOTAL_DOWNLOADS = 8               # plafond global (tous profils confondus)
MAX_RETRIES = 3                       # retries par vidéo

# Limitation du nombre de vidéos par profil (None = tout)
MAX_VIDEOS: Optional[int] = None

# Intervalle entre deux passes (en secondes)
MIN_INTERVAL_S = 60 * 60         # 1h
MAX_INTERVAL_S = 2 * 60 * 60     # 2h

# ==========================

def ydl_opts_for(user_dir: str) -> Dict[str, Any]:
    """Options yt-dlp spécifiques à un utilisateur (répertoire de sortie séparé)."""
    archive_path = os.path.join(user_dir, "archive.txt")
    return {
        "outtmpl": os.path.join(user_dir, "%(id)s - %(title).100s.%(ext)s"),
        "format": "bestvideo+bestaudio/best",
        "merge_output_format": "mp4",
        "ignoreerrors": True,
        "continuedl": True,
        "noplaylist": False,
        "quiet": False,           # passe à True si tu veux des logs compacts
        "no_warnings": True,
        "writesubtitles": False,
        "writeinfojson": True,
        "sleep_interval_requests": 1.0,
        "overwrites": False,
        "download_archive": archive_path,   # évite de retélécharger ce qui a déjà été téléchargé
        "retries": 5,                       # robustesse côté réseau
        # "proxy": "socks5://127.0.0.1:9050",
    }

def _extract_entries(profile_url: str, opts: Dict[str, Any]) -> List[dict]:
    """Extraction synchrone de la playlist (profil) -> liste d'entrées."""
    with YoutubeDL(opts) as ydl:
        info = ydl.extract_info(profile_url, download=False)
    if not info or "entries" not in info:
        return []
    return [e for e in info["entries"] if e]

def _download_one(url: str, opts: Dict[str, Any]) -> None:
    """Téléchargement synchrone d'une URL (à exécuter dans un thread)."""
    with YoutubeDL(opts) as ydl:
        ydl.download([url])

async def _download_one_async(
    url: str,
    exec_pool: ThreadPoolExecutor,
    per_user_sem: asyncio.Semaphore,
    global_sem: asyncio.Semaphore,
    idx: int,
    total: int,
    username: str,
    opts: Dict[str, Any],
):
    """Wrapper asyncio + retries + sémaphores (par user + global)."""
    attempt = 0
    while True:
        attempt += 1
        try:
            async with per_user_sem:
                async with global_sem:
                    print(f"[{username}] [{idx}/{total}] start {url} (try {attempt})")
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(exec_pool, _download_one, url, opts)
                    print(f"[{username}] [{idx}/{total}] done  {url}")
                    return
        except Exception as e:
            if attempt >= MAX_RETRIES:
                print(f"[{username}] [{idx}/{total}] FAIL {url} after {attempt} tries: {e}")
                return
            backoff = 1.5 ** attempt
            print(f"[{username}] [{idx}/{total}] retry in {backoff:.1f}s ({url}) cause: {e}")
            await asyncio.sleep(backoff)

def _build_urls_for_entries(entries: List[dict], username: str) -> List[str]:
    urls = []
    for e in entries:
        url = e.get("webpage_url") or e.get("url")
        if not url and e.get("id"):
            url = f"https://www.tiktok.com/@{username}/video/{e['id']}"
        if url:
            urls.append(url)
    return urls

async def _process_username(
    username: str,
    exec_pool: ThreadPoolExecutor,
    global_download_sem: asyncio.Semaphore,
):
    """Pipeline complet pour un username : extraction puis téléchargements parallèles."""
    user_dir = os.path.join(OUTPUT_ROOT, username)
    os.makedirs(user_dir, exist_ok=True)
    opts = ydl_opts_for(user_dir)

    profile_url = f"https://www.tiktok.com/@{username}"
    print(f"\n=== Profil @{username} → {user_dir} ===")

    entries = _extract_entries(profile_url, opts)
    if not entries:
        print(f"[{username}] Aucune entrée récupérée (profil privé/inexistant ?)")
        return

    if isinstance(MAX_VIDEOS, int):
        entries = entries[:MAX_VIDEOS]

    urls = _build_urls_for_entries(entries, username)
    total = len(urls)
    print(f"[{username}] {total} élément(s) à télécharger "
          f"(concurrence user={MAX_CONCURRENCY_VIDEOS_PER_USER}).")

    per_user_sem = asyncio.Semaphore(MAX_CONCURRENCY_VIDEOS_PER_USER)

    tasks = [
        _download_one_async(
            u,
            exec_pool,
            per_user_sem,
            global_download_sem,
            idx=i + 1,
            total=total,
            username=username,
            opts=opts,
        )
        for i, u in enumerate(urls)
    ]
    await asyncio.gather(*tasks, return_exceptions=False)

class GracefulExit(Exception):
    pass

def _install_signal_handlers(loop: asyncio.AbstractEventLoop):
    def _handle_signal(sig, frame):
        print(f"\nSignal {sig.name} reçu — arrêt propre après la passe en cours…")
        raise GracefulExit()

    for s in (signal.SIGINT, signal.SIGTERM):
        signal.signal(s, _handle_signal)

async def run_daemon():
    if not USERNAMES:
        print("Liste USERNAMES vide.")
        sys.exit(1)

    os.makedirs(OUTPUT_ROOT, exist_ok=True)
    _install_signal_handlers(asyncio.get_running_loop())

    max_workers = max(MAX_TOTAL_DOWNLOADS, MAX_CONCURRENCY_USERS)
    print(f"Daemon démarré. max_workers={max_workers}, "
          f"global_limit={MAX_TOTAL_DOWNLOADS}, users_limit={MAX_CONCURRENCY_USERS}, "
          f"per_user_limit={MAX_CONCURRENCY_VIDEOS_PER_USER}")

    with ThreadPoolExecutor(max_workers=max_workers) as exec_pool:
        global_download_sem = asyncio.Semaphore(MAX_TOTAL_DOWNLOADS)
        user_sem = asyncio.Semaphore(MAX_CONCURRENCY_USERS)

        while True:
            cycle_started_at = datetime.now(timezone.utc)
            print(f"\n===== NOUVELLE PASSE — {cycle_started_at.isoformat()} =====")

            async def run_user(username: str):
                async with user_sem:
                    await _process_username(username, exec_pool, global_download_sem)

            try:
                await asyncio.gather(*(run_user(u) for u in USERNAMES))
            except GracefulExit:
                print("Arrêt demandé pendant la passe. Fin immédiate.")
                return

            # Intervalle aléatoire entre 1h et 2h
            sleep_seconds = random.uniform(MIN_INTERVAL_S, MAX_INTERVAL_S)
            next_run_at = datetime.now(timezone.utc).timestamp() + sleep_seconds
            next_run_str = datetime.fromtimestamp(next_run_at, tz=timezone.utc).isoformat()

            print(f"Passe terminée. Prochaine passe dans ~{int(sleep_seconds)}s (≈ {next_run_str}).")
            try:
                await asyncio.sleep(sleep_seconds)
            except GracefulExit:
                print("Arrêt demandé pendant l’attente. Fin propre.")
                return

if __name__ == "__main__":
    try:
        asyncio.run(run_daemon())
    except KeyboardInterrupt:
        print("\nInterrompu par l'utilisateur.")