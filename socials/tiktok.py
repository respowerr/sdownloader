import os
import sys
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional, Dict, Any
from yt_dlp import YoutubeDL

USERNAMES = [
    " ",
    " ",
    " ",
]

OUTPUT_ROOT = os.path.join(os.getcwd(), "tiktok_downloads")

MAX_CONCURRENCY_USERS = 3          # nb de profils traités en parallèle
MAX_CONCURRENCY_VIDEOS_PER_USER = 4
MAX_TOTAL_DOWNLOADS = 8
MAX_RETRIES = 3

MAX_VIDEOS: Optional[int] = None


def ydl_opts_for(user_dir: str) -> Dict[str, Any]:
    """Options yt-dlp spécifiques à un utilisateur (répertoire de sortie séparé)."""
    return {
        "outtmpl": os.path.join(user_dir, "%(id)s - %(title).100s.%(ext)s"),
        "format": "bestvideo+bestaudio/best",
        "merge_output_format": "mp4",
        "ignoreerrors": True,
        "continuedl": True,
        "noplaylist": False,
        "quiet": False,
        "no_warnings": True,
        "writesubtitles": False,
        "writeinfojson": True,
        "sleep_interval_requests": 1.0,
        "overwrites": False,
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

    urls = []
    for e in entries:
        url = e.get("webpage_url") or e.get("url")
        if not url and e.get("id"):
            url = f"https://www.tiktok.com/@{username}/video/{e['id']}"
        if url:
            urls.append(url)

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

async def main():
    if not USERNAMES:
        print("Liste USERNAMES vide.")
        sys.exit(1)

    os.makedirs(OUTPUT_ROOT, exist_ok=True)

    max_workers = max(MAX_TOTAL_DOWNLOADS, MAX_CONCURRENCY_USERS)
    with ThreadPoolExecutor(max_workers=max_workers) as exec_pool:
        global_download_sem = asyncio.Semaphore(MAX_TOTAL_DOWNLOADS)
        user_sem = asyncio.Semaphore(MAX_CONCURRENCY_USERS)

        async def run_user(username: str):
            async with user_sem:
                await _process_username(username, exec_pool, global_download_sem)

        await asyncio.gather(*(run_user(u) for u in USERNAMES))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrompu par l'utilisateur.")
