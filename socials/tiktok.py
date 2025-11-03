import os
import sys
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional
from yt_dlp import YoutubeDL

USERNAME = ""
OUTPUT_DIR = os.path.join(os.getcwd(), "tiktok_downloads")
USER_DIR = os.path.join(OUTPUT_DIR, USERNAME)

MAX_VIDEOS: Optional[int] = None
MAX_CONCURRENCY = 4
MAX_RETRIES = 3

ydl_opts = {
    "outtmpl": os.path.join(USER_DIR, "%(id)s - %(title).100s.%(ext)s"),
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

def _extract_entries(profile_url: str) -> List[dict]:
    """Extraction synchrone de la playlist (profil) -> entrées."""
    with YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(profile_url, download=False)
    if not info or "entries" not in info:
        return []
    return [e for e in info["entries"] if e]

def _download_one(url: str) -> None:
    """Téléchargement synchrone d'une seule URL (à appeler dans un thread)."""
    with YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])

async def _download_one_async(
    url: str,
    sem: asyncio.Semaphore,
    exec_pool: ThreadPoolExecutor,
    idx: int,
    total: int,
):
    """Wrapper asyncio avec retries + sémaphore."""
    attempt = 0
    while True:
        attempt += 1
        try:
            async with sem:
                print(f"[{idx}/{total}] start {url} (try {attempt})")
                await asyncio.get_running_loop().run_in_executor(exec_pool, _download_one, url)
                print(f"[{idx}/{total}] done  {url}")
                return
        except Exception as e:
            if attempt >= MAX_RETRIES:
                print(f"[{idx}/{total}] FAIL {url} after {attempt} tries: {e}")
                return
            backoff = 1.5 ** attempt
            print(f"[{idx}/{total}] retry in {backoff:.1f}s ({url}) cause: {e}")
            await asyncio.sleep(backoff)

async def main():
    if not USERNAME:
        print("Erreur : définissez USERNAME.")
        sys.exit(1)

    os.makedirs(USER_DIR, exist_ok=True)
    profile_url = f"https://www.tiktok.com/@{USERNAME}"
    print(f"Téléchargement asynchrone des posts de @{USERNAME} dans : {USER_DIR}")

    entries = _extract_entries(profile_url)
    if not entries:
        print("Aucune entrée récupérée (profil privé/inexistant ?)")
        return

    if isinstance(MAX_VIDEOS, int):
        entries = entries[:MAX_VIDEOS]

    urls = []
    for e in entries:
        url = e.get("webpage_url") or e.get("url")
        if not url and e.get("id"):
            url = f"https://www.tiktok.com/@{USERNAME}/video/{e['id']}"
        if url:
            urls.append(url)

    total = len(urls)
    print(f"{total} élément(s) à télécharger (concurrence={MAX_CONCURRENCY}).")

    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as exec_pool:
        tasks = [
            _download_one_async(u, sem, exec_pool, idx=i + 1, total=total)
            for i, u in enumerate(urls)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        failures = [r for r in results if isinstance(r, Exception)]
        if failures:
            print(f"{len(failures)} échec(s) sur {total}.")

if __name__ == "__main__":
    asyncio.run(main())
