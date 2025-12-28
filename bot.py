import json
import random
import logging
import asyncio
import os
import httpx
from telethon import TelegramClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncpg
from urllib.parse import urlparse
import hashlib
from PIL import Image
import imagehash
import base64
from datetime import datetime
from dotenv import load_dotenv
import signal
import numpy as np
# --- Load Config ---
load_dotenv()
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
SESSION = "wallpaper_bot"
DATABASE_URI = os.getenv("DATABASE_URI")

# --- Bot Groups ---
try:
    from bot_config import BOT_GROUPS
except ImportError:
    BOT_GROUPS = {
        "group_1": {
            "id": -1002370505230,
            "categories": ["anime", "cars", "nature"],
            "interval_seconds": 60,
            "post_on_startup": True
        },
        "group_2": {
            "id": -1002392972227,
            "categories": ["flowers", "abstract", "space"],
            "interval_seconds": 300,
            "post_on_startup": True
        },
    }

SIMILARITY_THRESHOLD = 5
logging.basicConfig(level=logging.INFO)

shutdown_requested = False
ACTIVE_TASKS = set()


def handle_shutdown():
    global shutdown_requested
    shutdown_requested = True
    logging.info("Shutdown requested...")


# --- Database ---
async def create_db_pool():
    return await asyncpg.create_pool(DATABASE_URI)


# --- Hashing ---
def calculate_hashes(filepath):
    sha256 = hashlib.sha256(open(filepath, "rb").read()).hexdigest()
    phash = str(imagehash.average_hash(Image.open(filepath)))
    return sha256, phash


async def check_image_hashes_in_db(pool, sha256, phash):
    async with pool.acquire() as conn:
        if await conn.fetchrow("SELECT 1 FROM wallpapers WHERE sha256=$1", sha256):
            return False
    return True


# --- Download ---
async def download_image(url, filename):
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(url)
        r.raise_for_status()
        with open(filename, "wb") as f:
            f.write(r.content)
    return filename


# --- Fetch Wallpaper ---
async def get_random_wallpaper(pool, categories):
    async with pool.acquire() as conn:
        return await conn.fetchrow("""
            SELECT jpg_url, tags, category
            FROM wallpapers
            WHERE status='pending'
            AND category = ANY($1)
            ORDER BY RANDOM()
            LIMIT 1
        """, categories)


# --- Send Wallpaper ---
async def send_wallpaper_to_group(client, pool, config):
    if shutdown_requested:
        return

    task = asyncio.current_task()
    ACTIVE_TASKS.add(task)

    try:
        wallpaper = await get_random_wallpaper(pool, config["categories"])
        if not wallpaper:
            return

        url = wallpaper["jpg_url"]
        filename = f"{random.randint(1000,9999)}_{os.path.basename(urlparse(url).path)}"

        path = await download_image(url, filename)
        sha256, phash = calculate_hashes(path)

        if not await check_image_hashes_in_db(pool, sha256, phash):
            os.remove(path)
            return

        caption = " ".join(f"#{t.replace(' ', '')}" for t in wallpaper["tags"] or ["wallpaper"])

        await client.send_file(config["id"], path, caption=caption)
        await asyncio.sleep(3)
        await client.send_file(config["id"], path, caption="HD Download", force_document=True)

        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE wallpapers
                SET status='posted', sha256=$1, phash=$2
                WHERE jpg_url=$3
            """, sha256, phash, url)

        os.remove(path)

    except Exception as e:
        logging.error(f"Post failed: {e}")

    finally:
        ACTIVE_TASKS.discard(task)


# --- 🔥 Manual Startup Trigger ---
async def trigger_startup_posts(client, pool):
    logging.info("Triggering startup posts (sequential)...")

    for config in BOT_GROUPS.values():
        if not config.get("post_on_startup", True):
            continue

        await send_wallpaper_to_group(client, pool, config)

        # small cooldown between groups (VERY important)
        await asyncio.sleep(5)


def json_serial(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()

    if isinstance(obj, (bytes, bytearray)):
        return base64.b64encode(obj).decode("utf-8")

    if isinstance(obj, np.integer):
        return int(obj)

    if isinstance(obj, np.floating):
        return float(obj)

    if isinstance(obj, np.ndarray):
        return obj.tolist()

    return str(obj)


def normalize_json(data):
    """
    Ensures data is 100% JSON-serializable.
    Safe for nested dicts/lists.
    """
    return json.loads(json.dumps(data, default=json_serial))


# --- Main ---
async def main():
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, handle_shutdown)
    loop.add_signal_handler(signal.SIGTERM, handle_shutdown)

    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start(bot_token=BOT_TOKEN)

    pool = await create_db_pool()

    # 🔥 Immediate run
    await trigger_startup_posts(client, pool)

    scheduler = AsyncIOScheduler()
    for name, config in BOT_GROUPS.items():
        scheduler.add_job(
            send_wallpaper_to_group,
            "interval",
            args=[client, pool, config],
            seconds=config["interval_seconds"],
            id=name,
            max_instances=1,
            coalesce=True
        )

    scheduler.start()
    logging.info("Bot started.")

    while not shutdown_requested:
        await asyncio.sleep(1)

    if ACTIVE_TASKS:
        await asyncio.gather(*ACTIVE_TASKS)

    await client.disconnect()
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
