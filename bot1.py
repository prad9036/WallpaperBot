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
    logging.info("Loaded BOT_GROUPS from bot_config.py.")
except ImportError:
    logging.warning("bot_config.py not found. Using default BOT_GROUPS.")
    BOT_GROUPS = {
        "group_1": {"id": -1002370505230, "categories": ["anime", "cars", "nature"], "interval_seconds": 60},
        "group_2": {"id": -1002392972227, "categories": ["flowers", "abstract", "space"], "interval_seconds": 300},
    }

SIMILARITY_THRESHOLD = 5
logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

# --- Graceful Shutdown ---
shutdown_requested = False
ACTIVE_TASKS = set()

def handle_shutdown():
    global shutdown_requested
    shutdown_requested = True
    logging.info("Shutdown requested. Waiting for ongoing wallpaper posts to complete...")

# We don't need to get the event loop here; asyncio.run() will handle it.

# --- Database Pool ---
async def create_db_pool():
    try:
        return await asyncpg.create_pool(DATABASE_URI)
    except Exception as e:
        logging.critical(f"Failed to connect to DB: {e}")
        return None

# --- Hashing ---
def calculate_hashes(filepath):
    try:
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(block)
        sha256 = sha256_hash.hexdigest()
        p_hash = str(imagehash.average_hash(Image.open(filepath)))
        return sha256, p_hash
    except Exception as e:
        logging.error(f"Error calculating hashes for {filepath}: {e}")
        return None, None

async def check_image_hashes_in_db(pool, sha256, p_hash):
    async with pool.acquire() as conn:
        exact_match = await conn.fetchrow("SELECT id FROM wallpapers WHERE sha256 = $1", sha256)
        if exact_match:
            return "skipped", {"reason": "Duplicate", "details": {"type": "SHA256_match", "id": exact_match['id']}}

        all_p_hashes = await conn.fetch("SELECT phash FROM wallpapers WHERE phash IS NOT NULL")
        max_diff = 64
        new_hash = imagehash.hex_to_hash(p_hash)

        for db_hash_str in all_p_hashes:
            phash_hex = db_hash_str['phash']
            if not phash_hex or not phash_hex.strip():
                continue
            db_hash = imagehash.hex_to_hash(phash_hex)
            hash_diff = new_hash - db_hash
            if hash_diff < SIMILARITY_THRESHOLD:
                similarity_percentage = ((max_diff - hash_diff) / max_diff) * 100
                return "skipped", {
                    "reason": "Similar",
                    "details": {
                        "type": "p_hash_match",
                        "diff": hash_diff,
                        "similarity_percentage": round(similarity_percentage, 2)
                    }
                }
    return "proceed", None

# --- JSON Serializer ---
def json_serial(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode('utf-8')
    raise TypeError(f"Type {type(obj)} not serializable")

# --- Update DB ---
async def update_wallpaper_status(pool, jpg_url, status, reasons=None, sha256=None, phash=None, tg_response=None):
    async with pool.acquire() as conn:
        existing_data = await conn.fetchval("SELECT tg_response FROM wallpapers WHERE jpg_url = $1", jpg_url)
        if not existing_data:
            existing_data = {}
        elif isinstance(existing_data, str):
            try:
                existing_data = json.loads(existing_data)
            except Exception:
                existing_data = {}

        existing_data['status'] = status
        if reasons:
            existing_data['reasons'] = reasons
        if tg_response:
            existing_data['telegram_response'] = tg_response
        
        # Merge reasons if they exist from a previous state
        if reasons and 'reasons' in existing_data:
            existing_data['reasons'].update(reasons)

        await conn.execute(
            """
            UPDATE wallpapers
            SET status = $1, sha256 = $2, phash = $3, tg_response = $4
            WHERE jpg_url = $5
            """,
            status, sha256, phash, json.dumps(existing_data, default=json_serial), jpg_url
        )

# --- Download Image ---
async def download_image(url, filename):
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.get(url)
            r.raise_for_status()
            with open(filename, "wb") as f:
                f.write(r.content)
        return filename
    except Exception as e:
        logging.warning(f"Download failed for {url}: {e}")
        return None

# --- Fetch Random Wallpaper ---
async def get_random_wallpaper(pool, categories):
    try:
        async with pool.acquire() as conn:
            query = """
                SELECT jpg_url, tags, category FROM wallpapers
                WHERE category = ANY($1::text[]) AND status = 'pending'
                ORDER BY RANDOM()
                LIMIT 1;
            """
            result = await conn.fetchrow(query, categories)
            if result:
                return {"jpg_url": result["jpg_url"], "tags": result["tags"], "category": result["category"]}
            return None
    except Exception as e:
        logging.error(f"Database query failed: {e}")
        return None

# --- Send Wallpaper to Group ---
async def send_wallpaper_to_group(client, pool, config):
    if shutdown_requested:
        logging.info(f"Skipping wallpaper send for group {config['id']} due to shutdown request.")
        return

    task = asyncio.current_task()
    ACTIVE_TASKS.add(task)
    try:
        group_id = config["id"]
        categories = config["categories"]
        wallpaper = await get_random_wallpaper(pool, categories)
        if not wallpaper:
            logging.info(f"No new wallpapers found for categories {categories} for group {group_id}.")
            return

        jpg_url = wallpaper["jpg_url"]
        tags = wallpaper["tags"]
        caption = " ".join([f"#{t.replace(' ', '')}" for t in tags]) if tags else "#wallpaper"
        category = wallpaper.get("category", "wallpaper")
        filename = f"{category}_{random.randint(1000,9999)}_{os.path.basename(urlparse(jpg_url).path)}"

        path = await download_image(jpg_url, filename)
        if not path:
            reasons = {"reason": "Download failed"}
            await update_wallpaper_status(pool, jpg_url, "failed", reasons)
            logging.error(f"Download failed for {jpg_url}")
            return

        sha256, phash = calculate_hashes(path)
        if not sha256 or not phash:
            reasons = {"reason": "Hashing failed"}
            await update_wallpaper_status(pool, jpg_url, "failed", reasons)
            os.remove(path)
            logging.error(f"Hashing failed for {jpg_url}")
            return

        status_check, reasons = await check_image_hashes_in_db(pool, sha256, phash)
        if status_check == "skipped":
            log_details = f"{reasons['details']['type']}"
            if 'similarity_percentage' in reasons['details']:
                log_details += f" ({reasons['details']['similarity_percentage']}% similar)"
            logging.warning(f"Skipping {jpg_url}: {reasons['reason']} - {log_details}")
            await update_wallpaper_status(pool, jpg_url, "skipped", reasons, sha256, phash)
            os.remove(path)
            return

        try:
            preview_response = await client.send_file(group_id, path, caption=caption, force_document=False)
            await asyncio.sleep(5)
            hd_response = await client.send_file(group_id, path, caption="HD Download", force_document=True)
            tg_response = {"preview": preview_response.to_dict(), "hd": hd_response.to_dict()}
            reasons = {"reason": "Success"}
            await update_wallpaper_status(pool, jpg_url, "posted", reasons, sha256, phash, tg_response)
            logging.info(f"Posted wallpaper {jpg_url} to group {group_id}")
        except Exception as telegram_e:
            reasons = {"reason": "Telegram upload failed", "details": str(telegram_e)}
            await update_wallpaper_status(pool, jpg_url, "failed", reasons)
            logging.error(f"Telegram upload failed for {jpg_url}: {telegram_e}")
        finally:
            if os.path.exists(path):
                os.remove(path)
    finally:
        ACTIVE_TASKS.discard(task)

# --- Handle pending HD uploads on startup ---
async def handle_pending_hd_uploads(client, pool):
    async with pool.acquire() as conn:
        # Corrected query: Use a JSONB path operator to find rows with 'preview' but no 'hd'
        pending_hd_rows = await conn.fetch("""
            SELECT jpg_url, tg_response, category
            FROM wallpapers
            WHERE tg_response @> '{"preview": {}}'::jsonb
              AND NOT tg_response @> '{"hd": {}}'::jsonb;
        """)

    if not pending_hd_rows:
        logging.info("No pending HD uploads found on startup.")
        return

    logging.info(f"Found {len(pending_hd_rows)} pending HD uploads. Retrying...")

    for row in pending_hd_rows:
        try:
            tg_data = row['tg_response']
            if isinstance(tg_data, str):
                tg_data = json.loads(tg_data)
            
            jpg_url = row['jpg_url']
            category = row.get('category', 'wallpaper')

            target_group_id = None
            for group_cfg in BOT_GROUPS.values():
                if category in group_cfg['categories']:
                    target_group_id = group_cfg['id']
                    break

            if not target_group_id:
                logging.warning(f"No group found for category '{category}' of {jpg_url}")
                continue

            filename = f"{category}_{random.randint(1000,9999)}_{os.path.basename(urlparse(jpg_url).path)}"
            path = await download_image(jpg_url, filename)
            if not path:
                logging.warning(f"Failed to download {jpg_url} for pending HD upload")
                continue

            try:
                hd_response = await client.send_file(
                    target_group_id,
                    path,
                    caption="HD Download",
                    force_document=True
                )
                tg_data['hd'] = hd_response.to_dict()
                await update_wallpaper_status(
                    pool,
                    jpg_url,
                    "posted",
                    {"reason": "HD uploaded after restart"},
                    tg_response=tg_data
                )
                logging.info(f"Completed pending HD upload for {jpg_url} in group {target_group_id}")
            finally:
                if os.path.exists(path):
                    os.remove(path)
        except Exception as e:
            logging.error(f"Error handling pending HD upload for {row.get('jpg_url', 'N/A')}: {e}")

# --- Main ---
async def main():
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, handle_shutdown)
    loop.add_signal_handler(signal.SIGTERM, handle_shutdown)

    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start(bot_token=BOT_TOKEN)

    db_pool = await create_db_pool()
    if not db_pool:
        logging.critical("Cannot start bot without DB connection.")
        return

    await handle_pending_hd_uploads(client, db_pool)

    scheduler = AsyncIOScheduler()
    for group_name, config in BOT_GROUPS.items():
        scheduler.add_job(
            send_wallpaper_to_group,
            'interval',
            args=[client, db_pool, config],
            seconds=config['interval_seconds'],
            id=f'job_{group_name}',
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60
        )
    scheduler.start()
    logging.info("Wallpaper bot started.")

    # Replaced client.run_until_disconnected() with a manual loop
    while not shutdown_requested:
        await asyncio.sleep(1)
        
    # Wait for any active wallpaper tasks to finish before exiting
    if ACTIVE_TASKS:
        logging.info(f"Waiting for {len(ACTIVE_TASKS)} active wallpaper tasks to finish...")
        await asyncio.gather(*ACTIVE_TASKS)
    
    await client.disconnect()
    await db_pool.close()
    
    logging.info("All tasks completed. Exiting.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Program interrupted by user.")

