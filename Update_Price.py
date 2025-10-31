import asyncio
import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime, timedelta, timezone
import logging
import os
from dotenv import load_dotenv

# ----------------- Load Environment -----------------
load_dotenv()

# MongoDB and General Config
MONGO_URI = os.getenv("MONGO_URI")
LOG_FILE = os.getenv("LOG_FILE", "Update_PriceCron_Queue.log")

# Adjustable tuning from .env
WORKERS = int(os.getenv("WORKERS", 20))
RATE_LIMIT = int(os.getenv("RATE_LIMIT", 5))
SCHEDULER_INTERVAL = int(os.getenv("SCHEDULER_INTERVAL", 30))
HISTORY_CLEAN_HOURS = int(os.getenv("HISTORY_CLEAN_HOURS", 3))
JOB_INTERVAL_MINUTES = int(os.getenv("JOB_INTERVAL_MINUTES", 60))
RETRY_INTERVAL_MINUTES = int(os.getenv("RETRY_INTERVAL_MINUTES", 10))

# ----------------- Logging Setup -----------------
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
print(f"🚀 Worker started | Workers={WORKERS}, RateLimit={RATE_LIMIT}")

# ----------------- MongoDB Setup -----------------
client = AsyncIOMotorClient(MONGO_URI)
db = client['cronjob']

users_collection = db['users']
packages_collection = db['packages']
price_update_collection = db['price_update_cronjobs']
price_update_history_collection = db['price_update_cronjobs_history']

# ----------------- Utilities -----------------
def safe_datetime(value):
    """Safely parse various datetime formats to UTC-aware datetime."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S"):
            try:
                dt = datetime.strptime(value, fmt)
                return dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue
    return None

# ----------------- Logging Events -----------------
async def log_event(job, event_type, error=None):
    now = datetime.now(timezone.utc)
    doc = {
        "user_id": str(job.get("user_id")),
        "url": job.get("url"),
        "task_name": job.get("title", "Unnamed Task"),
        "status": event_type,
        "error": str(error) if error else None,
        "timestamp": now
    }
    await price_update_history_collection.insert_one(doc)
    logging.info(f"{event_type.upper()} | {job.get('url')} | User: {job.get('user_id')}")

# ----------------- HTTP Request (Rate-Limited) -----------------
semaphore = asyncio.Semaphore(RATE_LIMIT)

async def fetch_with_retry(session, url, retries=3, delay=3):
    """Perform HEAD request with limited retries."""
    async with semaphore:
        for attempt in range(retries):
            try:
                async with session.head(url, timeout=20) as resp:
                    return resp.status
            except Exception as e:
                if attempt < retries - 1:
                    await asyncio.sleep(delay * (attempt + 1))
                else:
                    raise e

# ----------------- Worker -----------------
async def worker(queue, session):
    """Worker process that handles jobs from queue."""
    while True:
        job = await queue.get()
        try:
            await log_event(job, "started")
            status = await fetch_with_retry(session, job["url"])
            now = datetime.now(timezone.utc)

            if status == 200:
                await log_event(job, "completed")
                next_run = now + timedelta(minutes=JOB_INTERVAL_MINUTES)
            else:
                await log_event(job, f"failed ({status})")
                next_run = now + timedelta(minutes=RETRY_INTERVAL_MINUTES)

        except Exception as e:
            await log_event(job, "error", str(e))
            next_run = datetime.now(timezone.utc) + timedelta(minutes=JOB_INTERVAL_MINUTES)

        finally:
            await price_update_collection.update_one(
                {"_id": job["_id"]},
                {"$set": {"last_run": next_run.isoformat()}}
            )
            await asyncio.sleep(1)
            queue.task_done()

# ----------------- Scheduler -----------------
async def scheduler(queue):
    """Schedules new jobs for execution."""
    while True:
        try:
            now = datetime.now(timezone.utc)
            users_cursor = users_collection.find({"Status": "Active"})

            async for user in users_cursor:
                expiry = safe_datetime(user.get("Expiry_Date"))
                if not expiry or expiry <= now:
                    continue

                user_id = str(user["_id"])
                active_package = user.get("Package_Name")
                if not active_package:
                    continue

                async for job in price_update_collection.find({"user_id": user_id, "status": "Online"}):
                    last_run = safe_datetime(job.get("last_run"))
                    if not last_run or now >= last_run:
                        await queue.put(job)
                        logging.info(f"⏱️ Scheduled job for user {user_id}: {job['url']}")
                        await asyncio.sleep(0.5)

            await asyncio.sleep(SCHEDULER_INTERVAL)

        except Exception as e:
            logging.error(f"Scheduler error: {e}")
            await asyncio.sleep(10)

# ----------------- Cleanup -----------------
async def cleanup():
    """Periodically cleans up old history logs."""
    while True:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=HISTORY_CLEAN_HOURS)
        result = await price_update_history_collection.delete_many({"timestamp": {"$lt": cutoff}})
        if result.deleted_count > 0:
            logging.info(f"🧹 Cleaned {result.deleted_count} old history logs")
        await asyncio.sleep(HISTORY_CLEAN_HOURS * 3600)

# ----------------- Main -----------------
async def main():
    """Main async entrypoint."""
    queue = asyncio.Queue()
    async with aiohttp.ClientSession() as session:
        workers = [asyncio.create_task(worker(queue, session)) for _ in range(WORKERS)]
        sched = asyncio.create_task(scheduler(queue))
        cleaner = asyncio.create_task(cleanup())
        await asyncio.gather(*workers, sched, cleaner)

# ----------------- Run -----------------
if __name__ == "__main__":
    asyncio.run(main())
