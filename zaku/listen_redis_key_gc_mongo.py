import asyncio
import os
import time
from collections import deque, defaultdict

from dotenv import load_dotenv
import redis
import loguru

from interfaces import get_mongo_client
from server import TaskServer

logging = loguru.logger
load_dotenv()


async def listen_with_batch_processing(batch_size=1000):
    total_processed = 0
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")
    try:
        r = redis.asyncio.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True,
            ssl=True
        )
        await r.ping()
        pubsub = r.pubsub()
        await pubsub.psubscribe(f'__keyevent@{REDIS_DB}__:expired')

        logging.info(
            "Start listening for key"
            " expiration events (batch processing mode)..."
        )

        keys_buffer = deque()
        last_print_time = time.time()

        async for message in pubsub.listen():
            if message['type'] == 'pmessage':
                expired_key = message['data']
                keys_buffer.append(expired_key)
                total_processed += 1
                current_time = time.time()
                if (len(keys_buffer) >= batch_size or
                        current_time - last_print_time >= 1.0):

                    if keys_buffer:
                        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                        batch_count = len(keys_buffer)
                        logging.info(
                            f"[{timestamp}] Batch processing {batch_count} "
                            f"keys，In total:"
                            f" {total_processed}")
                        collection_name_job_ids_mapping = defaultdict(list)
                        logging.info(f"keys info {keys_buffer}")
                        for item in keys_buffer:
                            if ":" in item:
                                collection_name, job_id = item.rsplit(":", 1)
                                collection_name_job_ids_mapping[
                                    collection_name].append(job_id)
                        if collection_name_job_ids_mapping:
                            await TaskServer().initialize_mongodb()
                            mongo_client = get_mongo_client()
                            for collection_name, jobs in (
                                    collection_name_job_ids_mapping.items()):
                                logging.info(f"start gc document payload: "
                                             f"{collection_name}-->{jobs}")
                                await mongo_client.bulk_delete_payloads(
                                    collection_name, jobs)
                                logging.info(f"{collection_name} gc "
                                             f"successful")
                            await mongo_client.close()
                        keys_buffer.clear()
                        last_print_time = current_time

    except KeyboardInterrupt:
        logging.info(f"Listening stopped. All processed {total_processed} "
                     f"keys")
    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(listen_with_batch_processing())
