import redis
import time
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Connect to Redis server using environment variables
pool = redis.ConnectionPool(
    host=os.getenv("REDIS_HOST", "localhost"),
    password=os.getenv("REDIS_PASSWORD", ""),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=int(os.getenv("REDIS_DB", 0))
)
r = redis.StrictRedis(connection_pool=pool)

zset_key = os.getenv("REDIS_ZSET_KEY", 'SingleModelPreProcess')
value = os.getenv("REDIS_VALUE", 'meeting422:test/video/no_trust.mp4:0.6')

score = time.time()  # Current time as timestamp

r.zadd(zset_key, {value: score})