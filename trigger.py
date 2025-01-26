import redis
import time
# Connect to a local Redis server with authentication
pool = redis.ConnectionPool(host="18.144.100.85", password="123456", port=6379, db=0)
r = redis.StrictRedis(connection_pool= pool)
# r = redis.Redis(host='18.144.11.243', port=6379, db=0, password='123456')

zset_key = 'SingleModelPreProcess'
value = 'meeting200:test/meeting200/30SecTestVid.mp4:0.6'
# value = 'meeting422:test/video/no_trust.mp4:0.6'

# value = 'meeting157:test/video/1c5265f19d9990d9b7e00152f8742c98.mp4:0.6'
# value1 = 'meeting147:test/video/gitlab_video_3.mp4:0.6'
# value2 = 'meeting148:test/video/gitlab_video_4.mp4:0.6'
score = time.time()  # Current time as timestamp

r.zadd(zset_key, {value: score})
