import redis
import json


rds = redis.StrictRedis(host='10.1.4.62', port=6379, db=2, password='jiaqiubo')
# res = rds.smembers("tongcheng58_xzl_sset")
# for item in res:
#     rds.zadd('tongcheng58_xzl_zset', {item: 1})
# res = rds.hgetall("tongcheng58_xzl_detail_url_hashtable_back")


# res = rds.hgetall("tongcheng58_xzl_detail_url_hashtable")
# chushou = []
# beijing = 0
# shanghai = 0
# tianjin = 0
# chongqing = 0
# for item in res.items():
#     value = json.loads(item[1].decode())
#     if value['flag'] == "出售":
#         chushou.append(value['housing_url'])
#     province = value['province']
#     if province == "北京":
#         beijing += 1
#     if province == "天津":
#         tianjin += 1
#     if province == "上海":
#         shanghai += 1
#     if province == "重庆":
#         chongqing += 1
# print(beijing, tianjin, shanghai, chongqing)
# print(len(chushou))
# 49355 4874 30153 23215
# 5576

# 190 11022 42815 24522

# res = rds.zrevrangebyscore("fangtianxia_xzl_zset", 3, 2)
# print(res)
# for item in res:
#     rds.zadd("fangtianxia_xzl_zset", {item.decode(): 1})
res = rds.zrevrangebyscore("anjuke_xzl_zset", 3, 2)
print(res)
for item in res:
    rds.zadd("anjuke_xzl_zset", {item.decode(): 1})
