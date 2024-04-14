from pprint import pprint

import redis
from params_proto import PrefixProto, Proto
from redis.commands.json.path import Path
from redis.commands.search.field import TextField, TagField, NumericField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query, NumericFilter


class TaskQueueExample(PrefixProto):
    host = Proto(env="REDIS_HOST", default="localhost")
    port = Proto(env="REDIS_PORT", default=6379)
    password = Proto(None, env="REDIS_PASSWORD")
    db = Proto(env="REDIS_DB", default=0)


pprint(vars(TaskQueueExample))

r = redis.Redis(**vars(TaskQueueExample))

# r = redis.Redis(host='localhost', port=6379)
user1 = {
    "name": "Paul John",
    "email": "paul.john@example.com",
    "age": 42,
    "city": "London",
}
user2 = {
    "name": "Eden Zamir",
    "email": "eden.zamir@example.com",
    "age": 29,
    "city": "Tel Aviv",
}
user3 = {
    "name": "Paul Zamir",
    "email": "paul.zamir@example.com",
    "age": 35,
    "city": "Tel Aviv",
}

user4 = {
    "name": b"Sarah Zamir".decode(),
    "email": "sarah.zamir@example.com",
    "age": 30,
    "city": "Paris",
}
r.json().set("folder:user:1", Path.root_path(), user1)
r.json().set("folder:user:2", Path.root_path(), user2)
r.json().set("folder:user:3", Path.root_path(), user3)
r.json().set("folder:user:4", Path.root_path(), user4)

# prep
r.ft("folder:user").dropindex()
schema = (
    TextField("$.name", as_name="name"),
    TextField("$.email", as_name="email"),
    TagField("$.city", as_name="city"),
    NumericField("$.age", as_name="age"),
)
r.ft("folder:user").create_index(
    schema,
    definition=IndexDefinition(
        prefix=["folder:user:"],
        index_type=IndexType.JSON,
    ),
)
r.ft("folder:user")
result = r.ft("folder:user").search("com")
print(result)

user = {
    "name": b"Sarah Zamir".decode(),
    "email": "sarah.zamir@example.com",
    "age": 111,
    "city": "Paris",
}
rec = r.json().set("folder:user:5", Path.root_path(), user)
rec = r.json().set("folder:user:5.city", ".", "Beijing")

q = Query("@city: { Paris }")
result = r.ft("folder:user").search(q)
print(result)

q1 = Query("*").add_filter(NumericFilter("age", 30, 40))
result = r.ft("folder:user").search(q1)
print(result)

exit()

# r.ft().Search("job_queue-1", "job1")


# r.json()

# r.set("job-queue-debug.jobs", '[]')
# a = r.delete("job-queue-debug")
# a = r.delete("job-queue-debug.job")
# print(a)
# r.json().set("job-queue-debug", Path.root_path(), 'baz')
# r.json().set("job-queue-debug.jobs", Path.root_path(), '[]')
#
# obj = {"foo": "bar"}
# assert r.json().set("obj", Path.root_path(), obj)

# Test that flags prevent updates when conditions are unmet
# assert r.json().set("obj", Path("foo"), "baz", nx=True) is None
# assert r.json().set("obj:1", Path("qaz"), "baz", xx=True) is None

# print(Path.root_path())
#
# r.json().set("folder:1", Path.root_path(), {"value": "baz", "status": "in_progress", "grab_ts": 100})
# r.json().set("folder:2", Path.root_path(), {"value": "baz", "status": "in_progress", "grab_ts": 101})
# r.json().set("folder:3", Path.root_path(), {"value": "baz", "status": "in_progress", "grab_ts": 102})
# r.json().set("folder:4", Path.root_path(), {"value": "baz", "status": "in_progress", "grab_ts": 103})
#
# schema = (
#     TextField("$.folder.value", as_name="value"),
#     TagField("$.folder.status", as_name="status"),
#     NumericField("$.folder.grab_ts", as_name="grab_ts"),
# )
# try:
#     # r.json().set("folder", Path.root_path(), [])
#     r.ft().create_index(
#         schema,
#         definition=IndexDefinition(prefix=["folder:"], index_type=IndexType.JSON),
#     )
#     print("Created index")
# except ResponseError as e:
#     pass
#
#
# query = Query("folder:")
# result = r.ft().search(query)
# print(result)
# for i in trange(100):
#     r.json().arrappend(
#         "folder:arr",
#         Path.root_path(),
#         {
#             "value": i,
#             "grab_ts": 100 + i,
#             "status": "in_progress",
#         },
#     )
#
# a = r.json().get("folder:arr")
# print(a)
# a = r.json().arrpop("some.arr", index=0)
# a = r.json().arrpop("folder:arr", index=0)
# print(a)
#
# result = r.ft().search('folder:arr.grab_ts' )
# print(result)

# q1 = Query("Paul").add_filter(NumericFilter("age", 30, 40))
# r.ft().search(q1)

# r.set('foo', {"key": "value"})
# a = r.get('foo')
# print(a)
#
# r.rpush("job_queue-1", "job1")
# job = r.lpop("job_queue-1")
# print("job", job)
#
# job = r.lpop("job_queue-1")
# print("job", job)
