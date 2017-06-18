# -*- coding: UTF-8 -*-
import kodb
import os
import requests


# remove existing files
try:
	os.remove("ko-test.db")
	os.remove("ko-test.db.KO_CONFIG")
	os.remove("ko-test.db.KO_META")
except:
	pass

print("Retrieving Test Data")
users = requests.get("https://jsonplaceholder.typicode.com/users").json()
posts = requests.get("https://jsonplaceholder.typicode.com/posts").json()

db = kodb.KoDB("ko-test.db")
assert db is not None
print("OK - DB Create Test".encode('utf8'))

db.store

pdb = db.table("posts")
assert pdb is not None
print("OK - Table Create Test")

# Check if the tables have updated
assert "posts" in db.tables()
print("OK - Table Verification Test")

for user in users:
	db.store(user["username"], user)

#print(db.items())
assert len(db.items()) > 0
print("OK - Data Population Test")

for post in posts:
	pdb.store(post["id"], post)

# print(pdb.items())
assert len(pdb.items()) > 0
print("OK - Table Data Population Test")

class TestClass(object):
	"""docstring for TestClass"""
	def __init__(self, arg):
		self.arg = arg


test = TestClass("12345")
obj = db.table("objects")
obj.store("dummy",  {"object": test})
assert len(obj.items()) > 0
print("OK - Python Object Population Test")

db.close()
print("Reopening Test Data")
# Not safe loading
db = kodb.KoDB("ko-test.db", load_type="load")
assert "posts" in db.tables()
print("OK - [Table] Recreation Test")

print(db.items())
assert len(db.items()) > 0
print("OK - [Data Check] Recreation Test")

# print(pdb.items())
assert len(pdb.items()) > 0
print("OK - [Table Data Check] Recreation Test")

test_table = db.table("test")

test_table.store("abc", {"value": "Nelson"})
assert test_table.get("abc").value == "Nelson"
print("OK - Modify Pre Test")

test_table.store("abc", {"value": "Wilfred"})
assert test_table.get("abc").value == "Wilfred"
print("OK - Modify Post Test")

objn = db.table("objects")
result = objn.query(lambda x: x.object.arg == "12345")
# print(result)
assert len(result) > 0
print("OK - [Object] Recreation Test")

result = pdb.query(lambda x: "eius" in x.title)
assert len(result) > 0
print("OK -  Search Test")
# print(result)

import time
then = time.time()
for x in range(0, 1000): test_table.store("{}".format(x), {"value": "The quick brown fox jumps over the lazy dog."})
now = time.time()
print("Time Taken for commit-on-store: {}".format(now-then))

then = time.time()
db.KO_NO_COMMIT = True
for x in range(0, 1000): test_table.store("{}".format(x), {"value": "The quick brown fox jumps over the lazy dog."})
db.commit()
now = time.time()
print("Time Taken for not-commit-on-store: {}".format(now-then))