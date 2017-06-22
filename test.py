# -*- coding: UTF-8 -*-
import kodb
import os
import requests
import cProfile
import glob

# remove existing files
try:
	for file in glob.glob("ko-test.db/**", recursive=True):
		try:
			os.remove("file")
		except:
			pass
	os.rmdir("ko-test.db")
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
for x in range(0, 5000): test_table.store("{}".format(x), {"value": "The quick brown fox jumps over the lazy dog."})
now = time.time()
print("[KoDB] commit-on-store:\t\t\t {}".format(now-then))

then = time.time()
db.KO_NO_COMMIT = True
for x in range(5001, 10000): test_table.store("{}".format(x), {"value": "The quick brown fox jumps over the lazy dog."})
now = time.time()
print("[KoDB] No commit-on-store:\t\t {}".format(now-then))
db.commit()

then = time.time()
db.KO_NO_COMMIT = True
x = []
for item in test_table.get_all(): x.append(item)
now = time.time()
print("[KoDB] Entire Database traversal:\t {}".format(now-then))

db.KO_NO_COMMIT = False

def test():
	for x in range(0, 10000): test_table.store("{}".format(x), {"value": "The quick brown fox jumps over the lazy dog."})

# cProfile.run("test()")

try:
	os.remove("tinyDB-test.json")
except:
	pass

from tinydb import TinyDB, Query
db = TinyDB("tinyDB-test.json")
User = Query()
then = time.time()
for x in range(0, 5000): db.insert({"id": x, "value": "The quick brown fox jumps over the lazy dog."})
now = time.time()
print("[TinyDB] Insert:\t\t\t {}".format(now-then))

then = time.time()
data = [{"id": x, "value": "The quick brown fox jumps over the lazy dog."} for x in range(5001, 10000)]
db.insert_multiple(data)
now = time.time()
print("[TinyDB] Multiple insert:\t\t {}".format(now-then))


then = time.time()
db.KO_NO_COMMIT = True
x = []
for item in db.all(): x.append(item)
now = time.time()
print("[TinyDB] Entire Database traversal:\t {}".format(now-then))