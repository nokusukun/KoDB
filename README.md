# KoDB
YAML based nosql database library.

## Usage
---
Grab the latest version in `dist` unpack and run... 

```python setup.py install```

#### Using the DB

To initalize or open a database it's as straightforward as calling the KoDB class.
```python
db = kodb.KoDB("ko-test.db") # Safely loads the database.
db = kodb.KoDB("ko-test.db", load_type="load") # Serializes all of the items in the database.
```
The Database can be used as is since the database object is the default table.
By adding the `load_type="load"` option, this allows KoDB to load and store serialized python objects.

#### Data Storage
```python
data = 	{
	    "title": "delectus aut autem",
	    "completed": False
	  	}
db.store("todo\_1", data)	
```
This stores the data with `todo_1` as the Document ID.

Tables can be created by calling the `.table` function.
```python
my_table = db.table("mytable")
my_table.store("kampraaf", {"type": "wooden table", "price": {"value": 150, "currency": "EUR"}})
```

#### Retrieval
There are multiple ways of accessing data.
```python
# returns the document with the todo\_1 id
db.get("todo\_1")

# returns documents that matches the lambda expression.
my_table.query(lambda x: x.price["value"] > 500) 

# Returns a list generator that returns all of the table's items.
db.get_all()
```

#### Others
```python
# Returns a list of a table's IDs.
my_table.items()

# Retuns a list of the database's tables.
db.tables()

# Checks if a document exists.
db.exits("todo_1")

# Commit the database to the file.
db.commit()

# Closes the Database.
db.close
```

_See: test.py for more use cases._

### Todo
	- Commit History
	- Dropping of tables
	- Clean and Rebuilding of tables