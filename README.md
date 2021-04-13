# Data Management (Beta)

Wrapper on pymongo with added data validation based on marshmallow.


## Quickstart

Install this package using the following pip command:
```bash

$ pip3 install git+https://git@github.com/licenseware/lware-components-data_management.git

```

You can use `git+ssh` if you don't have ssh keys configured.


Needs for connection in environment variables the follwing keys:
- MONGO_DATABASE_NAME
- MONGO_CONNECTION_STRING

Or you need to specify bellow parameters on each method:
- db_name
- conn_string

Import MongoData class and marshmallow for validating input data:
```py

import uuid
import datetime as dt
from mongo_data import MongoData as m
from marshmallow import Schema, fields


# Create a schema
class DummySchema(Schema):
    _id = fields.Str(required=False)
    name = fields.Str(required=True)
    files = fields.List(fields.Str, required=False)
    age = fields.Integer(required=True, error_messages={"required": "Age is required."})
    birthdate = fields.DateTime(default=dt.datetime(2017, 9, 29))


class AnotherDummySchema(Schema):
    _id = fields.Str(required=False)
    name = fields.Str(required=True)
    

# Will be used later
id1 = str(uuid.uuid4())

# Let's add some dummy data
dummy_data = \
{
    "_id": id1,
    "name": "John",
    "files": ["f1", "f2"],
    "age": 20,
    "birthdate": dt.datetime(2021, 9, 29).strftime( '%Y-%m-%d %H:%M:%S' )
}

```

**If response from MongoData is of type `str` an error ocurred.** 


### INSERT ONE 

```py

id_list = m.insert(
    schema=DummySchema, 
    collection="testcollection", 
    data={
        "_id": id1,
        "name": "John Show",
        "files": ["f1", "f2"],
        "age": "20",
        "birthdate": dt.datetime(2021, 9, 29).strftime('%Y-%m-%d %H:%M:%S')
    }
)

```

### INSERT MULTIPLE

```py

id_list = m.insert(
    schema=AnotherDummySchema, 
    collection="testcollection", 
    data=[
        {
            # "_id": if not present uuid4 id will be generated
            "name": "Sun Lee"
        }, 
        {
            "_id": str(uuid.uuid4()),
            "name": "Leo Day"
        },
        {
            "_id": str(uuid.uuid4()),
            "name": "Horhe Trsa"
        },
    ]
)

```

### FETCH ONE 
```py

data_dict = m.fetch(match=id1, collection="testcollection")

```
You will receive a dictionary which matched `{"_id": id1}`


### FETCH MULTIPLE
```py

doc_list = m.fetch(
    match = {'name': 'John Show'},
    collection = "testcollection",
    as_list = True
)

```
By default `fetch` returns a generator, set `as_list = True` and you will receive a list.


### FETCH MULTIPLE WITH AGGREGATION
```py

doc_list = m.aggregate(
    collection = "testcollection",
    pipeline   = [{ "$match": {'name': 'John'} }],
    as_list = True
)

```
By default `aggregate` returns a generator, set `as_list = True` and you will receive a list.


### UPDATE ONE
```py

modified_doc_nbr = m.update(
    collection = "testcollection",
    match      = id1,
    new_data   = {'name': 'New John Show'}
)

```


### UPDATE MULTIPLE
```py

modified_doc_nbr = m.update(
    collection = "testcollection",
    match      = {'name': 'John Show'},
    new_data   = {'name': 'GOT John Show'}
)

```
All documents with `match` (filter) found will be updated. 
   

### DELETE ONE
```py

deleted_docs_nbr = m.delete(
    match = id1,
    collection = "testcollection",
)

```

### DELETE MULTIPLE
```py

deleted_docs_nbr = m.delete(
    match      = {'name': 'GOT John Show'},
    collection = "testcollection",
)

```
All documents with `match` (filter) found will be deleted. 



### CUSTOM METHODS

You can access any methods from pymongo by getting the collection:

```py
    
collection = m.get_collection('collection_name', 'db_name', 'conn_string')

collection.distinct(key, filter=None)

```

