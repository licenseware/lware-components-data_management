# Data Management (Beta)

This library simplifies CRUD + Data Validation on MongoDB.


## Quickstart

Install this package using the following pip command:
```bash

$ pip3 install git+https://git@github.com/licenseware/lware-components-data_management.git

```
You can use `git+ssh` if you don't have ssh keys configured.


Set bellow environment variables:
- `MONGO_DATABASE_NAME`
- `MONGO_CONNECTION_STRING`


You can import `DataManagement` class and set the needed parameters on instantiation:
```py

import uuid
from marshmallow import Schema, fields
from data_management import DataManagement



class DummySchema(Schema):
    _id = fields.Str(required=True)
    test_field = fields.Str(required=True)


dm = DataManagement(
    schema = DummySchema, 
    collection_name = "my_collection_name"
    connection_string=None, # not needed if you have MONGO_CONNECTION_STRING in environment variables
    db_name=None            # not needed if you have MONGO_DATABASE_NAME in environment variables
)


data = { 
    "_id": str(uuid.uuid4()), 
    "test_field": "some data" 
}

#add validated data to mongodb
dm.insert_one(data)


```    

Another way you can use this class is to import `dm` instance of `DataManagement` class:

```py

import uuid
from marshmallow import Schema, fields
from data_management import dm #this


class DummySchema(Schema):
    _id = fields.Str(required=True)
    test_field = fields.Str(required=True)


data = { 
    "_id": str(uuid.uuid4()), 
    "test_field": "some data" 
}


#This will add collection to default MONGO_DATABASE_NAME 
response, status_code = dm.schema(DummySchema).collection("mycollection").insert_one(data)


```

You can also add a collection to a new database:
```py
response, status_code = (
    dm.db("newdb")
    .schema(DummySchema)        # !!! if not specified will use previous schema
    .collection("mycollection") # !!! if not specified will use previous collection
    .insert_one(data)
)
```

After the `insert_one` operation is executed database will be reset to default database.
You can override this behaviour by setting `switch_db_default` parameter to `False`.
```py
dm.db("newdb", switch_db_default=False)
```


If `dm` object is None it means that `DataManagement` class coulnd't be instantiated on import (probably environment variables are missing). 


Methods available:

```py
'collection', 'db', 'delete_all', 'delete_one', 'get_all', 'get_by_id', 'get_one_with_filter', 'get_with_aggregation', 'insert_data', 'insert_many', 'insert_one', 'replace_one', 'return_distinct_values', 'update_one'
```