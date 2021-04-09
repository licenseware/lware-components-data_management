import pytest
from assertpy import assert_that
import os
from src.data_management import DataManagement, dm
from marshmallow import Schema, fields
import uuid

# You need a running mongodb instance running 
# mongo express is useful for visualizing data

MONGO_ROOT_USERNAME = 'licensewaredev'
MONGO_ROOT_PASSWORD ='license123ware'
MONGO_DATABASE_NAME='db'
MONGO_HOSTNAME= 'localhost' #for a docker environment use 'mongodb' (service name)
MONGO_PORT=27017

os.environ['MONGO_DATABASE_NAME'] = MONGO_DATABASE_NAME
os.environ['MONGO_CONNECTION_STRING'] = f"mongodb://{MONGO_ROOT_USERNAME}:{MONGO_ROOT_PASSWORD}@{MONGO_HOSTNAME}:{MONGO_PORT}"

print(os.getenv("MONGO_DATABASE_NAME"))
print(os.getenv("MONGO_CONNECTION_STRING"))

# print(dir(dm))


class DummySchema(Schema):
    _id = fields.Str(required=True)
    test_field = fields.Str(required=True)


def test_insert_one_envset_with_chain_assignment():

    print("Current: ", dm.db_name)

    response, status_code = (
        dm.db("chaindb") #switch_db_default=False
        .schema(DummySchema)
        .collection("chain_collection")
        .insert_one({ 
            "_id": str(uuid.uuid4()), 
            "test_field": "dm db=chaindb collection=chain_collection" 
        })
    )

    #dm.switch_to_default_db()
    
    print("New: ", dm.db_name)
    print(response)

    assert_that(status_code).is_in(202)
    assert_that(response).contains_entry({'status': 'success'})



def test_insert_one_envset_with_chain_assignment_defaultdb():

    print("Should be `db`: ", dm.db_name)

    response, status_code = (
        dm.schema(DummySchema)
        .collection("chain_collection")
        .insert_one({ 
            "_id": str(uuid.uuid4()), 
            "test_field": "dm db=default(db) collection=chain_collection" 
        })

    )
    
    print(response)

    assert_that(status_code).is_in(202)
    assert_that(response).contains_entry({'status': 'success'})



def test_insert_one_envset_with_chain_assignment_swichdb():

    print("Should be `db`: ", dm.db_name)

    response, status_code = (
        dm.schema(DummySchema)
        .collection("chain_collection")
        .insert_one({ 
            "_id": str(uuid.uuid4()), 
            "test_field": "dm db=db collection=chain_collection" 
        })

    )
    
    print(response)

    assert_that(status_code).is_in(202)
    assert_that(response).contains_entry({'status': 'success'})



def test_insert_one_envset_with_schema_collection():

    dm = DataManagement(schema=DummySchema, collection_name="mycollection")

    data = { 
        "_id": str(uuid.uuid4()), 
        "test_field": "added in mycollection with local instance of dm" 
    }

    response, status_code = dm.insert_one(data)

    print(response)

    assert_that(status_code).is_in(202)
    assert_that(response).contains_entry({'status': 'success'})
    


def test_insert_one_envset_with_schema_collection_db():

    dm = DataManagement(schema=DummySchema, collection_name="mycollection", db_name="newdb")

    response, status_code = dm.insert_one({ 
                                    "_id": str(uuid.uuid4()), 
                                    "test_field": "added in mycollection with local instance of db newdb" 
                                })

    print(response)

    assert_that(status_code).is_in(202)
    assert_that(response).contains_entry({'status': 'success'})