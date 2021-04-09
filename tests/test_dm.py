import pytest
from assertpy import assert_that
import os
from src.data_management import DataManagement
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



class DummySchema(Schema):
    _id = fields.Str(required=True)
    test_field = fields.Str(required=True)


dummy_json_data = { 
    "_id": str(uuid.uuid4()), 
    "test_field": "does it work?" 
}


def test_insert_one_envset_with_chain_assignment():

    dm = DataManagement()

    response, status_code = (
        dm.db("chaindb")
        .schema(DummySchema)
        .collection("chain_collection")
        .insert_one(dummy_json_data)

    )
    
    print(response)

    assert_that(status_code).is_in(202)
    assert_that(response).contains_entry({'status': 'success'})



def test_insert_one_envset_with_chain_assignment_dbdefault():

    dm = DataManagement()

    response, status_code = (
        dm
        .schema(DummySchema)
        .collection("chain_collection")
        .insert_one(dummy_json_data)

    )
    
    print(response)

    assert_that(status_code).is_in(202)
    assert_that(response).contains_entry({'status': 'success'})






def test_insert_one_envset_with_schema_collection():

    dm = DataManagement(schema=DummySchema, collection_name="mycollection")

    response, status_code = dm.insert_one(dummy_json_data)

    print(response)

    assert_that(status_code).is_in(202)
    assert_that(response).contains_entry({'status': 'success'})
    


def test_insert_one_envset_with_schema_collection_db():

    dm = DataManagement(schema=DummySchema, collection_name="mycollection", db_name="newdb")

    response, status_code = dm.insert_one(dummy_json_data)

    print(response)

    assert_that(status_code).is_in(202)
    assert_that(response).contains_entry({'status': 'success'})
