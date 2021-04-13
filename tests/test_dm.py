import pytest
from assertpy import assert_that
import os
from src.data_management import DataManagement, dm, MongoData
from marshmallow import Schema, fields
import uuid
import datetime as dt


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
    _id = fields.Str(required=False)
    name = fields.Str(required=True)
    files = fields.List(fields.Str, required=False)
    age = fields.Integer(required=True, error_messages={"required": "Age is required."})
    birthdate = fields.DateTime(default=dt.datetime(2017, 9, 29))


# Id to be used by test funcs
id1 = str(uuid.uuid4())

dummy_data = \
{
    "_id": id1,
    "name": "John",
    "files": ["f1", "f2"],
    "age": "20",
    "birthdate": dt.datetime(2021, 9, 29).strftime( '%Y-%m-%d %H:%M:%S' )
}


def test_md_insert_one():
    
    id_list = MongoData.insert(
        schema=DummySchema, 
        collection="testcollection", 
        data={
            "_id": id1,
            "name": "John Show",
            "files": ["f1", "f2"],
            "age": "20",
            "birthdate": dt.datetime(2021, 9, 29).strftime( '%Y-%m-%d %H:%M:%S' )
        }
    )
    
    assert_that(id_list).is_not_none().contains_only(id1) 



def test_md_insert_many():
    
    id_list = MongoData.insert(
        schema=DummySchema, 
        collection="testcollection", 
        data=[
            dict(dummy_data, **{"_id": str(uuid.uuid4())}), 
            dict(dummy_data, **{"_id": str(uuid.uuid4())}),
            dict(dummy_data, **{"_id": str(uuid.uuid4())}),
        ]
    )
    
    print(id_list)

    assert_that(id_list).is_not_none().is_length(3)





def test_md_fetch_one_with_id():

    data_dict = MongoData.fetch(
        match = id1,
        collection="testcollection"
    )
    
    print(data_dict)

    assert_that(data_dict).contains_entry({"_id": id1})




def test_md_fetch_all_with_match():

    response = MongoData.fetch(
        match = {'name': 'John Show'},
        collection="testcollection"
    )
    
    print(list(response))



def test_md_update_one_with_id():

    response = MongoData.update(
        collection = "testcollection",
        match      = id1,
        new_data   = {'name': 'New John Show'}
    )
    
    print(response)


def test_md_update_all_with_match():

    response = MongoData.update(
        collection ="testcollection",
        match      = {'name': 'John Show'},
        new_data   = {'name': 'GOT John Show'}
    )

    print(response)










# def test_insert_one_envset_with_chain_assignment():

#     print("Current: ", dm.db_name)

#     response, status_code = (
#         dm.db("chaindb") #switch_db_default=False
#         .schema(DummySchema)
#         .collection("chain_collection")
#         .insert_one({ 
#             "_id": str(uuid.uuid4()), 
#             "name": "dm db=chaindb collection=chain_collection" 
#         })
#     )

#     #dm.switch_to_default_db()
    
#     print("New: ", dm.db_name)
#     print(response)

#     assert_that(status_code).is_in(202)
#     assert_that(response).contains_entry({'status': 'success'})



# def test_insert_one_envset_with_chain_assignment_defaultdb():

#     print("Should be `db`: ", dm.db_name)

#     response, status_code = (
#         dm.schema(DummySchema)
#         .collection("chain_collection")
#         .insert_one({ 
#             "_id": str(uuid.uuid4()), 
#             "name": "dm db=default(db) collection=chain_collection" 
#         })

#     )
    
#     print(response)

#     assert_that(status_code).is_in(202)
#     assert_that(response).contains_entry({'status': 'success'})



# def test_insert_one_envset_with_chain_assignment_swichdb():

#     print("Should be `db`: ", dm.db_name)

#     response, status_code = (
#         dm.schema(DummySchema)
#         .collection("chain_collection")
#         .insert_one({ 
#             "_id": str(uuid.uuid4()), 
#             "name": "dm db=db collection=chain_collection" 
#         })

#     )
    
#     print(response)

#     assert_that(status_code).is_in(202)
#     assert_that(response).contains_entry({'status': 'success'})



# def test_insert_one_envset_with_schema_collection():

#     dm = DataManagement(schema=DummySchema, collection_name="mycollection")

#     data = { 
#         "_id": str(uuid.uuid4()), 
#         "name": "added in mycollection with local instance of dm" 
#     }

#     response, status_code = dm.insert_one(data)

#     print(response)

#     assert_that(status_code).is_in(202)
#     assert_that(response).contains_entry({'status': 'success'})
    


# def test_insert_one_envset_with_schema_collection_db():

#     dm = DataManagement(schema=DummySchema, collection_name="mycollection", db_name="newdb")

#     response, status_code = dm.insert_one({ 
#                                     "_id": str(uuid.uuid4()), 
#                                     "name": "added in mycollection with local instance of db newdb" 
#                                 })

#     print(response)

#     assert_that(status_code).is_in(202)
#     assert_that(response).contains_entry({'status': 'success'})