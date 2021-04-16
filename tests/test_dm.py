import pytest
from assertpy import assert_that
import os
from src.mongodata import MongoData
from marshmallow import Schema, fields
import uuid
import datetime as dt


# Add init file in src for tests
# You need a running mongodb instance running 
# mongo express is useful for visualizing data

MONGO_ROOT_USERNAME = 'licensewaredev'
MONGO_ROOT_PASSWORD ='license123ware'
MONGO_DATABASE_NAME='db'
MONGO_HOSTNAME= 'localhost' #for a docker environment use 'mongodb' (service name)
MONGO_PORT=27017

os.environ['MONGO_DATABASE_NAME'] = MONGO_DATABASE_NAME
os.environ['MONGO_CONNECTION_STRING'] = f"mongodb://{MONGO_ROOT_USERNAME}:{MONGO_ROOT_PASSWORD}@{MONGO_HOSTNAME}:{MONGO_PORT}"

#print(os.getenv("MONGO_DATABASE_NAME"))
#print(os.getenv("MONGO_CONNECTION_STRING"))

# #print(dir(dm))


class DummySchema(Schema):
    _id = fields.Str(required=False)
    name = fields.Str(required=True)
    files = fields.List(fields.Str, required=False)
    age = fields.Integer(required=True, error_messages={"required": "Age is required."})
    birthdate = fields.DateTime(default=dt.datetime(2017, 9, 29))



class AnotherDummySchema(Schema):
    _id = fields.Str(required=False)
    name = fields.Str(required=True)


# Id to be used by test funcs
id1 = str(uuid.uuid4())

dummy_data = \
{
    # "_id": id1,
    "name": "John",
    "files": ["f1", "f2"],
    "age": 20,
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
    
    assert_that(id_list).is_instance_of(list).contains_only(id1)


def test_md_fetch_one_with_id():

    data_dict = MongoData.fetch(
        match = id1,
        collection="testcollection"
    )
    
    # print("\n\\ test_md_fetch_with_id:: data_dict, id1", data_dict, list(data_dict), id1)

    assert_that(data_dict).contains_entry({"_id": id1})
    assert_that(data_dict['age']).is_instance_of(int)
    assert_that(data_dict['birthdate']).is_instance_of(dt.datetime)



def test_md_insert_many():
    
    id_list = MongoData.insert(
        schema=DummySchema, 
        collection="testcollection", 
        data=[
            dummy_data, 
            dummy_data,
            dict(dummy_data, **{"_id": str(uuid.uuid4())}),
        ]
    )
    
    #print("\ntest_md_insert_many:: id_list", id_list)

    assert_that(id_list).is_not_none().is_length(3)




def test_md_fetch_all_with_match():

    datagen = MongoData.fetch(
        match = {'name': 'John Show'},
        collection="testcollection",
        
    )
    
    # print("\ntest_md_fetch_all_with_match:: datagen", datagen, type(datagen))

    assert_that(len(list(datagen))).is_greater_than_or_equal_to(1)



def test_md_update_one_with_id():

    response = MongoData.update(
        schema= AnotherDummySchema,
        collection = "testcollection",
        match      = id1,
        new_data   = {'name': 'New John Show'}
    )
    
    #print(response)

    assert_that(response).is_equal_to(1)



def test_md_update_all_with_match():
    
    import re
    regx = re.compile("^New John", re.IGNORECASE)

    response = MongoData.update(
        schema= AnotherDummySchema,
        collection = "testcollection",
        match      = {'name': regx},
        new_data   = {'name': 'John'}
    )

    #print(response)

    assert_that(response).is_greater_than_or_equal_to(1)



def test_md_fetch_with_agreggate():

    doc_list = MongoData.aggregate(
        collection = "testcollection",
        pipeline   = [{ "$match": {'name': 'John'} }],
        as_list = True        
    )

    # print(doc_list)

    assert_that(doc_list).is_instance_of(list).is_not_empty()


def test_md_fetch_distinct():

    doc_list = MongoData.fetch(
        match = 'name',
        collection = "testcollection",
        as_list = True        
    )

    # print(doc_list)

    assert_that(doc_list).is_instance_of(list).is_not_empty()




def test_md_delete_by_id():

    deleted_docs_nbr = MongoData.delete(
        collection = "testcollection",
        match      = id1,
    )

    #print(deleted_docs_nbr)

    assert_that(deleted_docs_nbr).is_equal_to(1)



def test_md_delete_with_query():

    deleted_docs_nbr = MongoData.delete(
        collection = "testcollection",
        match      = {'name': 'John'},
    )

    #print(deleted_docs_nbr)

    assert_that(deleted_docs_nbr).is_greater_than_or_equal_to(1)























# def test_insert_one_envset_with_chain_assignment():

#     #print("Current: ", dm.db_name)

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
    
#     #print("New: ", dm.db_name)
#     #print(response)

#     assert_that(status_code).is_in(202)
#     assert_that(response).contains_entry({'status': 'success'})



# def test_insert_one_envset_with_chain_assignment_defaultdb():

#     #print("Should be `db`: ", dm.db_name)

#     response, status_code = (
#         dm.schema(DummySchema)
#         .collection("chain_collection")
#         .insert_one({ 
#             "_id": str(uuid.uuid4()), 
#             "name": "dm db=default(db) collection=chain_collection" 
#         })

#     )
    
#     #print(response)

#     assert_that(status_code).is_in(202)
#     assert_that(response).contains_entry({'status': 'success'})



# def test_insert_one_envset_with_chain_assignment_swichdb():

#     #print("Should be `db`: ", dm.db_name)

#     response, status_code = (
#         dm.schema(DummySchema)
#         .collection("chain_collection")
#         .insert_one({ 
#             "_id": str(uuid.uuid4()), 
#             "name": "dm db=db collection=chain_collection" 
#         })

#     )
    
#     #print(response)

#     assert_that(status_code).is_in(202)
#     assert_that(response).contains_entry({'status': 'success'})



# def test_insert_one_envset_with_schema_collection():

#     dm = DataManagement(schema=DummySchema, collection_name="mycollection")

#     data = { 
#         "_id": str(uuid.uuid4()), 
#         "name": "added in mycollection with local instance of dm" 
#     }

#     response, status_code = dm.insert_one(data)

#     #print(response)

#     assert_that(status_code).is_in(202)
#     assert_that(response).contains_entry({'status': 'success'})
    


# def test_insert_one_envset_with_schema_collection_db():

#     dm = DataManagement(schema=DummySchema, collection_name="mycollection", db_name="newdb")

#     response, status_code = dm.insert_one({ 
#                                     "_id": str(uuid.uuid4()), 
#                                     "name": "added in mycollection with local instance of db newdb" 
#                                 })

#     #print(response)

#     assert_that(status_code).is_in(202)
#     assert_that(response).contains_entry({'status': 'success'})