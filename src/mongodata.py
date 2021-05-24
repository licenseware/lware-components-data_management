"""

Abstraction and validation of inserted data in mongodb


import licenseware.mongodata as m
or
from licenseware import mongodata as m

Available functions:
- get_collection
- insert
- fetch
- update
- delete
- aggregate

Needs the following environment variables:
- MONGO_DATABASE_NAME
- MONGO_CONNECTION_STRING
- MONGO_COLLECTION_NAME (optional)

"""

import os
from uuid import UUID
from pymongo import MongoClient
from pymongo.collection import Collection
from bson.json_util import dumps
from bson.objectid import ObjectId
import json
from .decorators import failsafe


#Utils

@failsafe
def validate_data(schema, data): 
    """
        Using Marshmallow schema class to validate data (dict or list of dicts) 
    """ 

    if isinstance(data, dict):
        data = schema().load(data)

    if isinstance(data, list):
        data = schema(many=True).load(data)

    return data


def valid_uuid(uuid_string):
    try:
        UUID(uuid_string)
        return True
    except ValueError:
        return False

def valid_object_id(oid_string):
    try:
        ObjectId(oid_string)
        return True
    except:
        return False

def _parse_oid(oid):
    if isinstance(oid, ObjectId):
        return json.loads(dumps(oid))['$oid']
    return oid

def _parse_doc(doc):
    if not isinstance(doc, dict): return doc
    if not "_id" in doc: return doc

    return dict(doc, **{"_id": _parse_oid(doc["_id"])})

def _parse_match(match):
    oid, uid, key = None, None, None
    if isinstance(match, str): 
        if valid_uuid(match): 
            match = {"_id": match}
            oid, uid = False, True
        elif valid_object_id(match):
            match = {"_id": ObjectId(match)}
            oid, uid = True, False
        else:
            key = match
    return oid, uid, key, match




def _append_query(dict_):
    """ 
        Force append to mongo document 
    """
    
    dict_.pop("_id", None)
    
    q = {'$set': {}, '$addToSet': {}}
    for k in dict_:
        
        if isinstance(dict_[k], str):
            q['$set'].update({k:dict_[k]})
            
        if isinstance(dict_[k], dict):
            for key in dict_[k]:
                key_ = ".".join([k, key]) # files.status
                q['$set'].update({key_:dict_[k][key]})
                
        if isinstance(dict_[k], list): 
            q['$addToSet'].update({k:{}})
            q['$addToSet'][k].update({ "$each": dict_[k]})
     
    if not q['$addToSet']: del q['$addToSet'] 
    if not q['$set']: del q['$set'] 

    # print(q)

    return q or dict_



#Mongo

default_db = os.getenv("MONGO_DB_NAME") or os.getenv("MONGO_DATABASE_NAME") or "db"
default_collection = os.getenv("MONGO_COLLECTION_NAME") or "Data"
mongo_connection = MongoClient(os.getenv("MONGO_CONNECTION_STRING"))


@failsafe
def get_collection(collection, db_name=None):
    """
        Gets the collection on which mongo CRUD operations can be performed

        If something fails will return a string with the error message.
    """
    
    collection = collection or default_collection
    db_name = db_name or default_db

    # print(db_name, collection, os.getenv("MONGO_CONNECTION_STRING"), mongo_connection)

    if not all([db_name, collection, mongo_connection]) :
        raise Exception("Can't create connection to mongo.")

    collection = mongo_connection[db_name][collection]
    
    return collection


@failsafe
def insert(schema, collection, data, db_name=None):
    """
        Insert validated documents in database.

        :schema     - Marshmallow schema class used to validate `data`
        :collection - collection name, schema name will be taken if not present
        :data       - data in dict or list of dicts format
        :db_name    - specify other db if needed, by default is MONGO_DATABASE_NAME from .env

        returns a list of ids inserted in the database in the order they were added
        If something fails will return a string with the error message.
    """

    collection = get_collection(collection, db_name)
    if not isinstance(collection, Collection): 
        return collection 

    data = validate_data(schema, data)
    if isinstance(data, str): return data

    if isinstance(data, dict):
        inserted_id = _parse_oid(collection.insert_one(data).inserted_id)
        return [inserted_id]

    if isinstance(data, list):
        inserted_ids = collection.insert_many(data).inserted_ids
        return [_parse_oid(oid) for oid in inserted_ids]

    raise Exception(f"Can't interpret validated data: {data}")




@failsafe
def fetch(match, collection, as_list=True, db_name=None):
    """
        Get data from mongo, based on match dict or string id.
        
        :match      - _id as string (will return a dict)
                    - mongo dict filter (will return a list of results)
                    - field_name as string (will return distinct values for that field)

        :collection - collection name
        :as_list    - set as_list to false to get a generator
        :db_name    - specify other db if needed by default is MONGO_DATABASE_NAME from .env
        
        If something fails will return a string with the error message.

    """
    
    oid, uid, key, match = _parse_match(match)

    collection = get_collection(collection, db_name)
    if not isinstance(collection, Collection): return collection 

    if oid or uid:
        found_docs = collection.find(match)
        doc = []
        if found_docs: doc = list(found_docs)[0]
        if oid: doc = _parse_doc(doc)
        return doc

    if key: 
        found_docs = collection.distinct(key)
    else:
        found_docs = collection.find(match)
        

    if as_list: 
        return [_parse_doc(doc) for doc in found_docs]
        
    return (_parse_doc(doc) for doc in found_docs)    
        


@failsafe
def update(schema, match, new_data, collection, append=False, db_name=None):
    """
        Update documents based on match query.
        
        :schema      - Marshmallow schema class
        :match       - id as string or dict filter query
        :new_data    - data dict which needs to be updated
        :collection  - collection name
        :append      - if true will APPEND new data to existing fields, if false will SET new data to fields  
        :db_name     - specify other db if needed by default is MONGO_DATABASE_NAME from .env
        
        returns number of modified documents

        If something fails will return a string with the error message.

    """

    _, _, _, match = _parse_match(match)
    
    collection = get_collection(collection, db_name)
    if not isinstance(collection, Collection): return collection 

    
    new_data = validate_data(schema, new_data)
    if isinstance(new_data, str): return new_data

    updated_docs_nbr = collection.update_many(
        filter={"_id": match["_id"]} if "_id" in match else match,
        update=_append_query(new_data) if append else {"$set": new_data},
        upsert=True
    ).modified_count

    return updated_docs_nbr




@failsafe
def delete(match, collection, db_name=None):
    """

        Delete documents based on match query.

        :match       - id as string or dict filter query, 
                     - if match == collection: will delete collection           
        :collection  - collection name
        :db_name     - specify other db if needed by default is MONGO_DATABASE_NAME from .env
        
        returns number of deleted documents
        
        If something fails will return a string with the error message.

    """
    _, _, key, match = _parse_match(match)

    col = get_collection(collection, db_name)
    if not isinstance(col, Collection): return col 

    if match == collection and key == match: 
        res = col.drop()
        return 1 if res is None else 0

    deleted_docs_nbr = col.delete_many(
        filter=match,
    ).deleted_count
    
    return deleted_docs_nbr



@failsafe
def aggregate(pipeline, collection, as_list=True, db_name=None):
    """
        Fetch documents based on pipeline queries.
        https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline/
        
        :pipeline   - list of query stages
        :collection - collection name
        :as_list    - set as_list to false to get a generator
        :db_name    - specify other db if needed by default is MONGO_DATABASE_NAME from .env
                
        If something fails will return a string with the error message.

    """

    collection = get_collection(collection, db_name)
    if not isinstance(collection, Collection): return collection 

    found_docs = collection.aggregate(pipeline, allowDiskUse=True)

    if as_list: return [_parse_doc(doc) for doc in found_docs]
        
    return (_parse_doc(doc) for doc in found_docs)    
    



    