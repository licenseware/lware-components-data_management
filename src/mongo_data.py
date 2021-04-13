import os
import uuid
import logging, traceback
from marshmallow import ValidationError
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure


MONGO_ROOT_USERNAME = 'licensewaredev'
MONGO_ROOT_PASSWORD ='license123ware'
MONGO_DATABASE_NAME='db'
MONGO_HOSTNAME= 'localhost' #for a docker environment use 'mongodb' (service name)
MONGO_PORT=27017

os.environ['MONGO_DATABASE_NAME'] = MONGO_DATABASE_NAME
os.environ['MONGO_CONNECTION_STRING'] = f"mongodb://{MONGO_ROOT_USERNAME}:{MONGO_ROOT_PASSWORD}@{MONGO_HOSTNAME}:{MONGO_PORT}"




def failsafe(func):
    def func_wrapper(*args, **kwargs):
        try:
        
           return func(*args, **kwargs)
           
        except ConnectionFailure as err:
            logging.error(traceback.format_exc())
            return "errors with mongo connection"

        except ValidationError as err:
            logging.error(traceback.format_exc())
            return "errors on validation"

        except Exception as err:
            logging.error(traceback.format_exc())
            return str(err)

    return func_wrapper



class MongoData:
    """
        Wrapper on pymongo with added data validation based on marshmallow

        Needs for connection in environment variables the follwing keys:
        - MONGO_CONNECTION_STRING
        - MONGO_DATABASE_NAME

        Or you need to specify them on each method:
        - db_name
        - conn_string

        Import MongoData class:
        ```py

            from mongo_data import MongoData as m

        ```

        You can get access to pymongo methods directly using:

        ```py
           
            collection = m.get_collection('collection_name', 'db_name', 'conn_string')

            collection.distinct(key, filter=None)

        ```

    """

    # def __init__(self, ):
        


    @classmethod
    @failsafe
    def get_collection(cls, collection, db_name=None, conn_string=None):
        
        # raise Exception("Fail")
        
        conn_string = conn_string or os.getenv("MONGO_CONNECTION_STRING")
        connection  = MongoClient(conn_string)

        collection = connection[db_name or os.getenv("MONGO_DATABASE_NAME")][collection]
        
        # make available all methods from MongoData 
        # in collection instance
        # setattr(collection, "m", cls(collection, db_name, conn_string)) 

        return collection


    @staticmethod
    @failsafe
    def validate_data(schema, data):  

        # raise Exception("Fail")

        if isinstance(data, dict):
            data = schema().load(data)

        if isinstance(data, list):
            data = schema(many=True).load(data)

        return MongoData._add_uuid(data)
    
    
    @classmethod
    @failsafe
    def insert(cls, collection, schema, data, db_name=None, conn_string=None):
        """
            Insert validated documents in database.

            :collection - collection name
            :schema     - Marshmallow schema class 
            :data       - data in dict or list of dicts format
            :db_name    - specify other db if needed, by default is MONGO_DATABASE_NAME from .env
            
            returns a list of ids inserted in the database in the order they were added
        """
        m = cls()
           
        collection = m.get_collection(collection, db_name, conn_string)
        if not isinstance(collection, Collection): 
            return collection 

        data = MongoData.validate_data(schema, data)
        if isinstance(data, tuple):
            return data

        if isinstance(data, dict):
            return [collection.insert_one(data).inserted_id]
    
        if isinstance(data, list):
            return collection.insert_many(data).inserted_ids

        return data
    

    @classmethod
    @failsafe
    def fetch(cls, collection, match, as_list=False, db_name=None, conn_string=None):
        """
            Get data from mongo, based on match dict or string id.
            
            :collection - collection name
            :match      - id as string or dict filter query
            :as_list    - return data found as list by default returns a generator
            :db_name    - specify other db if needed by default is MONGO_DATABASE_NAME from .env

            returns a generator of documents found or if as_list=True a list of documents found  

        """
        m = cls()
        
        by_id = False
        if isinstance(match, str): 
            match = {"_id": match}
            by_id = True
            
        collection = m.get_collection(collection, db_name, conn_string)
        if not isinstance(collection, Collection): return collection 

        found_docs = collection.find(match)
        
        if as_list or by_id:
            data = [r for r in found_docs]
            if by_id: data = data[0] if len(data) == 1 else data
            return data
        
        return (r for r in found_docs) # generator
        

    @classmethod
    @failsafe
    def update(cls, collection, match, new_data, db_name=None, conn_string=None):
        """
           Update documents based on match query.
            
            :collection  - collection name
            :match       - id as string or dict filter query
            :new_data    - data dict which needs to be updated
            :db_name     - specify other db if needed by default is MONGO_DATABASE_NAME from .env
            
            returns number of modified documents

        """

        m = cls()

        if isinstance(match, str): 
            match = {"_id": match}
        
        collection = m.get_collection(collection, db_name, conn_string)
        if not isinstance(collection, Collection): return collection 

        updated_docs_nbr = collection.update_many(
            filter=match,
            update={"$set": new_data},
            upsert=True
        ).modified_count
        
        return updated_docs_nbr


    @classmethod
    @failsafe
    def delete(cls, collection, match, db_name=None, conn_string=None):
        """

           Delete documents based on match query.

            :collection  - collection name
            :match       - id as string or dict filter query
            :db_name     - specify other db if needed by default is MONGO_DATABASE_NAME from .env
            
            returns number of deleted documents

        """

        m = cls()

        if isinstance(match, str): 
            match = {"_id": match}
        
        collection = m.get_collection(collection, db_name, conn_string)
        if not isinstance(collection, Collection): return collection 

        deleted_docs_nbr = collection.delete_many(
            filter=match,
        ).deleted_count
        
        return deleted_docs_nbr

    
    @classmethod
    @failsafe
    def gather(cls, collection, pipeline, as_list=False, db_name=None, conn_string=None):
        """
           Fetch documents based on pipeline queries.
           https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline/
            
            :collection - collection name
            :pipeline   - list of query stages
            :as_list    - return data found as list by default returns a generator
            :db_name    - specify other db if needed by default is MONGO_DATABASE_NAME from .env
            
            returns a generator of documents found or a list if as_list=True

        """

        m = cls()

        collection = m.get_collection(collection, db_name, conn_string)
        if not isinstance(collection, Collection): return collection 

        found_docs = collection.aggregate(pipeline, allowDiskUse=True)

        if as_list:
            return [r for r in found_docs]
        
        return (r for r in found_docs) # generator
        

    #Utils

    @staticmethod
    def _add_uuid(data):
        # Appends uuid4 ids for each dict in list
        # [{"field": "data", "field2": {"f": "d"}}, etc] 
        # => [{"_id": uuid4, "field": "data", "field2": {"f": "d"}}, etc] 

        if isinstance(data, list):
            data = [MongoData._uuid_to_dict(d) for d in data]
            return data
        return MongoData._uuid_to_dict(data)

    @staticmethod
    def _uuid_to_dict(data):
        if isinstance(data, dict):
            if "_id" not in data.keys():
                return dict(data, **{"_id": str(uuid.uuid4())})
        return data
