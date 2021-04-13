import os
import uuid
import json
import warnings
import logging, traceback
from bson.json_util import dumps
from marshmallow import ValidationError
from pymongo import (
    MongoClient,
    ReturnDocument,
    errors
)



def deprecated(message):
  def deprecated_decorator(func):
      def deprecated_func(*args, **kwargs):
          warnings.warn("{} is a deprecated function. {}".format(func.__name__, message),
                        category=DeprecationWarning,
                        stacklevel=2)
          warnings.simplefilter('default', DeprecationWarning)
          return func(*args, **kwargs)
      return deprecated_func
  return deprecated_decorator



MONGO_ROOT_USERNAME = 'licensewaredev'
MONGO_ROOT_PASSWORD ='license123ware'
MONGO_DATABASE_NAME='db'
MONGO_HOSTNAME= 'localhost' #for a docker environment use 'mongodb' (service name)
MONGO_PORT=27017

os.environ['MONGO_DATABASE_NAME'] = MONGO_DATABASE_NAME
os.environ['MONGO_CONNECTION_STRING'] = f"mongodb://{MONGO_ROOT_USERNAME}:{MONGO_ROOT_PASSWORD}@{MONGO_HOSTNAME}:{MONGO_PORT}"


class MongoData:

    connection = MongoClient(os.getenv("MONGO_CONNECTION_STRING"))

    @classmethod
    def get_collection(cls, collection, db):
        c = cls()
        if db: return c.connection[db][collection]
        return c.connection[os.getenv("MONGO_DATABASE_NAME")][collection]


    @staticmethod
    def validate_data(schema, data):  
        # try:

        if isinstance(data, dict):
            data = schema().load(data)

        if isinstance(data, list):
            data = schema(many=True).load(data)

        return MongoData.append_uuid(data)
    
        # except ValidationError as e: 
        #     print(traceback.format_exc())
        #     print(str(e))


    @staticmethod
    def append_uuid(data):
        # Appends uuid4 ids for each dict in list
        # [{"field": "data", "field2": {"f": "d"}}, etc] 
        # => [{"_id": uuid4, "field": "data", "field2": {"f": "d"}}, etc] 

        if isinstance(data, list):
            data = [MongoData.uuid_to_dict(d) for d in data]
            return data
        return MongoData.uuid_to_dict(data)

    @staticmethod
    def uuid_to_dict(data):
        if isinstance(data, dict):
            if "_id" not in data.keys():
                data.update({"_id": str(uuid.uuid4())})
                return data
        return data


    @classmethod
    def insert(cls, *, schema, collection, data, db_name=None):
        """
            Insert validated documents in database.

            :schema     - Marshmallow schema class 
            :collection - collection name
            :data       - data in dict or list of dicts format
            :db_name    - specify other db if needed, by default is MONGO_DATABASE_NAME from .env
            
            returns a list of ids inserted in the database in the order they were added
        """
        c = cls()
           
        collection = c.get_collection(collection, db_name)
        data = MongoData.validate_data(schema, data)

        if isinstance(data, dict):
            return [collection.insert_one(data).inserted_id]
    
        if isinstance(data, list):
            return collection.insert_many(data).inserted_ids

        return data
    

    @classmethod
    def fetch(cls, *, collection, match, as_list=False, db_name=None):
        """
            Get data from mongo, based on match dict or string id.
            
            :collection - collection name
            :match      - id as string or dict filter
            :as_list    - return data found as list by default returns a generator
            :db_name    - specify other db if needed by default is MONGO_DATABASE_NAME from .env

            returns a generator of documents found or if as_list=True a list of documents found  

        """
        c = cls()
        
        by_id = False
        if isinstance(match, str): 
            match = {"_id": match}
            by_id = True
            
        collection = c.get_collection(collection, db_name)
        found_docs = collection.find(match)
        
        if as_list or by_id:
            data = [r for r in found_docs]
            if by_id: data = data[0] if len(data) == 1 else data
            return data
        
        return (r for r in found_docs) # generator
        

    @classmethod
    def update(cls, *, collection, match, new_data, db_name=None):
        """
            
            :collection  - collection name
            :match       - id as string or dict filter
            :new_data    - data dict which needs to be updated
            :db_name     - specify other db if needed by default is MONGO_DATABASE_NAME from .env
            
            returns number of modified documents

        """

        c = cls()

        if isinstance(match, str): 
            match = {"_id": match}
        
        collection = c.get_collection(collection, db_name)
        
        updated_docs_nbr = collection.update_many(
            filter=match,
            update={"$set": new_data},
            upsert=True
        ).modified_count
        
        return updated_docs_nbr


    
    @classmethod
    def delete(cls, *, collection, match, new_data, db_name=None):
        """
            
            :collection  - collection name
            :match       - id as string or dict filter
            :new_data    - data dict which needs to be updated
            :db_name     - specify other db if needed by default is MONGO_DATABASE_NAME from .env
            
            returns number of modified documents

        """

        c = cls()

        if isinstance(match, str): 
            match = {"_id": match}
        
        collection = c.get_collection(collection, db_name)
        
        updated_docs_nbr = collection.update_many(
            filter=match,
            update={"$mod": new_data},
            upsert=True
        ).modified_count
        
        return updated_docs_nbr



    # def delete(self, id=None, data=None, filter=None):
    #     pass





















class DataManagement:
    """
        CRUD operations on Mongo with data validation.
        
    """

    def __init__(
        self, 
        schema=None, 
        collection_name=None, 
        connection_string=None, 
        db_name=None
    ):
        
        self.connection = MongoClient(connection_string or os.getenv("MONGO_CONNECTION_STRING"))
        self.db_name = db_name or os.getenv("MONGO_DATABASE_NAME")
        self.database = self.connection[self.db_name]
        self.collection_name = collection_name or "data"
        self.document_schema = schema() if schema else None
        self.default_db = self.db_name


    def db(self, db_name, switch_db_default=True):
        """ 
            Create a new db in mongo then switch to default database after operation is executed.
            You can turn of swiching to default database by setting `switch_db_default` to False
        """

        # Ensure connection
        if "connection" not in self.__class__.__dict__:
            self.__class__.__init__(self)

        if not switch_db_default: self.default_db = db_name
        self.db_name = db_name
        self.database = self.connection[db_name]
        return self

    def collection(self, collection):
        """ Specify collection name"""
        self.collection_name = collection
        return self
    
    def schema(self, schema):
        """ Marshmallow class schema """
        self.document_schema = schema()
        return self
    
    def switch_to_default_db(self):
        if self.db_name != self.default_db:
            self.db(self.default_db)

    
    def insert_one(self, json_data):
        if not json_data:
            return {"status": "fail", "message": "No input data provided"}, 400
        try:
            data = self.document_schema.load(json_data)
            collection = self._collection_db(self.collection_name)
            collection.insert_one(data)
            return {"status": "success", "message": "Created new document.", "document": data}, 202
        except ValidationError as err:
            print(err)
            return err.messages, 422
        finally:
            self.switch_to_default_db()


    def insert_data(self, json_data):
        if not json_data:
            return {"status": "fail", "message": "No input data provided"}, 400
        try:
            data = self.document_schema.load(json_data)
            collection = self.database[self.collection]
            collection.insert_one(data)
            return {"status": "success", "message": "Created new document.", "document": data}, 202
        except ValidationError as err:
            logging.warning(err)
            return err.messages, 422
        finally:
            self.switch_to_default_db()


    def insert_many(self, json_data):
        if not json_data:
            return {"status": "fail", "message": "No input data provided"}, 400
        try:
            collection = self._collection_db(self.collection_name)
            collection.insert_many(json_data)
            return {"status": "success", "message": "Created new documents."}, 202
        except ValidationError as err:
            print(err)
            return err.messages, 422
        finally:
            self.switch_to_default_db()


    def get_by_id(self, _id):
        try:
            collection = self._collection_db(self.collection_name)
            result = collection.find_one({"_id": _id})
            result_data = self.document_schema.dump(result)
            if not result_data:
                return {"status": "fail", "message": "id is not found"}, 404
            return result_data, 200
        except errors.ConnectionFailure as e:
            return {"status": "fail", "message": "errors in connection"}, 500
        finally:
            self.switch_to_default_db()
        

    def get_one_with_filter(self, _filter):
        try:
            collection = self.database[self.collection]
            result = collection.find_one(_filter)
            if not result:
                return {"status": "fail", "message": "id is not found"}, 404
            result_data = self.document_schema.dump(result)
            return result_data, 200
        except errors.ConnectionFailure as e:
            return {"status": "fail", "message": "errors in connection"}, 500
        finally:
            self.switch_to_default_db()


    def get_all(self, _filter=None):
        try:
            print(f"Get all with filter {_filter}")
            collection = self._collection_db(self.collection_name)
            results = collection.find(_filter)
            results_data = []
            for result in results:
                results_data.append(self.document_schema.dump(result))
            if len(results_data) < 1:
                return {"status": "fail", "message": "id is not found"}, 404
            return results_data, 200
        except errors.ConnectionFailure as e:
            return {"status": "fail", "message": "errors in connection"}, 500
        finally:
            self.switch_to_default_db()


    def replace_one(self, json_data, _filter=None):
        try:
            if _filter is None:
                _filter = {"_id": json_data["_id"]}
            collection = self._collection_db(self.collection_name)
            result = collection.replace_one(_filter, self.document_schema.load(json_data),
                                            upsert=True)
            new = collection.find_one(_filter)
            return self.document_schema.dump(new), 200
        except errors.ConnectionFailure as e:
            return {"status": "fail", "message": "errors in connection"}, 500
        finally:
            self.switch_to_default_db()


    def update_one(self, json_data, _filter=None):
        try:
            if _filter is None:
                _filter = {"_id": json_data["_id"]}
            collection = self._collection_db(self.collection_name)
            old = collection.find_one(_filter)
            if old:
                for k in json_data:
                    if "_id" not in k:
                        if isinstance(k, list):
                            old[k].extend(json_data[k])
                        else:
                            old[k] = json_data[k]
                return self.replace_one(json_data=self.document_schema.dump(old))
            return self.insert_one(json_data)
        except errors.ConnectionFailure as e:
            return {"status": "fail", "message": "errors in connection"}, 500
        finally:
            self.switch_to_default_db()


    def delete_all(self, _filter):
        try:
            collection = self._collection_db(self.collection_name)
            result = collection.delete_many(_filter)
            return {"status": "success", "message": "all data deleted"}, 200
        except errors.ConnectionFailure as e:
            return {"status": "fail", "message": "errors in connection"}, 500
        finally:
            self.switch_to_default_db()


    def delete_one(self, _id):
        try:
            collection = self._collection_db(self.collection_name)
            result = collection.delete_one({"_id": _id})
            return {"status": "success", "message": "data deleted"}, 200
        except errors.ConnectionFailure as e:
            return {"status": "fail", "message": "errors in connection"}, 500
        finally:
            self.switch_to_default_db()


    def get_with_aggregation(self, pipeline):
        try:
            collection = self._collection_db(self.collection_name)
            results = collection.aggregate(pipeline, allowDiskUse=True)
            return json.loads(dumps(results)), 200
        except errors.ConnectionFailure as e:
            return {"status": "fail", "message": "errors in connection"}, 500
        finally:
            self.switch_to_default_db()


    def return_distinct_values(self, key, _filter=None):
        try:
            collection = self._collection_db(self.collection_name)
            results = collection.distinct(key=key, query=_filter)
            if len(results) < 1:
                return None, 404
            return results, 200
        except errors.ConnectionFailure as e:
            return {"status": "fail", "message": "errors in connection"}, 500
        finally:
            self.switch_to_default_db()


    def _collection_db(self, name):
        collection = self.database[name]
        return collection



try:
    dm = DataManagement()
except:
    # Environment variables not specified. 
    # Needed data must be provided on class instantiation
    dm = None 



