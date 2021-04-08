from marshmallow import ValidationError
from pymongo import errors
import json, logging
from bson.json_util import dumps

from .. import mongo_db as mongo_connection


class DataManagement:
    def __init__(self, _schema=None, collection="data"):
        self.connection = mongo_connection
        self.database = self.connection["db"]
        self.collection = collection
        self.document_schema = _schema() if _schema is not None else None

    def collection_db(self, name):
        collection = self.database[name]
        return collection

    def get_by_id(self, _id):
        try:
            collection = self.collection_db(self.collection)
            result = collection.find_one({"_id": _id})
            result_data = self.document_schema.dump(result)
            if not result_data:
                return {"status": "fail", "message": "id is not found"}, 404
            return result_data, 200
        except errors.ConnectionFailure as e:
            return {"status": "fail", "message": "errors in connection"}, 500


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

    def get_all(self, _filter=None):
        try:
            print(f"Get all with filter {_filter}")
            collection = self.collection_db(self.collection)
            results = collection.find(_filter)
            results_data = []
            for result in results:
                results_data.append(self.document_schema.dump(result))
            if len(results_data) < 1:
                return {"status": "fail", "message": "id is not found"}, 404
            return results_data, 200
        except errors.ConnectionFailure as e:
            return {"status": "fail", "message": "errors in connection"}, 500

    def insert_one(self, json_data):
        if not json_data:
            return {"status": "fail", "message": "No input data provided"}, 400
        try:
            data = self.document_schema.load(json_data)
            collection = self.collection_db(self.collection)
            collection.insert_one(data)
        except ValidationError as err:
            print(err)
            return err.messages, 422
        return {"status": "success", "message": "Created new document.", "document": data}, 202

    def insert_many(self, json_data):
        if not json_data:
            return {"status": "fail", "message": "No input data provided"}, 400
        try:
            collection = self.collection_db(self.collection)
            collection.insert_many(json_data)
        except ValidationError as err:
            print(err)
            return err.messages, 422
        return {"status": "success", "message": "Created new documents."}, 202



    def insert_data(self, json_data):
        if not json_data:
            return {"status": "fail", "message": "No input data provided"}, 400
        try:
            data = self.document_schema.load(json_data)
            collection = self.database[self.collection]
            collection.insert_one(data)
        except ValidationError as err:
            logging.warning(err)
            return err.messages, 422
        return {"status": "success", "message": "Created new document.", "document": data}, 202


    def replace_one(self, json_data, _filter=None):
        try:
            if _filter is None:
                _filter = {"_id": json_data["_id"]}
            collection = self.collection_db(self.collection)
            result = collection.replace_one(_filter, self.document_schema.load(json_data),
                                            upsert=True)
            new = collection.find_one(_filter)
            return self.document_schema.dump(new), 200
        except errors.ConnectionFailure as e:
            return {"status": "fail", "message": "errors in connection"}, 500

    def update_one(self, json_data, _filter=None):
        try:
            if _filter is None:
                _filter = {"_id": json_data["_id"]}
            collection = self.collection_db(self.collection)
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

    def delete_all(self, _filter):
        try:
            collection = self.collection_db(self.collection)
            result = collection.delete_many(_filter)
            return {"status": "success", "message": "all data deleted"}, 200
        except errors.ConnectionFailure as e:
            return {"status": "fail", "message": "errors in connection"}, 500

    def delete_one(self, _id):
        try:
            collection = self.collection_db(self.collection)
            result = collection.delete_one({"_id": _id})
            return {"status": "success", "message": "data deleted"}, 200
        except errors.ConnectionFailure as e:
            return {"status": "fail", "message": "errors in connection"}, 500

    def get_with_aggregation(self, pipeline):
        try:
            collection = self.collection_db(self.collection)
            results = collection.aggregate(pipeline, allowDiskUse=True)
            return json.loads(dumps(results)), 200
        except errors.ConnectionFailure as e:
            return {"status": "fail", "message": "errors in connection"}, 500

    def return_distinct_values(self, key, _filter=None):
        try:
            collection = self.collection_db(self.collection)
            results = collection.distinct(key=key, query=_filter)
            if len(results) < 1:
                return None, 404
            return results, 200
        except errors.ConnectionFailure as e:
            return {"status": "fail", "message": "errors in connection"}, 500


