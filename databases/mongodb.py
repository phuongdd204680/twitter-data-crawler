import sys
from pymongo import MongoClient, UpdateOne

from constants.config import MongoDBConfig
from utils.dict_utils import flatten_dict, delete_none
from utils.logger_utils import get_logger

logger = get_logger('MongoDB')


class MongoDB:
    def __init__(self, connection_url=None, database=MongoDBConfig.DATABASE):
        if not connection_url:
            connection_url = MongoDBConfig.CONNECTION_URL

        self.connection_url = connection_url.split('@')[-1]
        try:
            self.connection = MongoClient(connection_url)
            self.mongo_db = self.connection[database]
        except Exception as e:
            logger.exception(f"Failed to connect to ArangoDB: {connection_url}: {e}")
            sys.exit(1)

    def get_docs(self, collection, keys: list = None, filter_: dict = None, batch_size=1000,
                 projection=None):  # change filter_ to obj

        filter_statement = {}
        if keys:
            filter_statement["_id"] = {"$in": keys}
        if filter_ is not None:
            filter_statement.update(filter_)
        if projection:
            projection_statement = self.get_projection_statement(projection)
            cursor = self.mongo_db[collection].find(
                filter=filter_statement, projection=projection_statement, batch_size=batch_size)
        else:
            cursor = self.mongo_db[collection].find(
                filter=filter_statement, batch_size=batch_size)
        return cursor

    def get_doc(self, collection, key: str = None, filter_: dict = None, batch_size=1000, projection=None):  # change filter_ to obj

        filter_statement = {}
        if key:
            filter_statement["_id"] = key
        if filter_ is not None:
            filter_statement.update(filter_)
        if projection:
            projection_statement = self.get_projection_statement(projection)
            cursor = self.mongo_db[collection].find_one(
                filter=filter_statement, projection=projection_statement, batch_size=batch_size)
        else:
            cursor = self.mongo_db[collection].find_one(
                filter=filter_statement, batch_size=batch_size)
        return cursor

    def get_docs_with_db(self, db, collection, keys: list = None, filter_: dict = None, batch_size=1000,
                         projection=None):  # change filter_ to obj
        projection_statement = self.get_projection_statement(projection)

        filter_statement = {}
        if keys:
            filter_statement["_id"] = {"$in": keys}
        if filter_ is not None:
            filter_statement.update(filter_)

        cursor = self.connection[db][collection].find(
            filter=filter_statement, projection=projection_statement, batch_size=batch_size)
        return cursor

    def update_docs(self, collection_name, data, keep_none=False, merge=True, shard_key=None, flatten=True):
        """If merge is set to True => sub-dictionaries are merged instead of overwritten"""
        try:
            col = self.mongo_db[collection_name]
            # col.insert_many(data, overwrite=True, overwrite_mode='update', keep_none=keep_none, merge=merge)
            bulk_operations = []
            if not flatten:
                if not shard_key:
                    bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
                else:
                    bulk_operations = [UpdateOne({"_id": item["_id"], shard_key: item[shard_key]}, {"$set": item}, upsert=True) for item in data]
                col.bulk_write(bulk_operations)
                return
            
            for document in data:
                unset, set_, add_to_set = self.create_update_doc(document, keep_none, merge, shard_key)
                if not shard_key:
                    bulk_operations += [
                        UpdateOne({"_id": item["_id"]},
                                  {"$unset": {key: value for key, value in item.items() if key != "_id"}}, upsert=True)
                        for item in unset]
                    bulk_operations += [
                        UpdateOne({"_id": item["_id"]},
                                  {"$set": {key: value for key, value in item.items() if key != "_id"}}, upsert=True)
                        for item in set_]
                    bulk_operations += [
                        UpdateOne({"_id": item["_id"]},
                                  {"$addToSet": {key: value for key, value in item.items() if key != "_id"}},
                                  upsert=True)
                        for item in add_to_set]
                if shard_key:
                    keys = ["_id", shard_key]
                    bulk_operations += [
                        UpdateOne({"_id": item["_id"], shard_key: item[shard_key]},
                                  {"$unset": {key: value for key, value in item.items() if key not in keys}},
                                  upsert=True)
                        for item in unset]
                    bulk_operations += [
                        UpdateOne({"_id": item["_id"], shard_key: item[shard_key]},
                                  {"$set": {key: value for key, value in item.items() if key not in keys}}, upsert=True)
                        for item in set_]
                    bulk_operations += [
                        UpdateOne({"_id": item["_id"], shard_key: item[shard_key]},
                                  {"$addToSet": {key: value for key, value in item.items() if key not in keys}},
                                  upsert=True)
                        for item in add_to_set]
            col.bulk_write(bulk_operations)
        except Exception as ex:
            logger.exception(ex)

    def remove_out_date_docs(self, collection_name, timestamp, filter_: dict = None):  # change filter to dict
        filter_statement = {
            "lastUpdatedAt": {"$lt": timestamp}
        }
        if filter_ is not None:
            filter_statement.update(filter_)

        self.mongo_db[collection_name].delete_many(filter_statement)

    def remove_docs_by_keys(self, collection_name, keys):
        filter_statement = {
            "_id": {"$in": keys}
        }
        self.mongo_db[collection_name].delete_many(filter_statement)

    @staticmethod
    def create_update_doc(document, keep_none=False, merge=True, shard_key=None):
        unset, set_, add_to_set = [], [], []
        if not keep_none:
            doc = flatten_dict(document)
            for key, value in doc.items():
                if value is None:
                    tmp = {
                        "_id": document["_id"],
                        key: ""
                    }
                    if shard_key:
                        tmp[shard_key] = document[shard_key]
                    unset.append(tmp)
                    continue
                if not merge:
                    continue
                if isinstance(value, list):
                    tmp = {
                        "_id": document["_id"],
                        key: {"$each": [i for i in value if i]}
                    }
                    if shard_key:
                        tmp[shard_key] = document[shard_key]
                    add_to_set.append(tmp)
                else:
                    tmp = {
                        "_id": document["_id"],
                        key: value
                    }
                    if shard_key:
                        tmp[shard_key] = document[shard_key]
                    set_.append(tmp)

        if not merge:
            if keep_none:
                set_.append(document)
            else:
                set_.append(delete_none(document))

        return unset, set_, add_to_set

    def delete_documents(self, collection, filter_):
        self.mongo_db[collection].delete_many(filter_)

    @staticmethod
    def get_projection_statement(projection: list = None):
        if projection is None:
            return {}

        projection_statements = {}
        for field in projection:
            projection_statements[field] = True

        return projection_statements