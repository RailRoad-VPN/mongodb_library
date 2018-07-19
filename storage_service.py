from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError


class StorageService(object):
    __version__ = 1

    def __init__(self):
        pass

    def create(self, data, collection_name: str, is_return: bool = False):
        pass

    def get(self, cond: dict, sort_field: str, collection_name: str):
        pass

    def update(self, cond: dict, data, collection_name: str, is_return: bool = False):
        pass


class StoredObject(object):
    __version__ = 1

    _storage_service = None
    _limit = None
    _offset = None

    def __init__(self, storage_service: StorageService, limit: int = None, offset: int = None):
        self._limit = limit
        self._offset = offset
        self._storage_service = storage_service


class DBStorageService(StorageService):
    __version__ = 1

    _db = None
    _mongo_client = None

    def __init__(self, config: dict) -> None:
        super().__init__()
        host = config['MONGODB']['host']
        port = config['MONGODB']['port']
        db_name = config['MONGODB']['db_name']
        try:
            self._mongo_client = MongoClient(f'mongodb://{host}:{port}', serverSelectionTimeoutMS=3000)
            self._mongo_client.server_info()
        except ServerSelectionTimeoutError as err:
            raise DFNDatabaseError(error=err.args[0], code=0)
        self._db = self._mongo_client[db_name]

    def create(self, data, collection_name: str, is_return: bool = False):
        collection = self._db[collection_name]
        if isinstance(data, list):
            result = collection.insert_many(data)
            if is_return:
                return result.inserted_ids
        elif isinstance(data, dict):
            result = collection.insert_one(data)
            if is_return:
                return result.inserted_id
        else:
            raise ValueError

    def update(self, cond: dict, data, collection_name: str, is_return: bool = False):
        collection = self._db[collection_name]
        if isinstance(data, list):
            result = collection.update_many(cond, data)
            if is_return:
                return result.inserted_ids
        elif isinstance(data, dict):
            result = collection.update_one(cond, data)
            if is_return:
                return result.inserted_id
        else:
            raise ValueError

    def get(self, cond: dict, collection_name: str, sort_field: str = None, limit: int = None, offset: int = None):
        collection = self._db[collection_name]
        result = collection.find(cond)
        if not result.alive:
            raise DFNDatabaseError(error='time out', code=0)
        if limit:
            result = result.limit(limit)
        if offset:
            result = result.skip(offset)
        if sort_field:
            result = result.sort(sort_field)
        return result


class DFNDatabaseError(Exception):
    __version__ = 1

    code = None
    error = None

    def __init__(self, code: int, error: str, *args):
        super().__init__(*args)
