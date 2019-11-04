from datetime import datetime

from pymongo import MongoClient, ReplaceOne, InsertOne


class MongoWriter(object):
    _threshold = 250

    def __init__(self, mdb_server, mdb_user, mdb_pass, db_name, col_name):
        self.db_name = db_name
        self.col_name = col_name
        self._statements = list()
        # Connect with 3.4 connection-string to Atlas -> user:pwd@server....
        self._client = MongoClient('mongodb://%s:%s@%s' % (mdb_user, mdb_pass, mdb_server))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._write_to_server()
        self._client.close()

    def _write_to_server(self):
        if not self._statements:
            return
        db = self._client.get_database(self.db_name)
        coll = db.get_collection(self.col_name)
        coll.bulk_write(self._statements)
        self._statements.clear()

    def write_data(self, doc, doc_key=None):
        doc['_ts'] = datetime.now()
        if doc_key:
            doc['_id'] = '%s' % doc_key
            self._statements.append(ReplaceOne(filter={'_id': doc['_id']}, replacement=doc, upsert=True))
        else:
            self._statements.append(InsertOne(document=doc))
        if len(self._statements) > self._threshold:
            self._write_to_server()


class MongoReader(object):
    def __init__(self, mdb_server, mdb_user, mdb_pass):
        self._client = MongoClient('mongodb://%s:%s@%s' % (mdb_user, mdb_pass, mdb_server))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._client.close()

    def find(self, db_name, collection_name, query, sorting=None, limit=-1, projection=None):
        db = self._client.get_database(db_name)
        col = db.get_collection(collection_name)
        ret = col.find(query, projection)
        if sorting:
            ret.sort(sorting)
        if limit > 0:
            ret.limit(limit=limit)
        for r in ret:
            yield r

    def aggregate(self, db_name, collection_name, pipeline):
        db = self._client.get_database(db_name)
        col = db.get_collection(collection_name)
        ret = col.aggregate(pipeline, allowDiskUse=True)
        for r in ret:
            yield r
