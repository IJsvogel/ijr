from datetime import datetime

from pymongo import MongoClient, ReplaceOne, InsertOne, UpdateOne, UpdateMany


class MongoWriter(object):
    """A MongoClient for bulk writing operations"""

    def __init__(self, mdb_server, mdb_user, mdb_pass, db_name, col_name, threshold=250):
        self.db_name = db_name
        self.col_name = col_name
        self._statements = list()
        # Connect with 3.4 connection-string to Atlas -> user:pwd@server....
        self._client = MongoClient('mongodb://%s:%s@%s' % (mdb_user, mdb_pass, mdb_server))
        self._threshold = threshold
        self._write_counter = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _write_to_server(self):
        """bulk write statements to the server"""
        if not self._statements:
            return
        db = self._client.get_database(self.db_name)
        coll = db.get_collection(self.col_name)
        coll.bulk_write(self._statements)
        self._statements.clear()
        self._write_counter += 1

    def close(self):
        """write statements and disconnect from MongoDB."""
        self._write_to_server()
        self._client.close()

    def write_data(self, doc: dict, doc_key: str = None):
        """write document with _ts (timestamp) included

        :Parameters:
         - `doc`: A document to be written
         - `doc_key` (optional): Document key (_id) to be used for 
         document replacement/upsert
        """
        doc['_ts'] = datetime.now()
        if doc_key is not None:
            doc['_id'] = '%s' % doc_key
            self._statements.append(ReplaceOne(filter={'_id': doc['_id']}, 
                                               replacement=doc, 
                                               upsert=True))
        else:
            self._statements.append(InsertOne(document=doc))
        if len(self._statements) > self._threshold:
            self._write_to_server()
        return self._write_counter

    def edit_data(self, query: dict, field: dict, mode: str):
        """edit document

        :Parameters:
         - `query`: A query that matches the document to update.
         - `field`: The modifications to apply.
         - `mode`: Editing mode 'one' or 'many'
        """
        if not isinstance(query, dict):
            raise TypeError('query must be an instance of dict')

        if not isinstance(field, dict):
            raise TypeError('field must be an instance of dict')

        if not isinstance(mode, str):
            raise TypeError('mode must be an instance of str')

        modes = {"one", "many"}
        mode = mode.lower()
        if mode not in modes:
            raise ValueError('Invalid mode. Expected one of: {"one", "many"}')

        if mode == "one":
            self._statements.append(UpdateOne(filter=query,
                                              update={'$set': field}))

        elif mode == "many":
            self._statements.append(UpdateMany(filter=query,
                                               update={'$set': field}))

        if len(self._statements) > self._threshold:
            self._write_to_server()


class MongoReader(object):
    def __init__(self, mdb_server, mdb_user, mdb_pass):
        self._client = MongoClient('mongodb://%s:%s@%s' % (mdb_user, mdb_pass, mdb_server))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
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

    def collections(self, db_name):
        db = self._client.get_database(db_name)
        return db.list_collection_names()
