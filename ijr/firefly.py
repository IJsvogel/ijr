"""for secret manager """
from google.cloud import secretmanager_v1 as sm
from types import SimpleNamespace
from json import loads as jloads
from ijr.generic_lib import running_in_gcf
from os import environ
from ijr.mongo_lib import MongoReader


class NestedNamespace(SimpleNamespace):
    """need to access list indexs by index id
         {"test_array": [{"test": "index value 0"}, {"test": "index value 1"}]}
         test_array.i0.test  will return the value of the test node in index 0 of test_array
         so "index value 0" """

    def __init__(self, dictionary, **kwargs):
        super().__init__(**kwargs)
        for key, value in dictionary.items():
            if isinstance(value, dict):
                self.__setattr__(key, NestedNamespace(value))
            elif isinstance(value, list):
                list_dict = {f"i{str(index)}": _value
                             for index, _value in enumerate(value)}
                self.__setattr__(key, NestedNamespace(list_dict))
            else:
                self.__setattr__(key, value)


class Secrets:

    def __init__(self, project_id=None):
        scraped_id = environ.get('GCP_PROJECT', project_id)
        self.project_id = scraped_id
        if running_in_gcf():
            self.client = sm.SecretManagerServiceClient()
        else:
            import logging
            logging.warning('SecretManager -> Running local; using ./account.json')
            self.client =sm.SecretManagerServiceClient.from_service_account_json('account.json')

    def dict_secret(self, secret_id, version_id=None):
        """
        Access the payload for the given secret version if one exists. The version
        can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
        """
        if version_id is None:
            version_id = "latest"
            # Build the resource name of the secret version.
        name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version_id}"
        secret = self.client.access_secret_version(name=name)
        _payload = secret.payload.data.decode('UTF-8')
        payload = jloads(_payload)
        return payload

    def dot_secret(self, secret_id, version_id=None):
        """
          Access the payload for the given secret version if one exists. The version
          can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
          Returns a namespace for "dot" access
          """

        payload = self.dict_secret(secret_id, version_id=None)
        if payload:
            return NestedNamespace(payload)


class Config:
    """defaults to a function running with an od variable 'MONGO' """
    DEFAULT_DB_NAME = "common"
    DEFAULT_COLLECTION_NAME = "configs"
    DEFAULT_SECRET_TARGET = "MONGO"

    def __init__(self, project_id=None, secret_target=None, secret_id=None):
        if secret_id is None:
            secret_id = self._get_secret_id_from_env_var(secret_target)
        self._function_name = environ.get('FUNCTION_NAME')
        secrets = Secrets(project_id)
        self.mongo = secrets.dot_secret(secret_id)

    def _get_secret_id_from_env_var(self, secret_target=None):
        if secret_target is None:
            secret_target = self.DEFAULT_SECRET_TARGET
        secret_id = environ[str(secret_target)]
        return secret_id

    def get_configs_dict(self, _id=None, db_name=None, collection_name=None):
        if _id is None:
            _id = self._function_name
        if db_name is None:
            db_name = self.DEFAULT_DB_NAME
        if collection_name is None:
            collection_name = self.DEFAULT_COLLECTION_NAME
        with MongoReader(mdb_server=self.mongo.MDB_SERVER,
                         mdb_user=self.mongo.MDB_USER,
                         mdb_pass=self.mongo.MDB_PASS) as mr_configs:
            doc_list = mr_configs.find(db_name=db_name,
                                       collection_name=collection_name,
                                       query={"_id": _id})
        return next(doc_list, None)

    def get_configs_dot(self, _id=None, db_name=None, collection_name=None):
        temp = self.get_configs_dict(_id=_id,
                                     db_name=db_name,
                                     collection_name=collection_name)
        if temp:
            return NestedNamespace(temp)
        return None
