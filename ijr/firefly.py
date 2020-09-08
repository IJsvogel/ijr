"""for secret manager """
from google.cloud import secretmanager
from types import SimpleNamespace
from json import loads as jloads
from ijr.generic_lib import running_in_gcf
import logging


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
                list_dict = {f"i{str(index)}": _value for index, _value in enumerate(value)}
                self.__setattr__(key, NestedNamespace(list_dict))
            else:
                self.__setattr__(key, value)


class Secrets:

    def __init__(self, project_id=None):
        from os import environ
        scraped_id = environ.get('GCP_PROJECT', project_id)
        self.project_id = scraped_id
        if running_in_gcf():
            self.client = secretmanager.SecretManagerServiceClient()
        else:
            logging.warning('SecretManager -> Running local; using ./account.json')
            self._client = secretmanager.SecretManagerServiceClient.from_service_account_json('account.json')

    def dict_secret(self, secret_id, version_id=None):
        """
        Access the payload for the given secret version if one exists. The version
        can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
        """
        if not version_id:
            version_id = "latest"
        # Build the resource name of the secret version.
        name = self.client.secret_version_path(self.project_id, secret_id, version_id)
        # Access the secret version.
        secret = self.client.access_secret_version(name)
        # return payload as a dict.
        _payload = secret.payload.data.decode('UTF-8')
        payload = jloads(_payload)
        return payload

    def dot_secret(self, secret_id, version_id=None):
        """
          Access the payload for the given secret version if one exists. The version
          can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
          Returns a namespace for "dot" access
          """
        if not version_id:
            version_id = "latest"
        # Build the resource name of the secret version.
        name = self.client.secret_version_path(self.project_id, secret_id, version_id)
        # Access the secret version.
        secret = self.client.access_secret_version(name)
        # return payload as a SimpleNamespace.
        _payload = secret.payload.data.decode('UTF-8')
        payload = jloads(_payload)
        secrets = NestedNamespace(payload)
        return secrets


class Config:
    """defaults to a function running with an od variable 'MONGO' """

    def __init__(self, project_id=None, secret_target=None):
        from os import environ as os
        self._function_name = os.get('FUNCTION_NAME')
        secrets = Secrets(project_id)
        _secret_target = "none" if secret_target is None else secret_target
        self.mongo = secrets.dot_secret(os.get(_secret_target, os['MONGO']))

    def get_configs_dict(self, function_name=None, db_name="common", collection_name="configs"):
        from ijr.mongo_lib import MongoReader
        if function_name is None:
            function_name = self._function_name
        with MongoReader(mdb_server=self.mongo.MDB_SERVER,
                         mdb_user=self.mongo.MDB_USER,
                         mdb_pass=self.mongo.MDB_PASS) as mr_configs:
            doc_list = mr_configs.find(db_name=db_name,
                                       collection_name=collection_name,
                                       query={"_id": function_name})
        return next(doc_list, None)

    def get_configs_dot(self, function_name=None):
        temp = self.get_configs_dict(function_name)
        if temp:
            return NestedNamespace(temp)
        return None
