import json
import logging

from google.cloud import pubsub

from ijr.generic_lib import running_in_gcf, default_object


class PubSubPublisher(object):
    _threshold = 25

    def __init__(self, topic, msg_type='Generic', **kwargs):
        self._messages = list()
        self._topic = topic
        self.msg_type = msg_type
        self._msg_kwargs = {k: str(v) for k, v in kwargs.items()}  # cast values to string for passing to pub/sub
        if running_in_gcf():
            self._client = pubsub.PublisherClient()
        else:
            logging.warning('PubSubPublisher -> Running local; using ./account.json')
            self._client = pubsub.PublisherClient.from_service_account_json('account.json')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._publish_messages()

    @property
    def msg_type(self):
        return self.__msg_type

    @msg_type.setter
    def msg_type(self, prop_value):
        if prop_value is None:
            raise Exception("msg_type can't be None")
        self.__msg_type = prop_value

    def _publish_messages(self):
        if not self._messages:
            return
        msg_dict = dict(_type=self.msg_type,
                        data=self._messages)

        msg = json.dumps(msg_dict, sort_keys=True, default=default_object)
        ret = self._client.publish(self._topic, msg.encode(), **self._msg_kwargs).result()

        self._messages.clear()
        return ret

    def publish(self, msg):
        self._messages.append(msg)
        if len(self._messages) > self._threshold:
            self._publish_messages()
