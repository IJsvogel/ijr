import datetime
import json
import logging

from google.cloud import pubsub

from ijr.generic_lib import running_in_gcf


def default_object(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()
    raise TypeError("Type %s not serializable" % type(o))


class PubSubPublisher(object):
    _threshold = 25

    def __init__(self, topic, msg_type):
        self._messages = list()
        self._topic = topic
        self._type = msg_type
        if running_in_gcf():
            self._client = pubsub.PublisherClient()
        else:
            logging.warning('PubSubPublisher -> Running local; using account.json')
            self._client = pubsub.PublisherClient.from_service_account_json('account.json')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._publish_messages()

    def _publish_messages(self):
        if not self._messages:
            return
        msg_dict = dict(_type=self._type,
                        data=self._messages)
        msg = json.dumps(msg_dict, sort_keys=True, default=default_object)
        ret = self._client.publish(self._topic, msg.encode()).result()
        self._messages.clear()
        return ret

    def publish(self, msg):
        self._messages.append(msg)
        if len(self._messages) > self._threshold:
            self._publish_messages()
