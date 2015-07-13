__author__ = 'Chris Day'
__publisher__ = 'Fabler'

import boto.dynamodb2
from boto.dynamodb2.table import Table

DYNAMO_REGION = 'us-west-2'

class CorgiCache:
    def __init__(self):
        self.dynamo = boto.dynamodb2.connect_to_region(DYNAMO_REGION)
        self.feeds = Table('feeds', connection=self.dynamo)
        return

    def feed_id_exists(self, feed_id):
        items = list(self.feeds.query_2(ID__eq=feed_id))
        return len(items) > 0

    def put_feed(self, data):
        assert 'ID' in data
        assert 'URL' in data
        self.feeds.put_item(data=data)
        return
