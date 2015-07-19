__author__ = 'Chris Day'
__publisher__ = 'Fabler'

import logging
import boto.dynamodb2
from boto.dynamodb2.table import Table

DYNAMO_REGION = 'us-west-2'


class CorgiCache:
    def __init__(self):
        self.dynamo = boto.dynamodb2.connect_to_region(DYNAMO_REGION)
        self.feeds = Table('Feeds', connection=self.dynamo)
        self.tokens = Table('Tokens', connection=self.dynamo)
        return

    def feed_id_exists(self, feed_id):
        items = list(self.feeds.query_2(ID__eq=feed_id))
        return len(items) > 0

    def put_feed(self, data):
        if 'ID' not in data or 'URL' not in data:
            logging.debug("invalid item, {0}".format(data))
            raise ValueError
        self.feeds.put_item(data=data)
        return

    def put_feed_batch(self, data):
        with self.feeds.batch_write() as batch:
            for item in data:
                if 'ID' not in item or 'URL' not in item:
                    logging.debug("invalid item, {0}".format(item))
                    raise ValueError
                batch.put_item(data=item)
        return

    def get_all_feeds(self):
        return self.feeds.scan()

    def get_token(self, use):
        items = list(self.tokens.query_2(USE__eq=use))
        if len(items) > 1:
            raise ValueError

        item = items[0]
        return item

    def update_token(self, token):
        if 'USE' not in token or 'TOKEN' not in token or 'REFRESH_TOKEN' not in token:
            logging.debug("invalid token, {0}".format(token))
            raise ValueError
        self.tokens.put_item(data=token)
        return
