__author__ = 'johann'

import json
from elasticsearch import Elasticsearch, client
from elasticsearch.exceptions import *
from conf.confreader import ConfReader

from random import randint
from datetime import datetime, timedelta
from utc import UTC

class ElasticBridge:

    def __init__(self):
        self.es = Elasticsearch()

    def check_index_creation(self):
        if self.es.indices.exists('feeds') is not True:
            self.es.index(index='feeds', doc_type='text', body={
                    "feed-id" : "",
                    "name" : "",
                    "type" : "",
                    "visit" : "",
                    "skip" : "",
                    "locale" : ""
                    })

            self.es.indices.refresh(index='feeds')
            print "[>] Index 'feeds' just created."

        if self.es.indices.exists('posts') is not True:
            self.es.index(index='posts', doc_type='text', body={
                    "post-id" : "",
                    "post-message" : "",
                    "user-id" : "",
                    "user-name" : "",
                    "key" : ""
                    })

            self.es.indices.refresh(index='posts')
            print "[>] Index 'posts' just created."

        if self.es.indices.exists('comments') is not True:
            self.es.index(index='comments', doc_type='text', body={
                    "comment-id" : "",
                    "comment-message" : "",
                    "post-id" : "",
                    "user-id" : "",
                    "user-name" : "",
                    "key" : ""
                    })

            self.es.indices.refresh(index='comments')
            print "[>] Index 'comments' just created."

        self.check_state_municipality_persistence()

    def check_state_municipality_persistence(self):
        if self.es.indices.exists('brazilian-places') is not True:
            self.es.index(index='brazilian-places', doc_type='text', body={
                    "name" : "",
                    "type" : ""
                    })
            print "[>] Index 'brazilian-places' just created."

        self.es.indices.refresh(index='brazilian-places')

        if len(self.es.search(index="brazilian-places", size=10)["hits"]["hits"]) < 2:
            file = open('conf/brazilianplaces.json')
            data = json.load(file)
            file.close()

            states = data["states"]
            municipalities = data["municipalities"]

            print "[>] Adding states and municipalities into index 'brazilian-places', please wait..."

            for state in states:
                self.es.index(index='brazilian-places', doc_type='text', body={
                        "name" : state,
                        "type" : "state"
                        })
            for municipality in municipalities:
                self.es.index(index='brazilian-places', doc_type='text', body={
                        "name" : municipality,
                        "type" : "municipality"
                        })


    def add_feed(self, feed_id, name, type, visit, skip="NO", locale="TO BE ANALYZED"):
        try:
            match = self.es.search(index='feeds', q="feed-id:%(feed_id)s" % {"feed_id" : feed_id})

            if not match['hits']['hits']:
                self.es.index(index='feeds', doc_type='text', body={
                        "feed-id" : feed_id,
                        "name" : name,
                        "type" : type,
                        "visit" : visit,
                        "skip" : skip,
                        "locale" : locale
                        })

            self.es.indices.refresh(index='feeds')
        except ConnectionTimeout:
            print "[!] ES ConnectionTimeout on add_feed, trying to add again..."
            self.add_feed(feed_id, name, type, visit, skip, locale)

    def add_post(self, post_id, post_message, user_id, user_name, key=""):
        if (key == "UNCATEGORIZED" and ConfReader.RUNNING_MODE == "DEBUG") or key != "REJECT":
            try:
                match = self.es.search(index='posts', q="post-id:%(post_id)s" % {"post_id" : post_id})

                if not match['hits']['hits']:
                    self.es.index(index='posts', doc_type='text', body={
                                    "post-id" : post_id,
                                    "post-message" : post_message,
                                    "user-id" : user_id,
                                    "user-name" : user_name,
                                    "key" : str(key)
                                    })
            except ConnectionTimeout:
                print "[!] ES ConnectionTimeout on add_post, trying to add again..."
                self.add_post(post_id, post_message, user_id, user_name, key)
        else:
            print "[!] Post not persisted in Elasticsearch due to having a key: " + key + \
                  ". Running mode: " + ConfReader.RUNNING_MODE + "."


    def add_comment(self, comment_id, comment_message, post_id, user_id, user_name, key=""):
        if (key == "UNCATEGORIZED" and ConfReader.RUNNING_MODE == "DEBUG") or key != "REJECT":
            if ":" not in comment_id:
                try:
                    match = self.es.search(index='comments', q="comment-id:%(comment_id)s" % {"comment_id" : comment_id})

                    if not match['hits']['hits']:
                        self.es.index(index='comments', doc_type='text', body={
                                        "comment-id" : comment_id,
                                        "comment-message" : comment_message,
                                        "post-id" : post_id,
                                        "user-id" : user_id,
                                        "user-name" : user_name,
                                        "key" : str(key)
                                        })
                except ConnectionTimeout:
                    print "[!] ES ConnectionTimeout on add_comment, trying to add again..."
                    self.add_comment(comment_id, comment_message, post_id, user_id, user_name, key)
            else:
                print "[!] Comment with ':' inside comment_id. The comment will not be saved due to technical issues."
        else:
            print "[!] Comment not persisted in Elasticsearch due to having a key: " + key + \
                  ". Running mode: " + ConfReader.RUNNING_MODE + "."

    def update_feed(self, feed_id, visit, skip="NOT DEFINED", locale="NOT DEFINED"):

        try:
            match = self.es.search(index='feeds', q="feed-id:%(feed_id)s" % {"feed_id" : feed_id})

            if len(match["hits"]["hits"]) > 1:
                print "More than one element with feed_id: " + feed_id + " found."
                for hit_index in range(1, len(match["hits"]["hits"])):
                    self.es.delete(index='feeds', doc_type='text', id=match["hits"]["hits"][hit_index]["_id"])
                print "The duplication mess was cleaned up for feed_id: " + feed_id
            elif len(match["hits"]["hits"]) < 1:
                print "No element with feed id: " + feed_id + " have been found."
                raise NotFoundError()

            _id = match["hits"]["hits"][0]["_id"]
            name = match["hits"]["hits"][0]["_source"]["name"]
            type = match["hits"]["hits"][0]["_source"]["type"]

            if skip == "NOT DEFINED":
                skip = match["hits"]["hits"][0]["_source"]["skip"]

            if locale == "NOT DEFINED":
                locale = match["hits"]["hits"][0]["_source"]["skip"]

            visit = str(visit)
            if "T" not in visit: visit = visit.replace(" ", "T")

            self.es.index(index='feeds', doc_type='text', id=_id, body={
                        "feed-id" : feed_id,
                        "name" : name,
                        "type" : type,
                        "visit" : visit,
                        "skip" : skip,
                        "locale" : locale
                        })
        except ConnectionTimeout:
            print "[!] ES ConnectionTimeout on update_feed, trying to add again..."
            self.update_feed(feed_id, visit)

    def get_feed(self, feed_id):
        match = self.es.search(index="feeds", q="feed-id:%(feed_id)s" % {"feed_id" : feed_id})

        if len(match["hits"]["hits"]) > 1:
            print "More than one element with feed_id: " + feed_id + " found."
            for hit_index in range(1, len(match["hits"]["hits"])):
                self.es.delete(index='feeds', doc_type='text', id=match["hits"]["hits"][hit_index]["_id"])
            print "The duplication mess was cleaned up for feed_id: " + feed_id
        elif len(match["hits"]["hits"]) < 1:
            print "No element with feed id: " + feed_id + " have been found on ES. This is expected for a just discovered feed."
            raise NotFoundError()

        return match["hits"]["hits"][0]["_source"]

    def get_all_places(self, limit=6000):
        return self.es.search(index="brazilian-places", size=limit)["hits"]["hits"]

    def get_all_feeds(self, limit=1000000):
        return self.es.search(index="feeds", size=limit)["hits"]["hits"]

    def get_all_posts(self, limit=1000000):
        return self.es.search(index="posts", size=limit)["hits"]["hits"]

    def delete_all_indexes(self):
        for index_to_be_deleted in ["feeds", "posts", "comments", "brazilian-places"]:
            try:
                self.es.indices.delete(index=index_to_be_deleted)
                print "[>] " + index_to_be_deleted + " deleted (and so, all its records!)."
            except NotFoundError: print "[!] " + index_to_be_deleted + " doesn't exist. It might already been deleted."

if __name__ == '__main__':
    eb = ElasticBridge()
    #print eb.get_feed("15547917722935134")
    #eb.check_index_creation()
    eb.delete_all_indexes()
    #
    #print es.indices.exists('comments')
    #utc = UTC()
    #eb.check_index_creation()
    #eb.add_feed("1234", "Kjattew", "user", utc.utcnow() + timedelta(minutes=randint(1, 5)))
    #print eb.get_feed("1234")
    #eb.update_feed("1234", utc.utcnow() + timedelta(minutes=randint(1, 5)))