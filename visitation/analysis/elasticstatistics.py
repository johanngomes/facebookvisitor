__author__ = 'johann'

from elasticsearch import Elasticsearch
from pprint import pprint

class ElasticStatistics:

    def __init__(self):
        self.es = Elasticsearch()
        self.LIMIT = 10000000

    def page_daily_posts_and_comments_categorized(self, feed_id):
        posts = self.es.search(index='posts', q="user-id:%(feed_id)s -key:UNCATEGORIZED" % {"feed_id" : feed_id}, size=self.LIMIT)
        comments = self.es.search(index='comments', q="post-id:%(feed_id)s_* -key:UNCATEGORIZED" % {"feed_id" : feed_id}, size=self.LIMIT)
        print "Posts of page " + feed_id + " categorized: " + str(len(posts["hits"]["hits"]))
        print "Comments in page " + feed_id + " categorized: " + str(len(comments["hits"]["hits"]))

    def posts_and_comments_per_type_feed(self):
        users = self.es.search(index='feeds', q="type:'user'", size=self.LIMIT)
        pages = self.es.search(index='feeds', q="type:'page'", size=self.LIMIT)
        events = self.es.search(index='feeds', q="type:'event'", size=self.LIMIT)
        groups = self.es.search(index='feeds', q="type:'group'", size=self.LIMIT)

        num_users = len(users["hits"]["hits"])
        num_pages = len(pages["hits"]["hits"])
        num_events = len(events["hits"]["hits"])
        num_groups = len(groups["hits"]["hits"])

        print num_users, num_pages, num_events, num_groups

        user_posts_found = 0
        for user in users["hits"]["hits"]:
            match = self.es.search(index='posts', q="user-id:%(feed_id)s*" % {"feed_id" : user["_source"]["feed-id"]}, size=self.LIMIT)
            user_posts_found += len(match["hits"]["hits"])
            #print "Processing number of user posts... currently in " + str(user_posts_found)
        print "Posts from users: " + str(user_posts_found)

        user_comments_found = 0
        for user in users["hits"]["hits"]:
            match = self.es.search(index='comments', q="user-id:%(feed_id)s*" % {"feed_id" : user["_source"]["feed-id"]}, size=self.LIMIT)
            user_comments_found += len(match["hits"]["hits"])
            #print "Processing number of user comments... currently in " + str(user_comments_found)
        print "Comments from users: " + str(user_comments_found)

        pages_posts_found = 0
        for page in pages["hits"]["hits"]:
            match = self.es.search(index='posts', q="user-id:%(feed_id)s*" % {"feed_id" : page["_source"]["feed-id"]}, size=self.LIMIT)
            pages_posts_found += len(match["hits"]["hits"])
            #print "Processing number of page posts... currently in " + str(pages_posts_found)
        print "Posts from pages: " + str(pages_posts_found)

        pages_comments_found = 0
        for page in pages["hits"]["hits"]:
            match = self.es.search(index='comments', q="user-id:%(feed_id)s*" % {"feed_id" : page["_source"]["feed-id"]}, size=self.LIMIT)
            pages_comments_found += len(match["hits"]["hits"])
            #print "Processing number of page comments... currently in " + str(pages_comments_found)
        print "Comments from pages: " + str(pages_comments_found)

        groups_posts_found = 0
        for group in groups["hits"]["hits"]:
            match = self.es.search(index='posts', q="user-id:%(feed_id)s*" % {"feed_id" : group["_source"]["feed-id"]}, size=self.LIMIT)
            groups_posts_found += len(match["hits"]["hits"])
            #print "Processing number of group posts... currently in " + str(groups_posts_found)
        print "Posts from groups: " + str(groups_posts_found)

        group_comments_found = 0
        for group in groups["hits"]["hits"]:
            match = self.es.search(index='comments', q="user-id:%(feed_id)s*" % {"feed_id" : group["_source"]["feed-id"]}, size=self.LIMIT)
            group_comments_found += len(match["hits"]["hits"])
            #print "Processing number of group comments... currently in " + str(group_comments_found)
        print "Comments from groups: " + str(group_comments_found)

        event_posts_found = 0
        for event in events["hits"]["hits"]:
            match = self.es.search(index='posts', q="user-id:%(feed_id)s*" % {"feed_id" : event["_source"]["feed-id"]}, size=self.LIMIT)
            event_posts_found += len(match["hits"]["hits"])
            #print "Processing number of event posts... currently in " + str(event_posts_found)
        print "Posts from events: " + str(event_posts_found)

        event_comments_found = 0
        for event in events["hits"]["hits"]:
            match = self.es.search(index='comments', q="user-id:%(feed_id)s*" % {"feed_id" : event["_source"]["feed-id"]}, size=self.LIMIT)
            event_comments_found += len(match["hits"]["hits"])
            #print "Processing number of event comments... currently in " + str(event_comments_found)
        print "Comments from events: " + str(event_comments_found)

if __name__ == '__main__':
    es = ElasticStatistics()

    feed_id = "468241373195655"

    es.page_daily_posts_and_comments_categorized(feed_id)
    es.posts_and_comments_per_type_feed()
