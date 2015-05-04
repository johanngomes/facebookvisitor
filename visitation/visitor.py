__author__ = 'johann'

import time
import facepy
import elasticsearch
import timeout_decorator

from datetime import timedelta
from initializer import Initializer
from pprint import pprint
from conf.confreader import ConfReader
from facebookexceptions import FacebookExceptions


class Visitor(Initializer):

    def __init__(self):
        Initializer.__init__(self)

        self.check_init_first_time()

        # posts posted more than 15 days ago will be ignored
        self.ignore_before = self.utc.utcnow() - timedelta(days=ConfReader.MAX_DAYS_SPENT_TO_CRAWL)
        print "** [!] RESTRICTION: Ignore posts before: " + str(self.ignore_before) + " **"

        self.recalculate_visits()

    def check_init_first_time(self):
        try:
            self.eb.get_feed(ConfReader.ROOT_FEED_ID)
        except elasticsearch.NotFoundError:
            print "[!] ROOT FEED NOT FOUND ON ELASTICSEARCH. Adding " + ConfReader.ROOT_FEED_ID + "..."
            root_page = self.graphFBAPI.get("/" + ConfReader.ROOT_FEED_ID)
            feed_type = self.check_feed_type(ConfReader.ROOT_FEED_ID)
            self.eb.add_feed(ConfReader.ROOT_FEED_ID, root_page["name"], feed_type, self.sc.set_next_visit()["raw"])

            pre_loaded_sources = self.mf.extract_facebook_sources()
            if pre_loaded_sources:
                for source in pre_loaded_sources:
                    try:
                        self.eb.get_feed(source["id"])
                    except elasticsearch.NotFoundError:
                        print "[!] PRE-LOADED FEED " + source["id"] + " NOT FOUND ON ELASTICSEARCH. Adding " + \
                              source["id"] + "..."
                        self.eb.add_feed(source["id"], source["name"], source["type"], self.sc.set_next_visit()["raw"])

    def check_feed_type(self, feed_id):
        request_succeeded = False
        while not request_succeeded:
            try:
                feed = self.graphFBAPI.get(feed_id + "?metadata=1")
                if feed.has_key("metadata"):
                    if feed["metadata"].has_key("type"):
                        return feed["metadata"]["type"]
                else:
                    print "Feed without type: " + feed_id
                    raise elasticsearch.NotFoundError("Strangely, type was not found on this feed.")
                request_succeeded = True
            except facepy.HTTPError:
                timeout = 15
                print "[!] Request not succeeded, waiting " + str(timeout) + " seconds to retry..."
                time.sleep(timeout)
                self.initialize_facebook_graph_api()

    def recalculate_visits(self):
        print "** [!] Visiting Elasticsearch to recalculate visits if necessary... **" + "\n** UTC NOW: " + \
              str(self.utc.utcnow()) + " **\n"
        results = self.eb.get_all_feeds()
        for result in results:
            source = result["_source"]
            # there is a next visit time for this source, and it needs to be analyzed
            if source["visit"] != "" and source.has_key("feed-id"):
                formatted_visit_from_es = self.sc.format_next_visit(source["visit"])
                print "formatted next_visit from elasticsearch: " + str(formatted_visit_from_es)
                next_visit = self.sc.format_next_visit(source["visit"])

                if next_visit["raw"] < self.utc.utcnow():
                    next_visit = self.sc.set_next_visit()

                    print "( # ) " + source["feed-id"] + " " + source["name"] + " [DELAYED] "\
                          + "(past date: " + str(source["visit"]).replace("T", " ") + "). Saving to Elasticsearch " \
                          + "and setting to: " + str(next_visit["raw"]) + ". Minutes to visit: " \
                          + str(next_visit["minutes"]) + "."
                    if next_visit["minutes"] == 0: print "Seconds to visit: " + str(next_visit["seconds"]) + "."

                    self.eb.update_feed(source["feed-id"], next_visit["raw"])
                else:
                    print "( @ ) " + source["feed-id"] + " " + source["name"] + " [ON TIME]. "  \
                          + "Next visit from Elasticsearch: " + str(next_visit["raw"]) + ". Minutes to visit: " \
                          + str(next_visit["minutes"])
                    if next_visit["minutes"] == 0:
                        print "Seconds to visit: " + str(next_visit["seconds"]) + "."

                self.bt.add_to_batch(source["feed-id"], source["name"], source["type"], next_visit["minutes"])

        self.batching_controller()

    @timeout_decorator.timeout(ConfReader.TIMEOUT_SECONDS)
    def batching_controller(self):
        while True:
            next_batch_minutes = min(self.bt.batch)
            requests = self.bt.get_next_requests(next_batch_minutes)
            if requests:
                print "[>] Next batch will be requested in: " + str(next_batch_minutes) + ". Number of requests: " + \
                      str(len(requests))
                seconds_for_request = next_batch_minutes * 60
                # tests, we should replace by seconds_for_request
                time.sleep(2)

                batch_request_succeeded = False
                while not batch_request_succeeded:
                    try:
                        responses = self.graphFBAPI.batch(requests=requests)

                        for request, response in zip(requests, responses):
                            if "_" not in request["source_id"] and "?" not in request["relative_url"]:
                                # it's a feed, not a page to paginate
                                feed_info = self.eb.get_feed(request["source_id"])
                                if feed_info["skip"] == "NO":
                                    if isinstance(response, facepy.FacebookError):
                                        if response.code == FacebookExceptions.UNSUPPORTED_GET_REQUEST:
                                            next_visit = self.sc.set_next_visit(ConfReader.REVISIT_INCREMENT)
                                            self.eb.update_feed(request["source_id"], next_visit["raw"], skip="YES")
                                            print "[!] Unsupported get request for " + str(request["relative_url"]) + \
                                                  ". The feed may deleted his own account. Setting SKIP to 'YES'."
                                    elif self.ff.detect_locale(request, response) != "BR":
                                        next_visit = self.sc.set_next_visit(ConfReader.REVISIT_INCREMENT)
                                        self.eb.update_feed(request["source_id"], next_visit["raw"], skip="YES",
                                                            locale="FOREIGN")
                                        print "[!] Feed locale is not BR. Setting LOCALE to 'FOREIGN'. " \
                                              "Setting SKIP to 'YES'."
                                    else:
                                        try:
                                            self.paginate_feed(response, request["source_id"], request["source_name"],
                                                               request["type"])
                                        except facepy.OAuthError as error:
                                            print "[!] " + str(error.message) + " for source_id: " + \
                                                  str(request["source_id"]) + ". The feed might be inaccessible. " \
                                                                              "Setting SKIP to 'YES'"
                                            next_visit = self.sc.set_next_visit(ConfReader.REVISIT_INCREMENT)
                                            self.eb.update_feed(request["source_id"], next_visit["raw"], skip="YES")
                                        except timeout_decorator.TimeoutError:
                                            print "[!] Timeout reached, paginate_feed terminated."
                                elif feed_info["skip"] == "YES":
                                    print "[>] Skipping " + request["source_id"] + ". The account has been deleted."
                            else:
                                if isinstance(response, facepy.OAuthError):
                                    print "Error accessing: " + str(response) + "\nMessage: " + str(response.message)
                                else:
                                    try:
                                        self.paginate_feed(response, request["source_id"], request["source_name"],
                                                           request["type"])
                                    except timeout_decorator.TimeoutError:
                                        print "[!] Timeout reached, paginate_feed terminated."

                        self.bt.delete_first_subset_of_requests(next_batch_minutes)
                        batch_request_succeeded = True
                    except facepy.HTTPError:
                        timeout = 15
                        print "[!] Batch request not succeeded, waiting " + str(timeout) + \
                              " to begin reauthenticating..."
                        time.sleep(timeout)
                        self.initialize_facebook_graph_api()

    def paginate_feed(self, response, feed_id, feed_name, type):
        data = response["data"]

        print "[>] RECEIVED to PAGINATE: " + str(feed_id)
        pprint(feed_name)

        for post in data:
            if post.has_key("created_time"):
                created_time =  self.sc.convert_facebook_datetime(post["created_time"])
                if created_time < self.ignore_before:
                    next_visit = self.sc.set_next_visit(ConfReader.REVISIT_INCREMENT)
                    self.eb.update_feed(feed_id, next_visit["raw"], locale="BR")
                    self.bt.add_to_batch(feed_id, feed_name, type, next_visit["minutes"])
                    return True

            if post.has_key("id") and post.has_key("from"):
                post_id = post["id"]
                post_name = post["from"]["id"]

                print "Discovered (post): " + post["id"]
                if post.has_key("message"):
                    post_message = post["message"]
                else:
                    post_message = ""
                post_categories = self.pf.categorize_post(post_message)
                self.eb.add_post(post["id"], post_message, post["from"]["id"], post["from"]["name"], post_categories)

                try:
                    feed_info = self.eb.get_feed(post["from"]["id"])
                    feed_type = feed_info["type"]
                except elasticsearch.NotFoundError:
                    feed_type = self.check_feed_type(post["from"]["id"])

                post_feed_type = feed_type

                next_visit = self.sc.set_next_visit()
                self.eb.add_feed(post["from"]["id"], post["from"]["name"], feed_type, next_visit["raw"])
                self.bt.add_to_batch(post["from"]["id"], post["from"]["name"], feed_type, next_visit["minutes"])

            if post.has_key("to"):
                data = post["to"]["data"]
                for to in data:
                    if to.has_key("category") and to.has_key("name") and to.has_key("id"):
                        print "Discovered (to): " + to["id"] + ", " + to["name"]
                        next_visit = self.sc.set_next_visit()
                        self.eb.add_feed(to["id"], to["name"], "page", next_visit["raw"])
                        self.bt.add_to_batch(to["id"], to["name"], "page", next_visit["minutes"])

            if post.has_key("likes"):
                data = post["likes"]["data"]
                for like in data:
                    print "Discovered (likes): " + like["id"] + ", " + like["name"]
                    try:
                        feed_info = self.eb.get_feed(like["id"])
                        feed_type = feed_info["type"]
                    except elasticsearch.NotFoundError:
                        feed_type = self.check_feed_type(like["id"])
                    # Discovered (likes): 427049834127522, Eloi Neto Netinho raise [100] Unsupported get request
                    next_visit = self.sc.set_next_visit()

                    self.eb.add_feed(like["id"], like["name"], feed_type, next_visit["raw"])
                    self.bt.add_to_batch(like["id"], like["name"], feed_type, next_visit["minutes"])
                paging = post["likes"]["paging"]
                if paging.has_key("next"):
                    next = paging["next"]
                    next_visit = self.sc.set_next_visit()
                    self.bt.add_to_batch(post_id, post_name, post_feed_type,
                                         next_visit["minutes"], url_params="?" + next.split("?")[1])


            if post.has_key("comments"):
                data = post["comments"]["data"]
                for comment in data:
                    if comment.has_key("message_tags"):
                        message_tags = comment["message_tags"]
                        for message_tag in message_tags:
                            if message_tag.has_key("id") and message_tag.has_key("name"):
                                print "Discovered (message_tags): " + message_tag["id"] + ", " + message_tag["name"]
                                next_visit = self.sc.set_next_visit()
                                self.eb.add_feed(message_tag["id"], message_tag["name"],
                                                 message_tag["type"], next_visit["raw"])
                                self.bt.add_to_batch(message_tag["id"], message_tag["name"],
                                                     message_tag["type"], next_visit["minutes"])
                    try:
                        feed_info = self.eb.get_feed(comment["from"]["id"])
                        feed_type = feed_info["type"]
                    except elasticsearch.NotFoundError:
                        feed_type = self.check_feed_type(comment["from"]["id"])

                    next_visit = self.sc.set_next_visit()
                    self.eb.add_feed(comment["from"]["id"], comment["from"]["name"], feed_type, next_visit["raw"])
                    self.bt.add_to_batch(comment["from"]["id"], comment["from"]["name"],
                                         feed_type, next_visit["minutes"])
                    post_categories = self.pf.categorize_post(comment["message"])
                    self.eb.add_comment(comment["id"], comment["message"], post_id,
                                        comment["from"]["id"], comment["from"]["name"], post_categories)

                paging = post["comments"]["paging"]
                if paging.has_key("next"):
                    next = paging["next"]
                    next_visit = self.sc.set_next_visit()
                    self.bt.add_to_batch(post_id, post_name, post_feed_type, next_visit["minutes"],
                                         url_params="?" + next.split("?")[1])

        if response.has_key("paging"):
            paging = response["paging"]
            if paging.has_key("next"):
                next = paging["next"]
                next_visit = self.sc.set_next_visit()
                self.bt.add_to_batch(post_id, post_name, post_feed_type, next_visit["minutes"],
                                     url_params="?" + next.split("?")[1])

        next_visit = self.sc.set_next_visit(ConfReader.REVISIT_INCREMENT)

        if "_" not in feed_id:
            # feed_id is not a comment
            self.eb.update_feed(feed_id, next_visit["raw"], locale="BR")
            feed_info = self.eb.get_feed(feed_id)
            self.bt.add_to_batch(feed_info["feed-id"], feed_info["name"], feed_info["type"], next_visit["minutes"])

        return True

    def paginate_likes(self, post_id, post_name, post_feed_type, next):
        likes = self.graphFBAPI.get(post_id + "/likes" + next)
        data = likes["data"]

        for like in data:
            print "Discovered (likes): " + like["id"] + ", " + like["name"]

            try:
                feed_info = self.eb.get_feed(like["id"])
                feed_type = feed_info["type"]
            except elasticsearch.NotFoundError:
                feed_type = self.check_feed_type(like["id"])

            next_visit = self.sc.set_next_visit()
            self.eb.add_feed(like["id"], like["name"], feed_type, next_visit["raw"])
            self.bt.add_to_batch(like["id"], like["name"], feed_type, next_visit["minutes"])

        if likes.has_key("paging"):
            paging = likes["paging"]
            if paging.has_key("next"):
                next = paging["next"]
                next_visit = self.sc.set_next_visit()
                self.bt.add_to_batch(post_id, post_name, post_feed_type, next_visit["minutes"],
                                     url_params="?" + next.split("?")[1])

    def paginate_comments(self, post_id, post_name, post_feed_type, next):
        comments = self.graphFBAPI.get(post_id + "/comments" + next)
        data = comments["data"]

        for comment in data:
            if comment.has_key("message_tags"):
                message_tags = comment["message_tags"]
                for message_tag in message_tags:
                    if message_tag.has_key("id") and message_tag.has_key("name"):
                        print "Discovered (message_tags): " + message_tag["id"] + ", " + message_tag["name"]
                        next_visit = self.bt.set_next_visit()
                        self.eb.add_feed(message_tag["id"], message_tag["name"],
                                         message_tag["type"], next_visit["raw"])
                        self.bt.add_to_batch(message_tag["id"], message_tag["name"],
                                              message_tag["type"], next_visit["minutes"])
            try:
                feed_info = self.eb.get_feed(comment["from"]["id"])
                feed_type = feed_info["type"]
            except elasticsearch.NotFoundError:
                feed_type = self.check_feed_type(comment["from"]["id"])

            next_visit = self.sc.set_next_visit()
            self.eb.add_feed(comment["from"]["id"], comment["from"]["name"], feed_type, next_visit["raw"])
            self.bt.add_to_batch(comment["from"]["id"], comment["from"]["name"], feed_type, next_visit["minutes"])
            post_categories = self.pf.categorize_post(comment["message"])
            self.eb.add_comment(comment["id"], comment["message"], post_id,
                                comment["from"]["id"], comment["from"]["name"], post_categories)

        if comments.has_key("paging"):
            paging = comments["paging"]
            if paging.has_key("next"):
                next = paging["next"]
                next_visit = self.sc.set_next_visit()
                self.bt.add_to_batch(post_id, post_name, post_feed_type, next_visit["minutes"],
                                     url_params="?" + next.split("?")[1])


if __name__ == '__main__':
    vs = Visitor()

