__author__ = 'johann'

import unicodedata
import re
import guess_language
from conf.confreader import ConfReader
#from initializer import Initializer
from elasticbridge import ElasticBridge
from facepy import GraphAPI
from facebookauthenticator import FacebookAuthenticator

class FeedFilter:

    def __init__(self, eb):
        #Initializer.__init__(self)
        #self.initialize_facebook_graph_api()

        self.places = []
        print "[>] Extracting places from 'brazilian-places' index in ElasticSearch... Please wait."

        #eb_places = eb.get_all_places()
        #
        #for place in eb_places:
        #    if place["_source"]["name"] != "": self.places.append(place["_source"]["name"])

        print "[>] Extraction finished."

    def strip_accents(self, string):
        return ''.join((c for c in unicodedata.normalize('NFD', unicode(string)) if unicodedata.category(c) != 'Mn')).upper()

    def detect_place_mention(self, response):
        data = response["data"]

        for post in data:
            if post.has_key("message"):
                post_message = self.strip_accents(post["message"])
                for place in self.places:
                    match = re.search(" " + place, post_message)
                    if match:
                        return "BR"


            if post.has_key("comments"):
                data = post["comments"]["data"]
                for comment in data:
                    comment_message = self.strip_accents(comment["message"])
                    for place in self.places:
                        match = re.search(" " + place, comment_message)
                        if match:
                            return "BR"

        return "UNKNOWN"

    def detect_portuguese_language(self, response):
        guessings = []

        data = response["data"]

        for post in data:
            if post.has_key("message"):
                post_message = self.strip_accents(post["message"])
                guessings.append(guess_language.guessLanguageTag(post_message))

            if post.has_key("comments"):
                data = post["comments"]["data"]
                for comment in data:
                    comment_message = self.strip_accents(comment["message"])
                    guessings.append(guess_language.guessLanguageTag(comment_message))

        try:
            percentage_guessed_pt = ( guessings.count("pt") * 100 )/len(guessings)
        except ZeroDivisionError:
            print "[>] Feed without posts, we can't determine its locale."
            return "UNKNOWN"

        if percentage_guessed_pt > ConfReader.PORTUGUESE_GUESSED_PERCENTAGE:
            return "BR"
        else:
            return "UNKNOWN"

    def detect_locale(self, request, response):
        if request:
            print "[>] Detecting feed locale... ID: " + request["source_id"] + " Name: "+ request["source_name"]
        else:
            print "[>] Detecting feed locale. Unknown properties."

        #locale = self.detect_place_mention(response)
        #
        #if locale == "BR":
        #    print "[>] Locale is BR (place mentioned)"
        #    return "BR"
        #elif locale == "UNKNOWN":
        locale = self.detect_portuguese_language(response)
        if locale == "BR":
            print "[>] Locale is BR (portuguese language detection)"
            return "BR"
        elif locale == "UNKNOWN":
            print "[>] Locale is UNKNOWN. This feed will be skipped and its posts will never be saved."
            return "UNKNOWN"
        #else:
        #    print "[!] No locale. (?)"
        #    exit()


if __name__ == '__main__':
    ff = FeedFilter(ElasticBridge())

    fa = FacebookAuthenticator()
    access_token = fa.get_saved_access_token()
    graphFBAPI = GraphAPI(access_token)
    print "** Facebook Crawler is using App Access Token: " + access_token + " **"

    slow_feeds = ["570693256352787", "859054397469912", "635580286574784", "571240006244349"]

    for feed in slow_feeds:
        response = graphFBAPI.get(feed + "/links")
        print ff.detect_locale(False, response)

