from visitation.postfilter import PostFilter

__author__ = 'johann'

from facebookauthenticator import FacebookAuthenticator
from batch import Batch
from elasticbridge import ElasticBridge
from facepy import GraphAPI, exceptions
from schedule import Schedule
from utc import UTC
from postfilter import PostFilter
from feedfilter import FeedFilter
from manualfeeder import ManualFeeder
from urllib2 import urlopen, URLError

class Initializer:

    def __init__(self):
        if self.check_internet_connectivity():
            self.utc = UTC()
        else:
            print "[!] There is no internet connection available."
            exit()

        print "[>] Initializing Facebook Graph API"
        self.initialize_facebook_graph_api()

        print "[>] Initializing ElasticSearch"
        self.initialize_elasticsearch()

        print "[>] Initializing Batch"
        self.initialize_batch()

        print "[>] Initializing Schedule"
        self.initialize_schedule()

        print "[>] Initializing Post Filter"
        self.initialize_post_filter()

        print "[>] Initializing Feed Filter"
        self.initialize_feed_filter()

        print "[>] Initializing Manual Feeder"
        self.initialize_manual_feeder()

    def check_internet_connectivity(self):
        try:
            response = urlopen('https://www.google.com', timeout=1)
            return True
        except URLError as err: pass
        return False

    def initialize_facebook_graph_api(self):
        fa = FacebookAuthenticator()
        self.access_token = fa.get_saved_access_token()

        try:
            self.graphFBAPI = GraphAPI(self.access_token)
        except exceptions.OAuthError:
            self.access_token = fa.get_new_access_token()
            self.graphFBAPI = GraphAPI(self.access_token)

        print "** Facebook Crawler is using App Access Token: " + self.access_token + " **"

    def initialize_elasticsearch(self):
        self.eb = ElasticBridge()
        self.eb.check_index_creation()

    def initialize_batch(self):
        self.bt = Batch()

    def initialize_schedule(self):
        self.sc = Schedule(self.utc)

    def initialize_post_filter(self):
        self.pf = PostFilter()

    def initialize_feed_filter(self):
        self.ff = FeedFilter(self.eb)

    def initialize_manual_feeder(self):
        self.mf = ManualFeeder(self.graphFBAPI)
