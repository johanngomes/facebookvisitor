__author__ = 'johann'

import json
import facepy
from pprint import pprint

class ManualFeeder:

    def __init__(self, graph_api):
        self.graph_api = graph_api

        file = open('conf/termgroups.json')
        self.data = json.load(file)
        file.close()

        self.data_sources = self.data["dataSources"]

        self.extract_facebook_sources()

    def extract_facebook_sources(self):
        print "[>] Extracting pre-loaded Facebook sources..."

        sources = []
        requests = []

        for source in self.data_sources:
            if source["source"].upper() == "FACEBOOK":
                requests.append({ 'method':'GET', 'fb_id' : source["id"], 'relative_url': source["id"] + "?metadata=1"})

        responses = self.graph_api.batch(requests=requests)

        for request, response in zip(requests, responses):
            if isinstance(response, facepy.FacebookError):
                print "[!] Pre-loaded feed id " + request["fb_id"] + " isn't available: " + str(response)
            else:
                sources.append( { "id" : response["id"], "name" : response["name"], "type" : response["metadata"]["type"] } )

        if not sources:
            print "[!] No Facebook sources found."

        return sources


if __name__ == '__main__':
    graph_api_access = facepy.GraphAPI("CAAPFlrZA3fakBAPEjfLiYPXTbZBnFHcXO0dM4lzEAji9x84rN5UvHpnZAjhZBiiEFqW1X4BPMPR5A1FjjNZAqdTNDy47a0VZCJuhgeeyljYwQGVoUiUeW9k7ZBvFJb5wZCRbdwYfMZAnZBP4Pbr1n0P8qGajpWZCsmZACjYnLRdwZCVYoUKgiQU6fsSWWzc2xu4wXdjPlZBdkBwnKFW72mPUZBQ3i4x5qmQtnPEksYZD")

    mf = ManualFeeder(graph_api_access)

