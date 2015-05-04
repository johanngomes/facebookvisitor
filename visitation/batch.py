__author__ = 'johann'

import random

from conf.confreader import ConfReader
from pprint import pprint

class Batch:
    """Seconds to the visit correlationed to the sources to visit, in blocks with size determined by SOURCES_PER_SET
    in Set Class"""

    def __init__(self):
        self.batch = {}
        self.feeds_already_visited_in_this_session = []

    def add_to_batch(self, source_id, source_name, type, minutes_to_visit, url_params=""):
        if source_id not in self.feeds_already_visited_in_this_session:
            if minutes_to_visit == 0: minutes_to_visit = 1
            if not self.batch.has_key(minutes_to_visit):
                self.batch[minutes_to_visit] = Set()
            if type == "page" or type == "group" or type == "event":
                self.batch[minutes_to_visit].add_to_set(source_id, source_name, type, source_id + "/feed" + url_params)
            elif type == "user":
                self.batch[minutes_to_visit].add_to_set(source_id, source_name, type, source_id + "/links" + url_params)
            else:
                print "** [X] WARNING: SOURCE CANNOT BE ADDED TO BATCH BECAUSE IT DOESN'T HAVE A RECOGNIZED TYPE. " \
                      "PROGRAM TERMINATED. **\n Source log:"
                print source_id, source_name, type
                return None

        self.feeds_already_visited_in_this_session.append(source_id)

    def get_next_requests(self, minutes_to_visit, method='GET'):
        requests = []
        if self.batch.has_key(minutes_to_visit):
            try:
                current_set = self.batch[minutes_to_visit].set[0]
            except IndexError:
                print "** [!] Set doesn't have any more batchs. Deleting key. Search another key minutes_to_visit.**"
                del self.batch[minutes_to_visit]
                return False
            for subset in current_set:
                requests.append( { 'method': method, 'relative_url': subset['request_url'],
                                   'source_id': subset['source_id'], 'source_name': subset['source_name'],
                                   'type': subset['type'] } )
        return requests

    def delete_first_subset_of_requests(self, minutes_to_visit):
        if self.batch.has_key(minutes_to_visit):
            try:
                self.batch[minutes_to_visit].set.pop(0)
            except IndexError:
                print "** [!] Subset cannot be deleted. Set is empty for minute: " + str(minutes_to_visit) + ". Deleting key. **"
                del self.batch[minutes_to_visit]
        else:
            print "** [!] Key " + str(minutes_to_visit) + " not found. **"


class Set:
    """Sources divided by group of sources_per_set, for a specific seconds_to_visit dictionary key"""

    def __init__(self):
        self.SOURCES_PER_SET = ConfReader.SOURCES_PER_SET

        self.set = [[]]
        self.current_index = 0

    def add_to_set(self, source_id, source_name, type, request_url):
        if self.current_index >= len(self.set): self.current_index = len(self.set) - 1
        if len(self.set[self.current_index]) < self.SOURCES_PER_SET:
            self.set[self.current_index].append({ "source_id" : source_id, "source_name" : source_name,
                                                  "type" : type, "request_url" : request_url  })
        else:
            self.set.append([])
            self.current_index += 1

if __name__ == '__main__':
    bt = Batch()

    type = ["user", "page"]
    multiple = 2

    for x in range(200):
        type_i = random.randint(0,1)
        if type[type_i] == "user": url = "/links"
        else: url = "/feed"
        multiple_to_add = random.randint(1,5) * multiple

        bt.add_to_batch(str(random.getrandbits(128)), "Abelardo", type[type_i], multiple_to_add)

    pprint(bt.batch)

    for second in bt.batch.keys():
        print "Second is: " + str(second)
        for list in bt.batch[second].set:
            pprint(list)
            pprint(len(list))

    print "Next requests of 10"

    pprint(bt.get_next_requests(10))
    bt.delete_first_subset_of_requests(10)
    pprint(bt.get_next_requests(10))
    bt.delete_first_subset_of_requests(10)
    bt.get_next_requests(10)
    bt.delete_first_subset_of_requests(10)
    bt.delete_first_subset_of_requests(10)
    bt.delete_first_subset_of_requests(10)
    bt.delete_first_subset_of_requests(10)

    raise Exception
    #EXCEPTIONS NAO ESTAO SENDO MOSTRADAS QUERIDA!


    #set = Set(30)
    #for x in range(100):
    #    set.add_to_set(str(random.getrandbits(128)), "user")
    #print set.set
    #
    #for x in set.set:
    #    print len(x)


