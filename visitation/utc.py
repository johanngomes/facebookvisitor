import urllib2
import socket
import time

from datetime import datetime
from conf.confreader import ConfReader

__author__ = 'johann'

class UTC:
    def __init__(self):
        self.scrapping_url = "http://www.worldtimeserver.com/current_time_in_UTC.aspx"
        self.utc_hours = self.get_utc_from_webscrapping()

    def get_utc_from_webscrapping(self, filter="HOURS"):
        try:
            data = urllib2.urlopen(self.scrapping_url)
        except urllib2.HTTPError:
            print self.scrapping_url + " unavailable. Trying again..."
            time.sleep(ConfReader.TRY_AGAIN_TIMEOUT)
            self.get_utc_from_webscrapping()
        except socket.error:
            print "Weird " + "socket.error: [Errno 10054] An existing connection was forcibly closed by the remote " \
                             "host. Probably, " + self.scrapping_url + " is unavailable. Trying again..."
            time.sleep(ConfReader.TRY_AGAIN_TIMEOUT)
            self.get_utc_from_webscrapping()

        web_source = data.readlines()
        for line_index in range(0 , len(web_source)):
            if "UTC/GMT is" in web_source[line_index]:
                if filter.upper() == "HOURS":
                    return int(web_source[line_index + 1].strip(" ")[:2])

    def utcnow(self):
        """Set the UTC time to self.utc_now. Python's datetime.utcnow() can't be used because it catch the time
         incorrectly, because it rely on OS environment variables."""

        utc_hours = self.utc_hours
        correct_hours = utc_hours - datetime.utcnow().hour
        #
        #if correct_hours > 0:
        return datetime.utcnow().replace(hour=datetime.utcnow().hour + correct_hours)
        #return datetime.utcnow().replace(hour=datetime.utcnow().hour + correct_hours -1, minute=59, second=03)
        #else:
        #    return datetime.utcnow().replace(hour=datetime.utcnow().hour + correct_hours, day=datetime.utcnow().day + 1)

if __name__ == '__main__':
    utc = UTC()
    print utc.utcnow()