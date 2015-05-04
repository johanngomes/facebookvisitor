__author__ = 'johann'

import time

from random import randint
from datetime import datetime, timedelta
from conf.confreader import ConfReader

class Schedule:
    def __init__(self, utc):
        self.utc = utc

    def convert_facebook_datetime(self, facebook_datetime):
        return datetime.strptime(facebook_datetime ,'%Y-%m-%dT%H:%M:%S+0000')

    def format_next_visit(self, visit):
        raw = datetime.strptime(visit.split(".")[0] ,'%Y-%m-%dT%H:%M:%S')

        minutes = round((time.mktime(raw.timetuple()) - time.mktime(self.utc.utcnow().timetuple()))/60)
        seconds = round(time.mktime(raw.timetuple()) - time.mktime(self.utc.utcnow().timetuple()))

        return {'raw' : raw, 'minutes' : minutes, 'seconds' : seconds}

    def set_next_visit(self, delay=False):
        if delay:
            minutes_increment = ConfReader.VISIT_MULTIPLE * randint(ConfReader.REVISIT_INCREMENT[0], ConfReader.REVISIT_INCREMENT[1])
        else:
            minutes_increment = ConfReader.VISIT_MULTIPLE * randint(ConfReader.VISIT_INCREMENT[0], ConfReader.VISIT_INCREMENT[1])
        next_visit = self.utc.utcnow().replace(second=0, microsecond=0) + timedelta(minutes=minutes_increment)
        next_visit_minutes = next_visit.minute
        if next_visit_minutes % ConfReader.VISIT_MULTIPLE != 0:
            ceiling, floor = next_visit_minutes, next_visit_minutes
            while floor % ConfReader.VISIT_MULTIPLE != 0 and ceiling % ConfReader.VISIT_MULTIPLE != 0:
                floor -= 1
                ceiling += 1
            if floor % ConfReader.VISIT_MULTIPLE == 0 and floor > 0:
                next_visit = self.utc.utcnow().replace(second=0, microsecond=0) + timedelta(minutes=floor)
            elif ceiling % ConfReader.VISIT_MULTIPLE == 0 and ceiling > 0:
                next_visit = self.utc.utcnow().replace(second=0, microsecond=0) + timedelta(minutes=ceiling)

        minutes = round((time.mktime(next_visit.timetuple()) - time.mktime(self.utc.utcnow().timetuple()))/60)
        seconds = round(time.mktime(next_visit.timetuple()) - time.mktime(self.utc.utcnow().timetuple()))

        while minutes % ConfReader.VISIT_MULTIPLE != 0:
            minutes += 1
            if minutes > 59: minutes = 1

        if minutes < 0 or seconds < 0:
            print "** [X] WARNING: MINUTES OR SECONDS < 0. PROGRAM TERMINATED. **\nSource log:"
            print "minutes: " + str(minutes) + " seconds: " + str(seconds) + " next visit: " + str(next_visit)
            raise ArithmeticError
            exit()

        return {'raw' : next_visit, 'minutes' : minutes, 'seconds' : seconds}

#if __name__ == '__main__':
#    sc = Schedule(UTC())
#    for x in range(100):
#        print sc.set_next_visit()


