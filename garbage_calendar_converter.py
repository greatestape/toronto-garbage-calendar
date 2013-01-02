#!/usr/bin/env python

from collections import defaultdict
import csv
import datetime
import re
import sys
import uuid

import icalendar
import pytz

def parse_garbage_file(f):
    """
    Takes a filehandle to a City of Toronto Solid Waste Pickup Schedule CSV
    (See: http://www1.toronto.ca/wps/portal/open_data/open_data_item_details?vgnextoid=2ee34b5073cfa310VgnVCM10000071d60f89RCRD&vgnextchannel=6e886aa8cc819210VgnVCM10000067d60f89RCRD)
    Returns an iterable of calendar name, icalendar.Calendar tuples.
    """
    reader = csv.DictReader(f)
    def make_calendar():
        return icalendar.Calendar()
    calendars = defaultdict(make_calendar)
    for row in reader:
        calendar = row.pop('Calendar')
        sunday_date = datetime.datetime.strptime(row.pop('WeekStarting'),
                                                 '%m/%d/%Y').date()
        for event in parse_row(sunday_date, row):
            calendars[calendar].add_component(event)

    for calendar_name in calendars.keys():
        calendar = calendars[calendar_name]
        calendar.add('version', '2.0')
        calendar.add('prodid', '-//Great Ape Synergies'
                               '//Toronto Waste Collection {0}'
                               '//EN'.format(calendar_name))
        yield (calendar_name, calendar)


def parse_row(sunday_date, pickup_schedule):
    """
    Takes the date indicating the Sunday before the week in question,
    and a dictionary showing the pickup date for each pickup type.
    Returns an iterable of icalendar Event objects, one representing
    each distinct pickup day, and what will be picked up.
    """
    for iso_weekday, summary in parse_pickups(pickup_schedule):
        if iso_weekday == 7:
            iso_weekday = 0
        date = sunday_date + datetime.timedelta(days=iso_weekday)
        event = icalendar.Event()
        event.add('summary', summary)
        event.add('dtstart', date)
        event.add('dtend', date)
        event.add('dtstamp', datetime.datetime.now(pytz.utc))
        event.add('uid', unicode(uuid.uuid4()))
        event.add('class', 'PUBLIC')
        yield event


# Map the city's day codes to ISO weekday codes. There's apparently no letter
# for Sunday.
WEEKDAY_MAP = {'M': 1, 'T': 2, 'W': 3, 'R': 4, 'F': 5, 'S': 6}


def parse_pickups(pickup_schedule):
    """
    Takes a dictionary mapping pickup types to pickup days and returns
    an iterable of day offset, pickup description tuples, representing
    each distinct day on the week when a pickup happens.
    """
    pickups_by_day = defaultdict(list)
    for pickup, day in pickup_schedule.items():
        if day != '0':
            pickups_by_day[WEEKDAY_MAP[day]].append(un_camel_case(pickup))
    return ((iso_weekday, '{0} pickup'.format(', '.join(sorted(pickups))))
            for iso_weekday, pickups in pickups_by_day.items())


def un_camel_case(name):
    """
    Takes a string and returns a new string with all instances of CamelCase
    replaced with regular, space-separated words.
    """
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1 \2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1 \2', s1)


if __name__ == '__main__':
    for name, calendar in parse_garbage_file(open(sys.argv[1], 'rb')):
        f = open('{0}.ics'.format(name), 'wb')
        f.write(calendar.to_ical())
        f.close()
