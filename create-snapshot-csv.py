#!/usr/bin/env python

# compatible with python 2 and 3

import bz2
import calendar
import codecs
import csv
import datetime
import errno
import functools
import io
import os
import sqlite3
import sys

def naive_utc_datetime_timestamp(dt):
    return (dt - datetime.datetime(1970, 1, 1)).total_seconds()

def string_to_unicode_py2(a):
    return a.decode('utf-8')

def string_to_unicode_py3(a):
    return a

if sys.version_info[:2] >= (3, 0):
    string_to_unicode = string_to_unicode_py3
else:
    string_to_unicode = string_to_unicode_py2

COLS = ['correlate_id',
        'vote',
        'is_public',
        'datetime_of_vote',
        'twitter_client',
        'is_verified',
        'lang',
        'time_zone',
        'utc_offset',
        'followers_count',
        'location_confidence',
        'location_id',
        'location_name',
        'age','gender']
    
def import_diff_file(conn, diff_file):
    if sys.version_info[:2] >= (3, 0):
        diff_file = io.TextIOWrapper(diff_file, encoding='utf-8')
    
    # we will normalize these columns by storing their values as integers
    ENUMS = ['vote', 'lang', 'time_zone',
             'location_name', 'gender', 'twitter_client']

    transform_cursor = conn.cursor()
    def transform_value(row, x):
        toret = string_to_unicode(row[x])
        if not toret: return None
        if x == 'correlate_id':
            toret = sqlite3.Binary(codecs.decode(toret, 'hex'))
        elif x == 'datetime_of_vote':
            if toret[23] != 'Z':
                raise Exception("Unsupported timestamp value: %r" % (toret,))
            toret = naive_utc_datetime_timestamp(datetime.datetime.strptime(toret[:19], "%Y-%m-%dT%H:%M:%S"))
        elif x in ENUMS:
            # resolve enum
            res = transform_cursor.execute("select value from enums where name = ?",
                                           (toret,))
            toret = res.fetchone()[0]

        return toret

    cursor = conn.cursor()

    for row in csv.DictReader(diff_file):
        def generate_enums():
            for name in ENUMS:
                if row[name]:
                    yield (string_to_unicode(row[name]),)

        # first insert all the enums
        cursor.executemany("insert or ignore into enums (name) values (?)",
                           generate_enums())

        cursor.execute("insert or replace into votes (%s) values (%s)"
                       % (','.join(COLS), ','.join('?' * len(COLS))),
                       list(map(functools.partial(transform_value, row), COLS)))

def import_all_data(conn):
    for year in sorted(map(int, os.listdir("diffs"))):
        for month in sorted(map(int, os.listdir("diffs/%04d" % (year,)))):
            for day in range(1, 1 + calendar.monthrange(year, month)[1]):
                zipped_diff_file_path = ("diffs/%04d/%02d/diff-%04d-%02d-%02d.csv.bz2" %
                                         (year, month, year, month, day))

                try:
                    f = bz2.BZ2File(zipped_diff_file_path, "r")
                except IOError as e:
                    if e.errno == errno.ENOENT:
                        continue
                else:
                    with f:
                        import_diff_file(conn, f)


def export_all_data(conn):
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
select
hex(correlate_id) as correlate_id,
(select name from enums where value = vote) as vote,
is_public,
strftime('%Y-%m-%dT%H:%M:%S.000Z', datetime_of_vote, 'unixepoch') as datetime_of_vote,
(case when twitter_client is NULL then '' else (select name from enums where value = twitter_client) end) as twitter_client,
is_verified,
(select name from enums where value = lang) as lang,
(case when time_zone is NULL then '' else (select name from enums where value = time_zone) end) as time_zone,
ifnull(utc_offset, '') as utc_offset,
followers_count,
(case when location_confidence is NULL then '' else location_confidence end) as location_confidence,
(case when location_id is NULL then '' else location_id end) as location_id,
(case when location_name is NULL then '' else (select name from enums where value = location_name) end) as location_name,
(case when age is NULL then '' else age end) as age,
(case when gender is NULL then '' else (select name from enums where value = gender) end) as gender
from votes
order by votes.correlate_id;
""")

    csvfile = sys.stdout
    
    with csvfile:
        writer = csv.DictWriter(csvfile, COLS)
        writer.writeheader()

        def convert_val_py2(a):
            return a.encode('utf-8') if isinstance(a, unicode) else a

        def convert_val_py3(a):
            return a

        if sys.version_info[:2] >= (3, 0):
            convert_val = convert_val_py3
        else:
            convert_val = convert_val_py2
        
        for row in cursor:
            towrite = {}
            for k in row.keys():
                towrite[k] = convert_val(row[k])

            writer.writerow(towrite)
            
def main():
    conn = sqlite3.connect("")

    conn.execute("create table if not exists enums (name text not null unique, value integer primary key)");

    conn.execute("create table if not exists votes (correlate_id blob primary key, vote integer not null, is_public integer not null, datetime_of_vote integer not null, twitter_client integer, is_verified integer not null, lang integer not null, time_zone integer, utc_offset integer, followers_count integer not null, location_confidence integer, location_id integer, location_name integer, age integer, gender integer)")

    import_all_data(conn)

    export_all_data(conn)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
