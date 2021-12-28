#! /usr/local/bin/python3
""" Count how often students transfer courses to a college. Using the source course_id:offer_nbr
    and destination institution; count the number of cases where the course transfers only as
    blanket credit.
    This is an alternate to count_transfers.py. This processes the CUNYfirst query output directly,
    whereas count_transfers.py worked from the transfers_applied table of the database.
    2021-11-27: Ran the CUNYfirst query with a longer history (start term 1152) to try to get a
    better sample size.
"""

import argparse
import csv
import os
import psycopg
import sys
import time

from collections import defaultdict, namedtuple
from pathlib import Path
from psycopg.rows import namedtuple_row

DEBUG = os.getenv('DEBUG_TRANSFER_STATISTICS')


def stats_factory():
  return {'student_ids': set(),
          'src_institution': 'unknown',
          'total_transfers': 0,
          'distinct_transfers': 0}
  # return Stats._make([set(), set(), 0, 0])._asdict()


def chunks(lst, n):
    """Yield successive n-sized chunks from lst.
       Stack Overflow https://stackoverflow.com/questions/312443/
                              how-do-you-split-a-list-into-evenly-sized-chunks
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def elapsed(since: float):
  h, ms = divmod(int(time.time() - since), 3600)
  m, s = divmod(ms, 60)
  return f'{h:02}:{m:02}:{s:02}'


if __name__ == '__main__':

  parser = argparse.ArgumentParser('Transfer Statistics App')
  parser.add_argument('-b', '--build_bkcr_only', action='store_true')
  parser.add_argument('-c', '--count_transfers', action='store_true')

  args = parser.parse_args()
  if args.build_bkcr_only:
    latest_query = None
    query_files = Path('./downloads/').glob('*csv')
    for query_file in query_files:
      if latest_query is None:
        latest_query = query_file
        latest_timestamp = query_file.stat().st_mtime
      else:
        this_timestamp = query_file.stat().st_mtime
        if this_timestamp > latest_timestamp:
          latest_query = query_file
          latest_timestamp = this_timestamp
    print(f'Using {latest_query.name}\nBuild bkcr-only Dict')
    session_start = time.time()

    # Create list of (course_id, offer_nbr, dst) tuples where the course appears is part of a rule
    # that transfers only as BKCR.
    with psycopg.connect('dbname=cuny_curriculum') as conn:
      with conn.cursor(row_factory=namedtuple_row) as cursor:
        only_bkcr = defaultdict(list)

        cursor.execute("""
        select s.course_id, s.offer_nbr, r.destination_institution as dest
        from source_courses s, transfer_rules r, destination_courses d
        where s.rule_id = d.rule_id
          and s.rule_id = r.id
          and true = ALL(select c.attributes ~* 'bkcr'
                           from cuny_courses c
                          where c.course_id = d.course_id
                            and c.offer_nbr = d.offer_nbr)
        """)
        for row in cursor:
          only_bkcr[row.dest].append((row.course_id, row.offer_nbr))
        total = sum([len(only_bkcr[institution]) for institution in only_bkcr.keys()])
        print(f'{total:,} blcr-only rules\n{elapsed(session_start)}')

      # Find all rules for transferring each of the above courses to the destination institution.
      with conn.cursor(row_factory=namedtuple_row) as cursor:
        course_rules = dict()
        for dest in sorted(only_bkcr.keys()):
          dest_start = time.time()
          rule_counts = []
          print(f'\n{dest[0:3]}: {len(only_bkcr[dest]):8,} only-bkcr rules')
          for sublist in chunks(only_bkcr[dest], 5000):
            courses_str = ', '.join([f'\n({course[0]},{course[1]})' for course in sublist])
            query = f"""
            select course_id, offer_nbr, string_agg(rule_key, ' ') as rules
              from transfer_rules r, source_courses s
             where r.destination_institution = '{dest}'
               and s.rule_id = r.id
               and (s.course_id, s.offer_nbr) in ({courses_str})
            group by course_id, offer_nbr
            """
            cursor.execute(query)
            min_count = 999
            max_count = 0
            for row in cursor:
              rules_list = row.rules.split()
              if (list_len := len(rules_list)) > max_count:
                max_count = list_len
              if list_len < min_count:
                min_count = list_len

              # Gather gross (cuny-wide) statistics on how many rules there are per course-dst pair
              print(f'\r  {len(rule_counts):,} / {len(only_bkcr[dest]):,}', end='')
              rule_counts.append(len(rules_list))
              course_rules[(row.course_id, row.offer_nbr, dest)] = rules_list
            # break  # after first chunk
          print('\r' + 80 * ' ' + f'\r  {elapsed(dest_start)}\n'
                f'  min: {min_count}\n  max: {max_count}\n'
                f'  avg: {(sum(rule_counts) / len(rule_counts)):0.3f}')
          # break  # after Baruch

        print(f'Build Total: {elapsed(session_start)}\n\nPopulate bkcr_course_rules Table')

    with psycopg.connect('dbname=cuny_curriculum') as conn:
      conn.autocommit = True
      populate_start = time.time()
      with conn.cursor() as cursor:
        cursor.execute("""
        drop table if exists bkcr_course_rules;
        create table bkcr_course_rules (
        course_id int,
        offer_nbr int,
        source text,
        destination text,
        num_rules int,
        rules text,
        primary key (course_id, offer_nbr, destination)
        )
        """)
        with cursor.copy('copy bkcr_course_rules '
                         '(course_id, offer_nbr, source, destination, num_rules, rules) '
                         'from stdin') as copy:
          counter = 0
          total = len(course_rules)
          for key, value in course_rules.items():
            sources = set()
            dests = set()
            this_dest = key[2].upper()
            dests.add(this_dest)
            for rule in value:
              parts = rule.split(':')
              sources.add(parts[0])
              dests.add(parts[1])
            assert len(sources) == 1, f'{key} {value} {sources}'
            assert len(dests) == 1, f'{key} {value} {dests}'
            counter += 1
            print(f'\r  {counter:,} / {total:,}', end='')
            values = [key[0], key[1], sources.pop(), key[2], len(value), ' '.join(value)]
            copy.write_row(values)
        print(f'\nPopulate took {elapsed(populate_start)}')

  if args.count_transfers:
    # Now go through the transfer evaluations and count how often the courses with a bkcr-only rule
    # transferred as (a) bkcr and (b) with other destination course ids.

    # Dict key is (src_course, dst_institution)
    # names for fields
    src_institution = 'src_institution'
    total_transfers = 'total_transfers'
    student_ids = 'student_ids'
    distinct_transfers = 'distinct_transfers'
    stats = defaultdict(stats_factory)

    with open(latest_query, 'r', newline='', errors='replace') as csv_file:
      csv_reader = csv.reader(csv_file)
      for line in csv_reader:
        if csv_reader.line_num == 1:
          cols = [c.lower().replace(' ', '_') for c in line]
          Row = namedtuple('Row', cols)
        else:
          row = Row._make(line)
          course = f'{row.src_course_id:06}:{row.src_offer_nbr}'
          dst = row.dst_institution[0:3]
          key = (course, dst)
          stats[key][src_institution] = row.src_institution[0:3]
          stats[key][total_transfers] += 1
          if row.student_id in stats[key][student_ids]:
            pass
          else:
            stats[key][student_ids].add(row.student_id)
            stats[key][distinct_transfers] += 1

    with open('./reports/count_transfers_raw.csv', 'w') as csv_file:
      csv_writer = csv.writer(csv_file)
      csv_writer.writerow(['To', 'From', 'Course', 'Actual Count', 'Total Count'])

      for key, value in stats.items():
        course, dst = key
        csv_writer.writerow([dst,
                            value[src_institution],
                            course,
                            value[distinct_transfers],
                            value[total_transfers]])
