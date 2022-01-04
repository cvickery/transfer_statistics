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
# import format_rules
import os
import psycopg
import subprocess
import sys
import time

from collections import defaultdict, namedtuple
from datetime import datetime
from openpyxl import Workbook, worksheet
from openpyxl.styles import Alignment, Font
from pathlib import Path
from psycopg.rows import namedtuple_row
from recordclass import recordclass

DEBUG = os.getenv('DEBUG_TRANSFER_STATISTICS')

DstCourse = recordclass('DstCourse', 'flags count rules')


def stats_factory():
  return {'student_ids': set(),
          'src_institution': 'unknown',
          'total_transfers': 0,
          'distinct_transfers': 0}


def dst_course_factory():
  return defaultdict(course_set_factory)


def course_set_factory():
  return DstCourse._make(('', 0, ''))


def dd_factory():
  """ For dict of dicts
  """
  return defaultdict(int)


def chunks(lst, n):
    """Yield successive n-sized chunks from lst.
       Stack Overflow https://stackoverflow.com/questions/312443/
                              how-do-you-split-a-list-into-evenly-sized-chunks
       This method is used to prevent the psycopg-3 driver from failing on large queries.
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def elapsed(since: float):
  """ Show the hours, minutes, and seconds that have elapsed since since seconds ago.
  """
  h, ms = divmod(int(time.time() - since), 3600)
  m, s = divmod(ms, 60)
  return f'{h:02}:{m:02}:{s:02}'


if __name__ == '__main__':

  session_start = time.time()
  parser = argparse.ArgumentParser('Transfer Statistics App')
  parser.add_argument('-b', '--build_bkcr_only', action='store_true')
  parser.add_argument('-c', '--count_transfers', action='store_true')

  args = parser.parse_args()
  if args.build_bkcr_only:
    print('Building bkcr-only Dict')

    # Create list of (course_id, offer_nbr, dst) tuples where the course appears as part of a rule
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
    # Go through the transfer evaluations and count how often the courses with a bkcr-only rule
    # transferred as (a) bkcr and (b) with other destination course ids. Count, also, the distinct
    # set of students affected for each transferred course.
    print('Count BKCR transfers')
    print('  Cache bkcr_course_rules table')
    count_start = time.time()

    # Cache the bkcr_only_rules table
    RuleInfo = namedtuple('RuleInfo', 'source rules')
    bkcr_rules = dict()
    with psycopg.connect('dbname=cuny_curriculum') as conn:
      with conn.cursor(row_factory=namedtuple_row) as cursor:
        cursor.execute('select * from bkcr_course_rules')
        for row in cursor:
          bkcr_rules[(row.course_id,
                      row.offer_nbr,
                      row.destination)] = RuleInfo._make([row.source, row.rules])
        print(f'  {len(bkcr_rules):,} rules took {elapsed(count_start)}')

        # Cache status of all bkcr_courses
        count_start = time.time()
        bkcr_courses = dict()
        cursor.execute("""
        select course_id, offer_nbr, course_status as status
        from cuny_courses
        where attributes ~* 'BKCR'
        """)
        for row in cursor:
          bkcr_courses[(int(row.course_id), int(row.offer_nbr))] = row.status
        # for key, value in bkcr_courses.items():
        #   print(key, value, file=sys.stderr)
        print(f'  {len(bkcr_courses):,} courses took {elapsed(count_start)}')

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
    print(f'  Lookup transfers using {latest_query.name}')
    lookup_start = time.time()

    report_name = f"./reports/{datetime.now().isoformat()[0:10]}.txt"
    report = open(report_name, 'w')
    num_transfers = defaultdict(int)
    student_sets = defaultdict(set)
    dst_courses = defaultdict(dst_course_factory)
    with open(latest_query, newline='', errors='replace') as query_file:
      reader = csv.reader(query_file)
      for line in reader:
        if reader.line_num == 1:
          Row = namedtuple('Row', [c.lower().replace(' ', '_') for c in line])
        else:
          row = Row._make(line)
          key = (int(row.src_course_id), int(row.src_offer_nbr), row.dst_institution)
          try:
            src_institution = bkcr_rules[key].source
            src_course = f'{row.src_subject:>6} {row.src_catalog_nbr.strip():<5}'
            dst_course = f'{row.dst_subject:>6} {row.dst_catalog_nbr.strip():<5}'.strip()
            dst_course_id = int(row.dst_course_id)
            dst_offer_nbr = int(row.dst_offer_nbr)
            try:
              course_status = bkcr_courses[(dst_course_id, dst_offer_nbr)]
              course_flags = ' B'
              if course_status == 'I':
                course_flags += 'I'
            except KeyError as ke:
              course_flags = ''

            num_transfers[(src_institution, src_course, row.dst_institution)] += 1
            student_sets[src_institution, src_course, row.dst_institution].add(row.student_id)
            # The next line is a mystery to me: it shouldn't be necessary explicitly to create the
            # dst_course recordclass separately from updating it. (REPL works without it.)
            dst_courses[(src_institution, src_course, row.dst_institution)][dst_course]
            dst_courses[(src_institution, src_course, row.dst_institution)][dst_course].count += 1

            dst_courses[(src_institution, src_course,
                         row.dst_institution)][dst_course].flags = course_flags
            dst_courses[(src_institution, src_course,
                         row.dst_institution)][dst_course].rules = bkcr_rules[key].rules
          except KeyError as ke:
            pass
    print(f'  Lookup took', elapsed(lookup_start))

    print('Generate Report')
    report_start = time.time()
    # Create separate dict for each college
    inst_dicts = defaultdict(dd_factory)
    for key in sorted(num_transfers.keys(), key=lambda k: k[2]):
      # dst_institution is key[2]
      inst_dicts[key[2]][key] = num_transfers[key]

    # Write the institution dicts to txt and xlsx
    centered = Alignment('center')
    bold = Font(bold=True)
    wb = Workbook()

    for key, value in inst_dicts.items():
      ws = wb.create_sheet(key)
      headings = ['Sending College', 'Course', 'Num Evaluations', 'Num Students',
                  'Receiving Courses', 'Rule Keys', 'Rule Descriptions']
      row = 1
      for col in range(len(headings)):
        ws.cell(row, col + 1, headings[col]).font = bold
        ws.cell(row, col + 1, headings[col]).alignment = centered

      for k, v in sorted(value.items(), key=lambda kv: kv[1], reverse=True):
        receivers = ', '.join([f'{c} [{v.count:,}]{v.flags}'
                               for c, v in dst_courses[k].items()])
        print(f'{key[0:3]}: {k[0][0:3]} {k[1]} {v:6,}: {receivers}', file=report)
        receivers = receivers.replace(', ', '\n')
        # Create list of all rules that _might_ have been involved in transferring this course
        all_rules = [v.rules.split() for v in dst_courses[k].values()]
        rules_set = set()
        for sublist in all_rules:
          for rule_str in sublist:
            rules_set.add(rule_str)
        row_values = [k[0][0:3], k[1], v, receivers, '\n'.join(sorted(rules_set))]

        row += 1
        for col in range(len(row_values)):
          ws.cell(row, col + 1).value = (row_values[col] if isinstance(row_values[col], int)
                                         else row_values[col].strip())
          ws.cell(row, col + 1).alignment = Alignment(horizontal='left',
                                                      vertical='top',
                                                      wrapText=True)
          if col == 2:
            ws.cell(row, col + 1).alignment = Alignment(horizontal='right', vertical='top')

    del wb['Sheet']
    wb.save('./reports/transfer_statistics.xlsx')
    print('Report generation took', elapsed(report_start))
    subprocess.run("pbcopy", universal_newlines=True, input=f'm {report_name}')

  print('Total time was', elapsed(session_start))
  exit()
