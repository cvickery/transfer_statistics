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
import subprocess
import sys
import time

from adjustcolwidths import adjust_widths

from collections import defaultdict, namedtuple
from datetime import datetime
from openpyxl import Workbook, worksheet
from openpyxl.styles import Alignment, Font
from pathlib import Path
from psycopg.rows import namedtuple_row
from recordclass import recordclass

DEBUG = os.getenv('DEBUG_TRANSFER_STATISTICS')

DstCourse = recordclass('DstCourse', 'flags count num_rules rules')


def stats_factory():
  return {'student_ids': set(),
          'src_institution': 'unknown',
          'total_transfers': 0,
          'distinct_transfers': 0}


def dst_course_factory():
  return defaultdict(course_set_factory)


def course_set_factory():
  return DstCourse._make(('', 0, 0, ''))


def rule_descriptions_factory():
  """ Provide default value for rule_descriptions table rows not yet populated.
  """
  return 'Description not available'


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
  parser.add_argument('-b', '--build_bkcr_course_rules', action='store_true')
  parser.add_argument('-c', '--count_transfers', action='store_true')

  args = parser.parse_args()

  if args.build_bkcr_course_rules:
    # =============================================================================================
    # Create dict of (course_id, offer_nbr) tuples, indexed by college, where the sending course
    # appears as part of a rule that transfers only as BKCR course(s).
    print('Building bkcr-only Dict')
    with psycopg.connect('dbname=cuny_curriculum') as conn:
      with conn.cursor(row_factory=namedtuple_row) as cursor:
        only_bkcr = defaultdict(list)

        cursor.execute("""
        select s.course_id, s.offer_nbr, r.destination_institution as dest
        from source_courses s, transfer_rules r, destination_courses d
        where s.rule_id = r.id
          and d.rule_id = r.id
          and true = ALL(select c.attributes ~* 'bkcr'
                           from cuny_courses c
                          where c.course_id = d.course_id
                            and c.offer_nbr = d.offer_nbr)
        """)

        for row in cursor:
          only_bkcr[row.dest].append((row.course_id, row.offer_nbr))
        total = sum([len(only_bkcr[institution]) for institution in only_bkcr.keys()])
        print(f'{total:,} bkcr-only rules\n{elapsed(session_start)}')

        # Find all rules for transferring each of the above courses to the destination institution.
        query = f"""
        select s.course_id, s.offer_nbr, string_agg(rule_key, ' ') as rules
          from transfer_rules r, source_courses s
         where r.destination_institution = %s
           and s.rule_id = r.id
        group by course_id, offer_nbr
        """
        with open('bkcr-only.txt', 'w') as bkcr_only_file:
          course_rules = dict()
          for dest in sorted(only_bkcr.keys()):
            dest_start = time.time()
            rule_counts = []
            print(f'\n{dest[0:3]}: {len(only_bkcr[dest]):8,} only-bkcr rules')

            cursor.execute(query, (dest, ))
            min_count = 999
            max_count = 0
            for row in cursor:
              if (row.course_id, row.offer_nbr) in only_bkcr[dest]:
                rules_list = row.rules.split()

                # Gather statistics on how many rules there are per course-dst pair
                if (list_len := len(rules_list)) > max_count:
                  max_count = list_len
                if list_len < min_count:
                  min_count = list_len

                if (cursor.rownumber % 100) == 0:
                  print(f'\r  {cursor.rownumber:,} / {cursor.rowcount:,} total rules', end='')

                rule_counts.append(len(rules_list))
                course_rules[(row.course_id, row.offer_nbr, dest)] = rules_list

            print('\r' + 80 * ' ' + f'\r  {elapsed(dest_start)}')
            print(f'\n{dest[0:3]}: {len(only_bkcr[dest]):,} Only-blanket rules',
                  f'\n   avg: {(sum(rule_counts) / len(rule_counts)):0.3f} rules per sending course'
                  f'\n   min: {min_count}'
                  f'\n   max: {max_count}', file=bkcr_only_file)
            bkcr_only_file.flush()
            # break  # Uncomment this to break after Baruch

        print(f'Build Total: {elapsed(session_start)}\n\nPopulate bkcr_course_rules Table')
        populate_start = time.time()

    # Write the course_rules dict to the db for use in the count_transfers operation
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
    # =============================================================================================
    # Go through the transfer evaluations and count how often the courses with a bkcr-only rule
    # transferred as (a) bkcr and (b) with other destination course ids. Count, also, the distinct
    # set of students affected for each transferred course.

    # NEED TO COUNT STUDENTS WHO RECEIVED ONLY BKCR VERSUS MIX OF BKXR AND REAL (OR ONLY REAL) FOR
    # EACH SOURCE COURSE

    print('Count Transfers')
    count_start = time.time()

    # Cache the bkcr_course_rules table built above
    RuleInfo = namedtuple('RuleInfo', 'source num_rules rules')
    bkcr_rules = dict()
    with psycopg.connect('dbname=cuny_curriculum') as conn:
      with conn.cursor(row_factory=namedtuple_row) as cursor:
        cursor.execute('select * from bkcr_course_rules')
        for row in cursor:
          rules_list = row.rules.split()
          bkcr_rules[(row.course_id,
                      row.offer_nbr,
                      row.destination)] = RuleInfo._make([row.source,
                                                          row.num_rules,
                                                          sorted(rules_list)])

        print(f' {len(bkcr_rules):10,} BKCR rules.\t\t\t{elapsed(count_start)}')

        # List of rule_keys for bkcr-only rules
        cursor.execute("""
        select r.rule_key
        from transfer_rules r, destination_courses d
        where d.rule_id = r.id
          and true = ALL(select c.attributes ~* 'bkcr'
                           from cuny_courses c
                          where c.course_id = d.course_id
                            and c.offer_nbr = d.offer_nbr)
        """)
        bkcr_only_rule_keys = [row.rule_key for row in cursor]
        print(f' {len(bkcr_only_rule_keys):10,} BKCR-only rule keys.\t{elapsed(count_start)}')

        # Cache the decriptions of the rules referenced in bkcr_rules
        rule_descriptions = defaultdict(rule_descriptions_factory)
        cursor.execute("""
        select rule_key, description
        from rule_descriptions
        where rule_key in (select unnest(string_to_array(rules, E' '))
        from bkcr_course_rules)
        """)
        for row in cursor:
          rule_descriptions[row.rule_key] = row.description
        print(f' {len(rule_descriptions):10,} rule descriptions.\t\t{elapsed(count_start)}')

        # Cache metadata for all cuny courses
        meta_start = time.time()
        Metadata = namedtuple('Metadata', 'is_ugrad is_active is_mesg is_bkcr')

        def _meta_str_(self):
          """ Indicate whether a course is not undergraduate, inactive, mesg, and/or bkcr
          """
          return_str = ''
          if not self.is_ugrad:
            return_str += 'G'
          if not self.is_active:
            return_str += 'I'
          if self.is_mesg:
            return_str += 'M'
          if self.is_bkcr:
            return_str += 'B'
          return return_str
        setattr(Metadata, '__repr__', _meta_str_)

        metadata = dict()
        cursor.execute("""
        select course_id, offer_nbr,
               career ~* '^U' as is_ugrad,
               course_status = 'A' as is_active,
               designation in ('MNL', 'MLA') as is_mesg,
               attributes ~* 'bkcr' as is_bkcr
        from cuny_courses
        """)
        for row in cursor:
          metadata[(int(row.course_id), int(row.offer_nbr))] = Metadata._make([row.is_ugrad,
                                                                               row.is_active,
                                                                               row.is_mesg,
                                                                               row.is_bkcr])
        print(f' {len(metadata):10,} Course metadata.\t\t{elapsed(count_start)}')

    # Process the latest transfer evaluations query file.
    # ---------------------------------------------------
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

    print(f'Lookup transfers using {latest_query.name}')

    lookup_start = time.time()
    print(f'{len(open(latest_query, errors="replace").readlines()):,}')

    num_transfers = defaultdict(int)
    student_sets = defaultdict(set)
    dst_courses = defaultdict(dst_course_factory)

    with open(latest_query, newline='', errors='replace') as query_file:
      reader = csv.reader(query_file)
      for line in reader:
        print(f'\r{reader.line_num:,}', end='')
        if reader.line_num == 1:
          Row = namedtuple('Row', [c.lower().replace(' ', '_') for c in line])
        else:
          row = Row._make(line)
          key = (int(row.src_course_id), int(row.src_offer_nbr), row.dst_institution)
          try:
            src_institution = bkcr_rules[key].source
            num_rules = bkcr_rules[key].num_rules
            rule_keys = bkcr_rules[key].rules

            src_course_str = f'{row.src_subject:>6} {row.src_catalog_nbr.strip()}'.strip()

            dst_course_id = int(row.dst_course_id)
            dst_offer_nbr = int(row.dst_offer_nbr)
            dst_meta = metadata[(dst_course_id, dst_offer_nbr)]
            dst_course_str = f'{row.dst_subject:>6} {row.dst_catalog_nbr.strip()}'

            num_transfers[(src_institution, src_course_str, row.dst_institution)] += 1
            student_sets[src_institution, src_course_str, row.dst_institution].add(row.student_id)

            # The next line is a mystery to me: it shouldn't be necessary explicitly to create the
            # dst_course recordclass separately from updating it. (REPL works without it.)
            dst_courses[(src_institution, src_course_str, row.dst_institution)][dst_course_str]

            dst_courses[(src_institution, src_course_str,
                         row.dst_institution)][dst_course_str].count += 1
            dst_courses[(src_institution, src_course_str,
                         row.dst_institution)][dst_course_str].flags = dst_meta
            # Looking for cases where there is only one rule, which will be bkcr by definition
            dst_courses[(src_institution, src_course_str,
                         row.dst_institution)][dst_course_str].num_rules = num_rules
            dst_courses[(src_institution, src_course_str,
                         row.dst_institution)][dst_course_str].rules = rule_keys
          except KeyError as ke:
            # These are transfers where there is no bkcr-only rule for the sending course
            print(ke, file=sys.stderr)
            pass
    print(f'\n  Lookup took', elapsed(lookup_start))

    print('Generate Report')
    # =============================================================================================
    report_start = time.time()

    # Create separate dict for each college
    institution_dicts = defaultdict(dd_factory)
    for key in sorted(num_transfers.keys(), key=lambda k: k[2]):
      # dst_institution is key[2]
      institution_dicts[key[2]][key] = (num_transfers[key], len(student_sets[key]))

    # Write the institution dicts to xlsx
    centered = Alignment('center')
    bold = Font(bold=True)
    wb = Workbook()

    for key, value in institution_dicts.items():
      ws = wb.create_sheet(key[0:3])
      headings = ['Sending College', 'Sending Course', 'Number of Students',
                  'Number of Evaluations', 'Receiving Courses', 'Rule Descriptions']
      row = 1
      for col in range(len(headings)):
        ws.cell(row, col + 1, headings[col]).font = bold
        ws.cell(row, col + 1, headings[col]).alignment = Alignment(horizontal='center',
                                                                   vertical='top',
                                                                   wrapText=True)
      row_counter = 0
      print(f'\n{key[0:3]} {len(value):,}')
      for k, v in sorted(value.items(), key=lambda kv: kv[1], reverse=True):
        row_counter += 1
        print(f'\r    {row_counter:,}', end='')
        receivers = ', '.join([f'{c} [{v.count:,}]{v.flags}'
                               for c, v in dst_courses[k].items()])
        receivers = receivers.replace(', ', '\n')
        # Create list of all rules that _might_ have been involved in transferring this course
        all_rules = [v.rules for v in dst_courses[k].values()]
        rules_set = set()
        for sublist in all_rules:
          for rule_str in sublist:
            rules_set.add(rule_str)
        # k[0]          Sending College
        # k[1]          Sending Course
        # v[1]          Number of students
        # v[0]          Number of evaluations
        # receivers     Receiving courses
        # rules_set     Rule Keys
        # descriptions  Rule descriptions
        descriptions = []
        is_problematic = False
        for rule in rules_set:
          descriptions.append(f'{rule[12:].replace(":", " ")}: {rule_descriptions[rule]}')
        descriptions.sort()

        # A row is problematic if there is only one rule in the rules set: by definition, that's a
        # bkcr rule
        if len(rules_set) == 1 and rule in rules_set:
            is_problematic = True

        row_values = [k[0][0:3], k[1], v[1], v[0], receivers, '\n'.join(descriptions)]

        row += 1
        for col in range(len(row_values)):
          ws.cell(row, col + 1).value = (row_values[col] if isinstance(row_values[col], int)
                                         else row_values[col].strip())
          ws.cell(row, col + 1).alignment = Alignment(horizontal='left',
                                                      vertical='top',
                                                      wrapText=True)
          if col == 2 or col == 3:
            ws.cell(row, col + 1).alignment = Alignment(horizontal='right', vertical='top')
            ws.cell(row, col + 1).number_format = '#,##0'
          if is_problematic:
            ws.cell(row, col + 1).font = Font(bold=True, color='800080')

    del wb['Sheet']

    # Adjust Column Widths
    adjust_widths(wb)

    # Finish up
    wb.save('./reports/transfer_statistics.xlsx')
    print('\nReport generation took', elapsed(report_start))

  print('Total time was', elapsed(session_start))
  exit()
