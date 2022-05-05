#! /usr/local/bin/python3
""" Count how often students transfer courses to a college. Using the source course_id:offer_nbr
    and destination institution; count the number of cases where the course transfers only as
    blanket credit.
    This is an alternate to count_transfers.py. This processes the CUNYfirst query output directly,
    whereas count_transfers.py worked from the transfers_applied table of the database.
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

# Dict of info about sending courses and their associated xfer rules, indexed by
# receiving college, where the sending course appears as part of one or more rules resulting
# in blanket credit at the receiving college.

# A SrcCourse is one that has one or more xfer rule that awards bkcr
SrcCourse = namedtuple('SrcCourse', 'src_course_str, src_institution, rules')
src_courses = defaultdict(dict)  # Index by (src_course_id, src_offer_nbr, dst_institution)

# A DstCourse is a course on the receving side for a given SrcCourse-Receiver pair. The count
# field is updated as the transfers file is processed; hence the use of recordclass rather than
# namedtuple
DstCourse = recordclass('DstCourse', 'course_str, flags count')


def xfr_stats_factory():
  """ Indexed by (src_course_id, src_offer_nbr, dst_institution)
  """
  return defaultdict(course_set_factory)


def course_set_factory():
  """ Info to show about one destination course:
        (course_str, flags, count)
  """
  return DstCourse._make(('Unknown', '', 0))


def rule_descriptions_factory():
  """ Provide default value for rule_descriptions table rows not yet populated.
  """
  return 'Description not available'


def dd_factory():
  """ For dict of dicts
  """
  return defaultdict(int)


def elapsed(since: float):
  """ Show the hours, minutes, and seconds that have elapsed since since seconds ago.
  """
  h, ms = divmod(int(time.time() - since), 3600)
  m, s = divmod(ms, 60)
  return f'{h:02}:{m:02}:{s:02}'


if __name__ == '__main__':

  session_start = time.time()
  # parser = argparse.ArgumentParser('Transfer Statistics App')
  # parser.add_argument('-b', '--build_bkcr_course_rules', action='store_true')
  # parser.add_argument('-c', '--count_transfers', action='store_true')
  # args = parser.parse_args()
  log_file = open('transfer_statistics.log', 'w')
  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor(row_factory=namedtuple_row) as cursor:

      print('Rules')

      # cursor.execute("""
      # select s.course_id, s.offer_nbr, r.destination_institution as dest
      # from source_courses s, transfer_rules r, destination_courses d
      # where s.rule_id = r.id
      #   and d.rule_id = r.id
      #   and true = ALL(select c.attributes ~* 'bkcr'
      #                    from cuny_courses c
      #                   where c.course_id = d.course_id
      #                     and c.offer_nbr = d.offer_nbr)
      # """)

      cursor.execute("""
      select s.course_id, s.offer_nbr, s.discipline, s.catalog_number,
             r.source_institution,
             r.destination_institution,
             string_agg(rule_key, ' ') as rules
      from source_courses s, transfer_rules r, destination_courses d
      where s.rule_id = r.id
        and d.rule_id = r.id
        and (d.is_bkcr or d.is_mesg)
      group by s.course_id, s.offer_nbr, s.discipline, s.catalog_number, source_institution,
               destination_institution
      """)
      for row in cursor:
        course_str = f'{row.discipline.strip()} {row.catalog_number.strip()}'
        src_key = (row.course_id, row.offer_nbr, row.destination_institution)
        src_courses[src_key] = SrcCourse._make([course_str,
                                                row.source_institution,
                                                row.rules.split()])
      print(f'  {cursor.rowcount:10,} Sending Courses\t{elapsed(session_start)}')

      # # Find all rules for transferring each of the above courses to the destination institution.
      # query = f"""
      # select s.course_id, s.offer_nbr, string_agg(rule_key, ' ') as rules
      #   from transfer_rules r, source_courses s
      #  where r.destination_institution = %s
      #    and s.rule_id = r.id
      # group by course_id, offer_nbr
      # """
      # with open('bkcr-only.txt', 'w') as bkcr_only_file:
      #   course_rules = dict()
      #   for dest in sorted(only_bkcr.keys()):
      #     dest_start = time.time()
      #     rule_counts = []
      #     print(f'\n{dest[0:3]}: {len(only_bkcr[dest]):8,} only-bkcr rules')

      #     cursor.execute(query, (dest, ))
      #     min_count = 999
      #     max_count = 0
      #     for row in cursor:
      #       if (row.course_id, row.offer_nbr) in only_bkcr[dest]:
      #         rules_list = row.rules_str.split()

      #         # Gather statistics on how many rules there are per course-dst pair
      #         if (list_len := len(rules_list)) > max_count:
      #           max_count = list_len
      #         if list_len < min_count:
      #           min_count = list_len

      #         if (cursor.rownumber % 100) == 0:
      #           print(f'\r  {cursor.rownumber:,} / {cursor.rowcount:,} total rules', end='')

      #         rule_counts.append(len(rules_list))
      #         course_rules[(row.course_id, row.offer_nbr, dest)] = rules_list

      #     print('\r' + 80 * ' ' + f'\r  {elapsed(dest_start)}')
      #     print(f'\n{dest[0:3]}: {len(only_bkcr[dest]):,} Only-blanket rules',
      #           f'\n   avg: {(sum(rule_counts) / len(rule_counts)):0.3f} rules per sending course'
      #           f'\n   min: {min_count}'
      #           f'\n   max: {max_count}', file=bkcr_only_file)
      #     bkcr_only_file.flush()
      #     # break  # Uncomment this to break after Baruch

  #     # print(f'Build Total: {elapsed(session_start)}\n\nPopulate bkcr_course_rules Table')
  #     populate_start = time.time()

  # # Write the course_rules dict to the db for use in the count_transfers operation
  #     cursor.execute("""
  #     drop table if exists bkcr_course_rules;
  #     create table bkcr_course_rules (
  #     course_id int,
  #     offer_nbr int,
  #     source text,
  #     destination text,
  #     num_rules int,
  #     rules text,
  #     primary key (course_id, offer_nbr, destination)
  #     )
  #     """)
  #     with cursor.copy('copy bkcr_course_rules '
  #                      '(course_id, offer_nbr, source, destination, num_rules, rules) '
  #                      'from stdin') as copy:
  #       counter = 0
  #       total = len(course_rules)
  #       for key, value in course_rules.items():
  #         sources = set()
  #         dests = set()
  #         this_dest = key[2].upper()
  #         dests.add(this_dest)
  #         for rule in value:
  #           parts = rule.split(':')
  #           sources.add(parts[0])
  #           dests.add(parts[1])
  #         assert len(sources) == 1, f'{key} {value} {sources}'
  #         assert len(dests) == 1, f'{key} {value} {dests}'
  #         counter += 1
  #         print(f'\r  {counter:,} / {total:,}', end='')
  #         values = [key[0], key[1], sources.pop(), key[2], len(value), ' '.join(value)]
  #         copy.write_row(values)

  #     print(f'\nPopulate took {elapsed(populate_start)}')

    # Go through the transfer evaluations and count how often the courses with a bkcr-only rule
    # transferred as (a) bkcr and (b) with other destination course ids. Count, also, the distinct
    # set of students affected for each transferred course.

    # print('\nCount Transfers: Init')
    # count_start = time.time()

    # # Cache the bkcr_course_rules table built above
    # RuleInfo = namedtuple('RuleInfo', 'source num_rules rules_str')
    # bkcr_rules = dict()
    #     cursor.execute('select * from bkcr_course_rules')
    #     for row in cursor:
    #       rules_list = row.rules.split()
    #       bkcr_rules[(row.course_id,
    #                   row.offer_nbr,
    #                   row.destination)] = RuleInfo._make([row.source,
    #                                                       row.num_rules,
    #                                                       sorted(rules_list)])

    #     print(f' {len(bkcr_rules):10,} BKCR rules.\t\t\t{elapsed(count_start)}')

    #     # List of rule_keys for bkcr-only rules
    #     cursor.execute("""
    #     select r.rule_key
    #     from transfer_rules r, destination_courses d
    #     where d.rule_id = r.id
    #       and true = ALL(select c.attributes ~* 'bkcr'
    #                        from cuny_courses c
    #                       where c.course_id = d.course_id
    #                         and c.offer_nbr = d.offer_nbr)
    #     """)
    #     bkcr_only_rule_keys = [row.rule_key for row in cursor]
    #     print(f' {len(bkcr_only_rule_keys):10,} BKCR-only rule keys.\t{elapsed(count_start)}')

      count_start = time.time()
      # Cache all decriptions
      rule_descriptions = defaultdict(rule_descriptions_factory)
      cursor.execute("""
      select rule_key, description
      from rule_descriptions
      """)
      for row in cursor:
        rule_descriptions[row.rule_key] = row.description
      print(f'  {len(rule_descriptions):10,} Rule Descriptions\t{elapsed(count_start)}')

      # Cache metadata for all cuny courses, and credits for real courses. Note that this info is
      # used only for receiving courses.
      meta_start = time.time()
      Metadata = namedtuple('Metadata', 'discipline catalog_number '
                                        'is_ugrad is_active is_mesg is_bkcr is_unknown')

      def _meta_str_(self):
        """ Flags to indicate if a course (probably) does not apply to an undergraduate program
            requirement. Not undergraduate, Not active, mesg, and/or bkcr
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
        if self.is_unknown:
          return_str += '?'
        return return_str
      setattr(Metadata, 'flags', _meta_str_)

      def _course_str(self):
        return f'{self.discipline.strip()} {self.catalog_number.strip()}'
      setattr(Metadata, 'course_str', _course_str)

      metadata = dict()  # Index by (course_id, offer_nbr)
      real_credit_courses = set()  # Members are (course_id, offer_nbr)

      cursor.execute("""
      select course_id, offer_nbr, discipline, catalog_number,
             career ~* '^U' as is_ugrad,
             course_status = 'A' as is_active,
             designation in ('MNL', 'MLA') as is_mesg,
             attributes ~* 'bkcr' as is_bkcr
      from cuny_courses
      """)
      for row in cursor:
        metadata[(row.course_id, row.offer_nbr)] = Metadata._make([row.discipline,
                                                                   row.catalog_number,
                                                                   row.is_ugrad,
                                                                   row.is_active,
                                                                   row.is_mesg,
                                                                   row.is_bkcr,
                                                                   True])
        if not (row.is_mesg or row.is_bkcr):
          real_credit_courses.add((row.course_id, row.offer_nbr))

      print(f'  {len(metadata):10,} All courses\t{elapsed(count_start)}')
      print(f'  {len(real_credit_courses):10,} Real-credit courses')
    exit()
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

    print(f'\nTransfers {latest_query.name[0:-4].strip("-0123456789")} '
          f'{time.strftime("%Y-%m-%d", time.localtime(latest_timestamp))}')

    lookup_start = time.time()
    print(f'{len(open(latest_query, errors="replace").readlines()):,}')

    num_transfers = defaultdict(int)
    units_taken = defaultdict(float)
    bkcr_credits_awarded = defaultdict(float)
    real_credits_awarded = defaultdict(float)
    student_sets = defaultdict(set)
    xfr_info = defaultdict(xfr_stats_factory)
    no_problem = defaultdict(int)

    zero_units_taken = 0

    with open(latest_query, newline='', errors='replace') as query_file:
      reader = csv.reader(query_file)
      for line in reader:
        print(f'\r{reader.line_num:,}', end='')
        if reader.line_num == 1:
          Row = namedtuple('Row', [c.lower().replace(' ', '_') for c in line])
        else:
          row = Row._make(line)
          # Ignore how non-credit courses transfer. They are presumably used for things like
          # Pathways exemptions, and not relevant for our analysis of which credit-bearing courses
          # fail to transfer as real courses.
          src_units_taken = float(row.units_taken)
          if src_units_taken == 0.0:
            zero_units_taken += 1
            continue

          src_course_id = int(row.src_course_id)
          src_offer_nbr = int(row.src_offer_nbr)
          dst_institution = row.dst_institution
          src_key = (src_course_id, src_offer_nbr, dst_institution)
          if src_key not in src_courses.keys():
            # Not a course of interest: no blanket credit rules for this course
            no_problem[dst_institution] += 1
            continue

          dst_key = (int(row.dst_course_id), int(row.dst_offer_nbr))  # index metadata
          row_key = src_key                                           # index xfr_info

          # For each source course, count the number of times it was transferred, how many
          # different students were involved (in case of re-evaluations), and each of the number
          # of units taken.

          # Log cases where the subject and catalog number don't match current cuny_courses info.
          src_course_str = f'{row.src_subject.strip()} {row.src_catalog_nbr.strip()}'
          if src_course_str != src_courses[src_key]['course_str']:
            print(f'Catalog course str ({src_courses[src_key]["course_str"]}) NE src course str '
                  f'({src_course_str}))', file=log_file)

          student_sets[row_key].add(row.student_id)
          num_transfers[row_key] += 1
          units_taken[row_key] += src_units_taken

          # Transfer outcomes: what destination course was assigned, and what was its nature?
          if dst_key in real_credit_courses:
            dst_real_credits = float(row.units_transferred)
          else:
            dst_real_credits = 0.0
          real_credits_awarded[row_key] += dst_real_credits

          dst_discipline = row.dst_subject.strip()
          dst_catalog_nbr = row.dst_catalog_nbr.strip()
          dst_course_str = f'{dst_discipline} {dst_catalog_nbr}'
          try:
            dst_meta = metadata[dst_key]
          except KeyError:
            # Gotta fake the metadata
            # discipline catalog_number is_ugrad is_active is_mesg is_bkcr, is_unknown
            dst_meta = Metadata._make([dst_discipline, dst_catalog_nbr,
                                      False, False, False, False, True])
          if dst_meta.course_str() != dst_course_str:
            print(f'Catalog course str ({dst_meta.course_str()}) NE dst course str '
                  f'({dst_course_str}))', file=log_file)

          # The next line is a mystery to me: it shouldn't be necessary explicitly to create the
          # dst_course recordclass separately from updating it. (REPL works without it.)
          # xfr_info[row_key][dst_key]

          xfr_info[row_key][dst_key].count += 1
          xfr_info[row_key][dst_key].flags = dst_meta.flags
          # xfr_info[row_key][dst_course_str].rules_str = rule_keys

    print(f'\n{zero_units_taken:9,} zero units taken'
          f'\n{no_problem:9,} no-bkcr xfers ignored')
    print(f'  Lookup took', elapsed(lookup_start))

    print('Count Transfers: Generate Report')
    # =============================================================================================
    """ The following dicts are all indexed by (src_course_id, src_offer_nbr, dest_institution):
          num_transfers         Count number of transfer evaluations (includes re-evals)
          student_sets          Count distinct students
          units_taken           Sum of credits earned for sending course
          real_credits_awarded  Sum of credits granted for non-bkcr coursss
          bkcr_credits_awarded  Sum of credits granted for bkcr courses
          xfer_info: Subdict of DstCourse indexed by (dst_course_id, dst_offer_nbr)
            .count        Count number of times dst course was awarded
            .course_str   “Discipline Catalog-Number”
            .flags        Not Undergrad (G), Not Active (I), Message (M), Bkcr (B), Unknown (?)


    """
    report_start = time.time()

    # Create separate dict for each college
    institution_dicts = defaultdict(dd_factory)
    for (src_institution, src_course_str, dst_institution) in sorted(num_transfers.keys(),
                                                                     key=lambda k: k[2]):
      key = (src_institution, src_course_str, dst_institution)
      institution_dicts[dst_institution][key] = (num_transfers[key], len(student_sets[key]))

    # Sort each institution dict by number of evaluations
    for dst_institution, institution_dict in institution_dicts.items():
      institution_dict[dst_institution] = sorted(institution_dict, reverse=True)
    print(institution_dict.keys())
    exit()

    # Write the institution dicts to xlsx
    centered = Alignment('center')
    bold = Font(bold=True)
    wb = Workbook()

    headings = ['Sending College', 'Sending Course', 'Number of Students',
                'Re-evaluations', 'Percent Real', 'Receiving Courses', 'Rule Descriptions']
    for dst_institution, values in institution_dicts.items():
      ws = wb.create_sheet(dst_institution[0:3])
      row = 1
      for col in range(len(headings)):
        ws.cell(row, col + 1, headings[col]).font = bold
        ws.cell(row, col + 1, headings[col]).alignment = Alignment(horizontal='center',
                                                                   vertical='top',
                                                                   wrapText=True)
      row_counter = 0
      print(f'\n{dst_institution[0:3]} {len(values):,}')
      for row_key, (num_students, num_evaluations) in sorted(values.items(), key=lambda kv: kv[1],
                                                             reverse=True):
        print(num_students, num_evaluations)
        row_counter += 1
        row_key = (src_institution, src_course_str, dst_institution)
        print(f'\r    {row_counter:,}', end='')

        receivers = ', '.join([f'{c} [{v.count:,}]{v.flags}'
                               for c, v in xfr_info[row_key].items()])
        receivers = receivers.replace(', ', '\n')
        # Create list of all rules that _might_ have been involved in transferring this course
        all_rules = [v.rules_str for v in xfr_info[row_key].values()]
        rules_set = set()
        for sublist in all_rules:
          for rule_str in sublist:
            rules_set.add(rule_str)
        # k[0]          Sending College
        # k[1]          Sending Course String
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

        # A row is problematic if the (average) real credits awarded is less than the number of
        # credits earned
        if len(rules_set) == 1 and rule in rules_set:
            is_problematic = True
        percent_real = 'hello'
        row_values = [row_key[0][0:3], k[1], num_students, num_evaluations - num_students,
                      percent_real, receivers, '\n'.join(descriptions)]

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
