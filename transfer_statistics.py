#! /usr/local/bin/python3
""" Count how often students transfer courses to a college and how many of their credits are
    transferred as blanket credits (bkcr).

    This code is an alternative to count_transfers.py, which simply ranks all courses transferred by
    their frequency of transfer. Here, courses are still ranked by frequency, but also by the
    percentage of credits that transfer as “real” course credits—those credits that have the
    potential of counting towards a program's requirements rather than free-electives. We don't
    count whether real courses actually do count towards program requirements ... yet. And we don't
    look at how GenEd requirements factor into the transfer process ... yet.

    This code gathers information about sending-side courses (“src_course”) from our database of
    CUNY courses and transfer rules. It then goes through the spreadsheet from a CUNYfirst query
    that reports details about all course transfers across CUNY colleges to gather information about
    how each src_course transferred to each receiving college (dst_institution).

    Everything is ogranized by dst_institutiion because the resulting workbook is organized as one
    spreadsheet per receiving college.

    Dictionaries
      src_courses
        Keys    [dst_institution][(src_course_id, src_offer_nbr)]
        Values  SrcCourse(src_institution, course_str, transfer_rules)
      rule_descriptions
        Key     rule_key
        Value   Natural language text
      metadata
        Key     (course_id, offer_nbr)
        Values  Metadata(course_str, is_undergraduate, is_active, is_mesg, is_bkcr, is_unknown)
                .flags_str is textual representation of the five booleans

"""


import argparse
import csv
import os
import psycopg
import subprocess
import sys
import time

from adjustcolwidths import adjust_widths

from collections import Counter, defaultdict, namedtuple
from datetime import datetime
from openpyxl import Workbook, worksheet
from openpyxl.styles import Alignment, Font
from pathlib import Path
from psycopg.rows import namedtuple_row
from recordclass import recordclass

DEBUG = os.getenv('DEBUG_TRANSFER_STATISTICS')


# elapsed()
# -------------------------------------------------------------------------------------------------
def elapsed(since: float):
  """ Show the hours, minutes, and seconds that have elapsed since since seconds ago.
  """
  h, ms = divmod(int(time.time() - since), 3600)
  m, s = divmod(ms, 60)
  return f'{h:02}:{m:02}:{s:02}'


if __name__ == '__main__':

  session_start = time.time()
  log_file = open('transfer_statistics.log', 'w')

  # Initialize From Curriculum Database
  # ===============================================================================================

  # A SrcCourse is one that has one or more xfer rules that awards bkcr
  SrcCourse = namedtuple('SrcCourse', 'src_institution, course_str, rules')
  src_courses = defaultdict(dict)  # Index by [dst_institution][src_course_id, src_offer_nbr]

  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor(row_factory=namedtuple_row) as cursor:

      print('Rules')

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
        src_key = (row.course_id, row.offer_nbr)
        dest = row.destination_institution
        src_courses[dest][src_key] = SrcCourse._make([row.source_institution,
                                                      course_str,
                                                      row.rules.split()])
      print(f'  {cursor.rowcount:10,} Sending Courses\t{elapsed(session_start)}')

      # Cache all rule decriptions, previously stored in the cuny_curriculum db.
      rule_descriptions = defaultdict(str)
      cursor.execute("""
      select rule_key, description
      from rule_descriptions
      """)
      for row in cursor:
        rule_descriptions[row.rule_key] = row.description
      print(f'  {len(rule_descriptions):10,} Rule Descriptions\t{elapsed(session_start)}')

      # Cache metadata for all cuny courses, and credits for real courses. Note: this info is
      # used only for receiving courses.
      meta_start = time.time()
      Metadata = namedtuple('Metadata', 'course_str '
                                        'is_ugrad is_active is_mesg is_bkcr is_unknown')

      def _flags_str(self):
        """ String giving status of “interesting” settings of the Metadata boolean values.
            Undergraduate-active-real courses will return the empty string.
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
      setattr(Metadata, 'flags', _flags_str)

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
        course_str = f'{row.discipline.strip()} {row.catalog_number.strip()}'
        metadata[(row.course_id, row.offer_nbr)] = Metadata._make([course_str,
                                                                   row.is_ugrad,
                                                                   row.is_active,
                                                                   row.is_mesg,
                                                                   row.is_bkcr,
                                                                   True])
        if not (row.is_mesg or row.is_bkcr):
          real_credit_courses.add((row.course_id, row.offer_nbr))

      print(f'  {len(real_credit_courses):10,} Real-credit courses')
      print(f'  {len(metadata):10,} All courses\t{elapsed(session_start)}')

  # Process latest transfer evaluations query file.
  # =============================================================================================
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

  print(f'\nTransfer Statistics {latest_query.name[0:-4].strip("-0123456789")} '
        f'{time.strftime("%Y-%m-%d", time.localtime(latest_timestamp))}')
  print(f'{len(open(latest_query, errors="replace").readlines()):,} Transfers')
  lookup_start = time.time()

  # XferCounts
  # ----------
  """ Indexed by [dst_institution]
      How many records; how many ignored
  """
  XferCounts = recordclass('XferCounts', 'total not_bkcr')

  def xfer_counts_factory():
    return XferCounts._make([0, 0])

  xfer_counts = defaultdict(xfer_counts_factory)
  zero_units_taken = 0  # This is a src_institution value

  # XferStats
  # ---------
  """ Indexed by [dst_institution][src_course]
  """
  DstCourse = recordclass('DstCourse', 'count flags')
  XferStats = recordclass('XferStats', 'num_evaluations students_set '
                          'units_taken real_credits bkcr_credits courses, rules')

  def dst_course_factory():
    return DstCourse._make([0, ''])

  def xfer_stats_maker():
    return XferStats._make((0, set(), 0.0, 0.0, 0.0, defaultdict(dst_course_factory), ''))

  def xfer_stats_factory():
    return defaultdict(xfer_stats_maker)

  xfer_stats = defaultdict(xfer_stats_factory)

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

        src_course = (int(row.src_course_id), int(row.src_offer_nbr))
        dst_institution = row.dst_institution
        dst_course = (int(row.dst_course_id), int(row.dst_offer_nbr))

        xfer_counts[dst_institution].total += 1
        if src_course not in src_courses[dst_institution].keys():
          # Not a course of interest: no blanket credit rules for this course (although I guess
          # bkcr could be awarded anyway)
          xfer_counts[dst_institution].not_bkcr += 1
          continue
        dst_rule_descriptions = '\n'.join([rule_descriptions[rule_key]
                                           for rule_key
                                           in src_courses[dst_institution][src_course].rules])

        # Log cases where the subject and catalog number don't match current cuny_courses info.
        src_course_str = f'{row.src_subject.strip()} {row.src_catalog_nbr.strip()}'
        if src_course_str != src_courses[dst_institution][src_course].course_str:
          print(f'Catalog course str ({src_courses[dst_institution][src_course].course_str}) '
                f'NE src course str ({src_course_str}))', file=log_file)

        # For each source course, count the number of times it was transferred, how many different
        # students were involved (in case of re-evaluations), the total number of units taken.
        xfer_stats[dst_institution][src_course].num_evaluations += 1
        xfer_stats[dst_institution][src_course].students_set.add(row.student_id)
        xfer_stats[dst_institution][src_course].units_taken += src_units_taken

        # Transfer outcomes: what destination course was assigned, and what was its nature?
        dst_course_id = int(row.dst_course_id)
        dst_offer_nbr = int(row.dst_offer_nbr)
        dst_course = (dst_course_id, dst_offer_nbr)
        dst_discipline = row.dst_subject.strip()
        dst_catalog_nbr = row.dst_catalog_nbr.strip()
        dst_course_str = f'{dst_discipline} {dst_catalog_nbr}'
        try:
          dst_meta = metadata[dst_course]
        except KeyError:
          # Gotta fake the metadata
          # discipline catalog_number is_ugrad is_active is_mesg is_bkcr, is_unknown
          dst_meta = Metadata._make([dst_course_str, False, False, False, False, True])
        if dst_meta.course_str != dst_course_str:
          print(f'Catalog course str ({dst_meta.course_str}) NE dst course str ({dst_course_str}))',
                file=log_file)

        units_transferred = float(row.units_transferred)
        if dst_course in real_credit_courses:
          xfer_stats[dst_institution][src_key].real_credits += units_transferred
        else:
          xfer_stats[dst_institution][src_key].bkcr_credits += units_transferred

        xfer_stats[dst_institution][src_key].courses[dst_course_str].count += 1
        xfer_stats[dst_institution][src_key].courses[dst_course_str].flags = dst_meta.flags()
        xfer_stats[dst_institution][src_key].rules = dst_rule_descriptions
        breakpoint()
  print('\r', 80 * ' ', f'\r{zero_units_taken:9,} zero units-taken xfers ignored')
  print(f'Transfer Statistics took {elapsed(lookup_start)}')
  print('\nPer Cent Transfer as Real Courses')
  for institution in sorted(xfer_counts.keys()):
    total = xfer_counts[institution].total
    not_bkcr = xfer_counts[institution].not_bkcr
    percent = 100.0 * not_bkcr / total
    print(f'    {institution[0:3]} {not_bkcr:7,} / {total:<7,} = {percent:5.1f}%')

  print('\nCount Transfers: Generate Report')
  # =============================================================================================
  report_start = time.time()

  for dst_institution in sorted(xfer_counts.keys()):
    # Sort institution’s src_course counts
    print(dst_institution[0:3])
    institution_dict = {key: xfer_stats[dst_institution][key] for key in xfer_stats[dst_institution]}
    for key, value in institution_dict.items():
      print(key, value)
      exit('chk ur logs')

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
                             for c, v in xfer_stats[row_key].items()])
      receivers = receivers.replace(', ', '\n')
      # Create list of all rules that _might_ have been involved in transferring this course
      all_rules = [v.rules_str for v in xfer_stats[row_key].values()]
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
