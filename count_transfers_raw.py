#! /usr/local/bin/python3
""" Count how often students transfer courses to a college.
    This is an alternate to count_transfers.py. This processes the CUNYfirst query output directly,
    whereas count_transfers.py worked from the transfers_applied table of the database.
    2021-11-27: Ran the CUNYfirst query with a longer history (start term 1152) to try to get a
    better sample size.
"""

import csv
import os
import sys

from collections import defaultdict, namedtuple
from pathlib import Path


def stats_factory():
  return {'student_ids': set(),
          'src_institution': 'unknown',
          'total_transfers': 0,
          'distinct_transfers': 0}
  # return Stats._make([set(), set(), 0, 0])._asdict()


if __name__ == '__main__':

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
        latest_timestamp = this_timestanp
  print(f'Using {latest_query.name}')

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
