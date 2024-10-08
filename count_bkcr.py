#! /usr/local/bin/python3
""" Query the transfer rules to count number of rules where the receiving side is all BKCR.
"""

from collections import Counter

import psycopg
from psycopg.rows import namedtuple_row

colleges = ['BAR', 'BCC', 'BKL', 'BMC', 'CSI', 'CTY', 'HOS', 'HTR', 'JJC', 'KCC',
            'LAG', 'LEH', 'MEC', 'NCC', 'NYT', 'QCC', 'QNS', 'SLU', 'SPS', 'YRK']
ignore = ['GRD', 'LAW', 'SPH']

all_bkcr = {}
totals = {}

# Initialize counters
for src in colleges:
  all_bkcr[src] = Counter(colleges)
  totals[src] = Counter(colleges)
  for dst in colleges:
    all_bkcr[src][dst] = 0
    totals[src][dst] = 0

if __name__ == '__main__':
  with psycopg.connect('dbname=cuny_curriculum') as conn:
    count_cursor = conn.cursor(row_factory=namedtuple_row)
    with conn.cursor(row_factory=namedtuple_row) as rule_cursor:
      rule_cursor.execute("""
      select r.id, r.source_institution, r.destination_institution,
             string_agg(d.is_mesg::text, ' ') as mesg,
             string_agg(d.is_bkcr::text, ' ') as bkcr
        from transfer_rules r, destination_courses d
       where d.rule_id = r.id
       group by r.id, r.source_institution, r.destination_institution
      """)
      num_rules = rule_cursor.rowcount
      for rule in rule_cursor.fetchall():
        rule_id = int(rule.id)
        src, dst = rule.source_institution[0:3], rule.destination_institution[0:3]
        if src in ignore or dst in ignore:
          continue
        totals[src][dst] += 1
        if 'false' not in rule.bkcr:
          all_bkcr[src][dst] += 1

      print('    SRC\\DST', ''.join([f'{c:>7}' for c in colleges]))
      for src in sorted(totals.keys()):
        # print('       ', ''.join([f'{k:>7}' for k in sorted(totals[src].keys())]))
        print(f'{src:>11} ', end='')
        for dst in sorted(totals[src].keys()):
          print(f'{totals[src][dst]:>7}', end='')
        print('\n # all_bkcr ', end='')
        for dst in sorted(all_bkcr[src].keys()):
          print(f'{all_bkcr[src][dst]:>7}', end='')
        print('\n % all_bkcr ', end='')
        for dst in sorted(all_bkcr[src].keys()):
          try:
            print(f'{100 * (all_bkcr[src][dst] / totals[src][dst]):>7.1f}', end='')
          except ZeroDivisionError:
            print('     --', end='')
        print()
      exit()

