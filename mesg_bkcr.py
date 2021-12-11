#! /usr/local/bin/python3
""" What is the relationship between BKCR, RD, min_credits, max_credits, and credit_source?
    See mesg_bkcr.out for results. Basically: nothing useful!
"""

import psycopg
from psycopg.rows import namedtuple_row

if __name__ == '__main__':
  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor(row_factory=namedtuple_row) as cursor:
      cursor.execute("""
      select count(*),
             c.designation,
             c.min_credits < 0.001 as min_is_zero,
             c.max_credits < 0.001 as max_is_zero,
             d.credit_source

        from destination_courses d, cuny_courses c
        where c.course_id = d.rule_id
          and c.attributes ~* 'BKCR'
      group by min_is_zero, max_is_zero, rollup(credit_source, c.designation)
      order by credit_source, c.designation, min_is_zero, max_is_zero
      """)
      for row in cursor.fetchall():
        try:
          min_is_zero = 'FT'[row.min_is_zero]
        except TypeError:
          min_is_zero = ' '
        try:
          max_is_zero = 'FT'[row.max_is_zero]
        except TypeError:
          max_is_zero = ' '
        try:
          row_count = f'{row.count:5,}'
        except TypeError:
          row_count = '     '
        try:
          if row.credit_source is None:
            credit_source = ' '
          else:
            credit_source = row.credit_source
        except TypeError:
          credit_source = ' '
        try:
          designation = f'{row.designation:<4}'
        except TypeError:
          designation = '    '
        print(f'{row_count} {credit_source} {designation:<4} '
              f'{min_is_zero} {max_is_zero}')
