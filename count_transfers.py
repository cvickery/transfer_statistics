#! /usr/local/bin/python3
""" Count how often students transfer courses to a college.
"""

import os
import sys

import psycopg
from psycopg.rows import namedtuple_row

if __name__ == '__main__':
  with psycopg.connect(dbname='cuny_transfers') as conn:
    with conn.cursor(row_factory=namedtuple_row) as cursor:
      # A course might appear to be transferred multiple times by the same student because of
      # re-evaluations, so that has to be accounted for.
      query = """
      select count(*), src_institution, src_course_id, src_offer_nbr, dst_institution
        from transfers_applied
        group by student_id, src_institution, src_course_id, src_offer_nbr, dst_institution
        order by dst_institution, count desc
      """
      cursor.execute(query)
      print('To,From,Course,Count')
      for row in cursor.fetchall():
        if int(row.count) > 5:
          print(f'{row.dst_institution[0:3]},{row.src_institution[0:3]},{row.src_course_id:06}:'
                f'{row.src_offer_nbr},{row.count:6,}')
