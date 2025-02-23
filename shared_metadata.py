#! /usr/local/bin/python3

import psycopg

from collections import namedtuple
from psycopg.rows import namedtuple_row

Metadata = namedtuple('Metadata', 'institution course_str '
                                  'is_ugrad is_active is_mesg is_bkcr is_unknown')
metadata = dict()
real_credit_courses = set()  # Members are (course_id, offer_nbr)


# _flags()
# -------------------------------------------------------------------------------------------------
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


# init
# -------------------------------------------------------------------------------------------------
# Info about courses: initialized and used by transfer_statisics; used by transfer_by_subjects

setattr(Metadata, 'flags', _flags_str)

with psycopg.connect('dbname=cuny_curriculum') as conn:
  with conn.cursor(row_factory=namedtuple_row) as cursor:

    # Cache metadata for all cuny courses, and credits for real courses. Note: this info is
    # used only for receiving courses.
    cursor.execute("""
    select course_id, offer_nbr, institution, discipline, catalog_number,
          career ~* '^U' as is_ugrad,
          course_status = 'A' as is_active,
          designation in ('MNL', 'MLA') as is_mesg,
          attributes ~* 'bkcr' as is_bkcr
    from cuny_courses
    """)
    for row in cursor:
      course_str = f'{row.discipline.strip()} {row.catalog_number.strip()}'
      metadata[(row.course_id, row.offer_nbr)] = Metadata._make([row.institution,
                                                                course_str,
                                                                row.is_ugrad,
                                                                row.is_active,
                                                                row.is_mesg,
                                                                row.is_bkcr,
                                                                False])
      if not (row.is_mesg or row.is_bkcr):
        real_credit_courses.add((row.course_id, row.offer_nbr))


# main()
# -------------------------------------------------------------------------------------------------
if __name__ == '__main__':
  print(f'  {len(real_credit_courses):10,} Real-credit courses')
  print(f'  {len(metadata):10,} All courses')
