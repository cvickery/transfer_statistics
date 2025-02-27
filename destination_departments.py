#! /usr/local/bin/python3
"""Given a rule, what department should review it?

  Create a db table once here, and let the app create a dict from it at run time.

  First divide the destination course set into administrative and non-administrative course sets.
    If the non-administrative set is all one discipline
      Return the department that owns that discipline
    Else
      If the sending course set is all one discipline
        How many receiving disciplines’ cuny_subjects match the sending cuny_subject
          1
            Return the department of the matching discipline
          0
            How many receiving disciplines’ cip_code match the sending cip_code
              1
                Return the department of the matching discipline
              0
                Return "Admin: no matching department"
              Else
                Return "Admin: multiple possible departments"
      Else
        Return "Admin: multiple sending disciplines" (does this happen?)

"""
import psycopg
import sys

from collections import defaultdict
from psycopg.rows import namedtuple_row
from shared_metadata import Metadata, metadata
from typing import Any

# Module Initializtion
# =================================================================================================
_id_to_key = dict()
_discipline_to_department = defaultdict()
_sending_courses = defaultdict(set)
_receiving_courses = defaultdict(set)

with psycopg.connect('dbname=cuny_curriculum') as conn:
  with conn.cursor(row_factory=namedtuple_row) as cursor:

    # Setup access by ID or rule_key
    cursor.execute('select id, rule_key from transfer_rules')
    _id_to_key = {row.id: row.rule_key for row in cursor.fetchall()}

    # Dict of discipline to department mappings, keyed by (institution, discipline)
    cursor.execute("""
    select institution, discipline, department
      from cuny_disciplines
     where status = 'A'
    """)
    _discipline_to_department = {(row.institution, row.discipline): row.department
                                 for row in cursor}
    # # Verify that a single discipline always maps to a single department
    # # This code was used when _discipline_to_department was a defaultdict(set)
    # # (Whatever happened to shared disciplines??)
    # len_1 = 0
    # for key, value in _discipline_to_department.items():
    #   match len(value):
    #     case 1:
    #       len_1 += 1
    #     case _:
    #       print(key, value, file=sys.stderr)
    # print(f'{len(_discipline_to_department)=}', file=sys.stderr)

    # cache metadata for sending side and receiving side courses, indexed by rule_key
    cursor.execute("""
    select course_id, offer_nbr, rule_id from source_courses
    """)
    for row in cursor:
      _sending_courses[_id_to_key[row.rule_id]].add(metadata[row.course_id, row.offer_nbr])

    cursor.execute("""
    select course_id, offer_nbr, rule_id from destination_courses
    """)
    for row in cursor:
      rule_key = _id_to_key[row.rule_id]
      try:
        _receiving_courses[rule_key].add(metadata[row.course_id, row.offer_nbr])
      except KeyError:
        # Add a Metadata namedtuple for this ‘gone missing’ course
        #  Extract the institution from the rule key, and set the is_unknown flag
        gone_missing = Metadata._make([rule_key.split(':')[1], 'UNKNOWN',
                                      False, False, False, False, True])
        _receiving_courses[rule_key].add(gone_missing)


# destination_department()
# -------------------------------------------------------------------------------------------------
def destination_department(arg: Any) -> str:
  """Map the sending side course(s) to the receiving side department(s)."""
  if isinstance(arg, int):
    rule_key = _id_to_key[arg]
  else:
    rule_key = arg

  # If department is found, detail is the department name. Otherwise, department is 'Admin' and
  # detail is an explanation.
  department = detail = ''
  dest_institution = rule_key[6:11]
  receiving_courses = _receiving_courses[rule_key]
  if not receiving_courses:
    raise ValueError(f'{rule_key}: No Receiving Courses')

  admin_courses, real_courses = set(), set()
  for c in receiving_courses:
    (admin_courses if c.is_mesg or c.is_bkcr else real_courses).add(c)

  real_subjects = {c.course_str.split(' ')[0] for c in real_courses}
  departments = {_discipline_to_department[dest_institution, subj] for subj in real_subjects}
  print(f'{dest_institution=} {admin_courses=} {real_courses=} {real_subjects=} {departments=}')
  match (len(admin_courses), len(real_courses)):
    case (0, _):
      # All real
      if len(real_subjects) == 1:
        ...
    case (_, 0):
      # All admin
      department = 'Admin'
      s = ' is' if len(admin_courses) == 1 else 's are all'
      detail = f'Receiving course{s} BKCR or MESG'
    case (_, _):
      # Mixed
      ...

  return {'rule_key': rule_key, 'department': department, 'detail': detail}


if __name__ == '__main__':

  # prompt for transfer_rules.rule_key or transfer_rules.id and show the institution and dept.
  rule_id = rule_key = None
  if len(sys.argv) > 1:
    arg = sys.argv[1]
  else:
    arg = input('Rule ID or Rule Key? ')

  if arg.lower() == 'all':
    print('not yet')
  else:
    try:
      rule_id = int(arg)
      print(destination_department(rule_id))
    except ValueError:
      rule_key = arg
      print(destination_department(rule_key))
