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
_all_to_keys = dict()
_disciplines = dict()
_discipline_to_department = defaultdict()
_department_names = dict()
_sending_courses = defaultdict(set)
_receiving_courses = defaultdict(set)

with psycopg.connect('dbname=cuny_curriculum') as conn:
  with conn.cursor(row_factory=namedtuple_row) as cursor:

    # Setup access by ID or rule_key
    cursor.execute('select id, rule_key from transfer_rules')
    _id_to_key = {row.id: row.rule_key for row in cursor.fetchall()}

    # All rule_keys where receiver is QCC or QNS
    cursor.execute("""
    select rule_key
      from transfer_rules
     where rule_key ~* ':(QCC01|QNS01):'
    """)
    _all_to_keys = sorted([row.rule_key for row in cursor.fetchall()],
                          key=lambda key: key[6:11])

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

    # Department Names
    cursor.execute("""
    select institution, department, department_name
      from cuny_departments
     where department_status = 'A'
    """)
    _department_names = {(row.institution, row.department): row.department_name for row in cursor}

    cursor.execute("""
    select *
      from cuny_disciplines
     where status = 'A'
       and department !~* '01$'
       and department !~* '^(PERMIT-|REG-|ADMIN-|PROV-|MISC-|UGRD-|ACAD)'
    """)
    _disciplines = {(row.institution, row.discipline): row for row in cursor.fetchall()}

    # Metadata for sending side and receiving side courses, indexed by rule_key
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

  # Rare, but observed case: a “real” course not offered by any department
  if real_subjects:
    if not departments:
      department = 'Admin'
      detail = f'No department for {', '.join(real_subjects)}'

    # If there is one department, the job is done, even if there are also admin courses.
    elif len(departments) == 1:
      department = departments.pop()
      detail = _department_names[dest_institution, department]

    # Rare (nonexistent?) case: multiple receiving departments
    elif len(departments) > 1:
      department = 'Admin'
      detail = f'Multiple receiving departments: {', '.join(departments)}'

  else:
    # Receiving side is only Admin
    admin_subjects = {c.course_str.split(' ')[0] for c in admin_courses}
    departments = {_discipline_to_department[dest_institution, subj] for subj in admin_subjects}

    # The subject might be for a real discipline ('BIO 499')
    if len(departments) == 1:
      department = departments.pop()
      # FIX THIS: it’s picking up admin “departments” like QCC01
      detail = _department_names[dest_institution, department]

    # Look at the sending cuny_subject to see if there is a match at the receiving side
    else:
      department = 'Admin'
      detail = 'Look at the sending course cuny_subject and/or CIP code'

  return {'rule_key': rule_key, 'department': department, 'detail': detail}


if __name__ == '__main__':

  # prompt for transfer_rules.rule_key or transfer_rules.id and show the institution and dept.
  rule_id = rule_key = None
  if len(sys.argv) > 1:
    arg = sys.argv[1]
  else:
    arg = input('Rule ID or Rule Key? ')

  if arg.lower() == 'all':
    # All rule_keys where QNS01 or QCC01 is the receiving institution
    for rule_key in _all_to_keys:
      try:
        dd = destination_department(rule_key)
        print(f'{rule_key}: {dd['department']:5} {dd['detail']}', file=sys.stderr)
      except KeyError:
        print(f'{rule_key}: KeyErr', file=sys.stderr)

  else:
    try:
      rule_id = int(arg)
      print(destination_department(rule_id))
    except ValueError:
      rule_key = arg
      print(destination_department(rule_key))
