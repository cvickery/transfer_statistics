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
_department_names = dict()
_cuny_subjects = defaultdict(set)  # departments that offer a cuny subject at an institution
_cip_codes = defaultdict(set)  # departments that offer a cip_code at an institution.
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
    _all_to_keys = sorted([row.rule_key for row in cursor], key=lambda key: key[6:11])

    # Info about non-administrative departments
    cursor.execute("""
    select institution, department, discipline, discipline_name, cip_code, cuny_subject
      from cuny_disciplines
     where status = 'A'
       and department !~* '01$'
       and department !~* '^(PERMIT-|REG-|ADMIN-|PROV-|MISC-|UGRD-|ACAD)'
    """)
    rows = cursor.fetchall()
    _disciplines = {(row.institution, row.discipline): row for row in rows}
    _cuny_subjects = {(row.institution, row.cuny_subject): row.department for row in rows}
    _cip_codes = {(row.institution, row.cip_code[0:2]): row.department for row in rows}

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
        gone_missing = Metadata._make([rule_key.split(':')[1], 'Unknown', 'Unknown',
                                      False, False, False, False, True])
        _receiving_courses[rule_key].add(gone_missing)


# oxfordize()
# -------------------------------------------------------------------------------------------------
def oxfordize(source_list: list, list_type: str) -> str:
  """Apply oxford-comma pattern to a list of strings."""
  sentence = ', '.join([' '.join(q) if isinstance(q, tuple) else q for q in source_list])
  if comma_count := sentence.count(','):
    assert list_type.lower() in ['and', 'or'], f'{sentence=} {comma_count=} {list_type=}'
    conjunction_str = f' {list_type}'
    if comma_count == 1:
      return sentence.replace(',', conjunction_str)
    else:
      last_comma = sentence.rindex(',') + 1
      return sentence[:last_comma] + conjunction_str + sentence[last_comma:]
  else:
    return sentence


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
  departments = set()
  for subj in real_subjects:
    try:
      departments.add(_disciplines[dest_institution, subj].department)
    except KeyError:
      # No active, non-administrative department for this subject
      pass

  # Rare, but observed case: “real” courses not offered by any department
  if real_subjects:
    if not departments:
      department = 'Admin'
      detail = f'No department for {oxfordize(real_subjects, 'or')}'

    # If there is one department, the job is done, even if there are also admin courses.
    elif len(departments) == 1:
      department = departments.pop()
      detail = _department_names[dest_institution, department]

    # Rare (nonexistent?) case: multiple receiving departments
    elif len(departments) > 1:
      department = 'Admin'
      detail = f'Multiple receiving departments: {oxfordize(departments, 'and')}'

  else:
    # Receiving side is only Admin, but the subject might be a real discipline ('BIOL 499')
    admin_subjects = {c.course_str.split(' ')[0] for c in admin_courses}
    try:
      departments = {_disciplines[dest_institution, subj].department
                     for subj in admin_subjects}
    except KeyError:
      # Not a real subject
      pass
    if len(departments) == 1:
      department = departments.pop()
      detail = _department_names[dest_institution, department]

    # Look at the sending cuny_subject to see if there is a match at the receiving side
    else:
      # Get set of sending cuny_subjects
      sending_subjects = set()
      for sending_course in _sending_courses[rule_key]:
        sending_subjects.add(sending_course.cuny_subject)
      # List of departments that offer those subjects (if any)
      receiving_departments = set()
      for sending_subject in sending_subjects:
        try:
          receiving_departments.add(_cuny_subjects[dest_institution, sending_subject])
        except KeyError:
          # The sending subject does not have a matching cuny_subject
          pass
      match len(receiving_departments):

        case 1:
          # Ideal case: there is one department that should handle it
          department = receiving_departments.pop()
          detail = f'Offers courses with same CUNY subject ({sending_subject})'

        case 0:
          # No match on cuny_subject; try CIP code area
          department = 'Admin'

          # Get sending department’s cip code
          sending_cip_codes = set()
          for sending_course in _sending_courses[rule_key]:
            try:
              sending_discipline = sending_course.course_str.split(' ')[0]
              sending_cip_codes.add(_disciplines[sending_course.institution,
                                                 sending_discipline].cip_code[0:2])
            except KeyError:
              pass
          # Find receiving departments with same cip code
          receiving_departments = set()
          for sending_cip_code in sending_cip_codes:
            receiving_departments.add(_cip_codes[dest_institution, sending_cip_code])
          match len(receiving_departments):
            case 0:
              detail = (f'No department at {dest_institution[0:3]} '
                        f'offers courses in CUNY subject {sending_subject} or CIP area '
                        f'{oxfordize(sending_cip_codes, 'or')}')
            case 1:
              detail = (f'No department offers courses in CUNY subject {sending_subject}, '
                        f'but {receiving_departments.pop()} offers courses in CIP area '
                        f'{oxfordize(sending_cip_codes, 'or')}')
            case _:
              detail = (f'No department offers courses in CUNY subject {sending_subject}, '
                        f'but {oxfordize(receiving_departments, 'and')} offer courses in CIP area '
                        f'{oxfordize(sending_cip_codes, 'or')}')

        case _:
          # Multiple cuny subject matches
          department = 'Admin'
          departments = oxfordize(receiving_departments, 'and')
          detail = f'{departments} offer courses in {sending_subject}'

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
        print(f'{rule_key:22} {dd['department']:12} {dd['detail']}', file=sys.stderr)
      except KeyError as err:
        print(f'{rule_key:20} {err}', file=sys.stderr)

  else:
    try:
      rule_id = int(arg)
      print(destination_department(rule_id))
    except ValueError:
      rule_key = arg
      print(destination_department(rule_key))
