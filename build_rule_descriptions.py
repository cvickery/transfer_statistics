#! /usr/local/bin/python3

import argparse
import os
import psycopg
import re
import sys

from collections import namedtuple
from format_rules import format_rule_by_key
from psycopg.rows import namedtuple_row

# Named tuples for a transfer rule and its source and destination course lists.
Transfer_Rule = namedtuple('Transfer_Rule', """
                           rule_id
                           source_institution
                           destination_institution
                           subject_area
                           group_number
                           source_disciplines
                           source_subjects
                           review_status
                           source_courses
                           destination_courses
                           """)

# The information from the source and destination courses tables is augmented with a count of how
# many offer_nbr values there are for the course_id.
Source_Course = namedtuple('Source_Course', """
                           course_id
                           offer_count
                           discipline
                           catalog_number
                           discipline_name
                           cuny_subject
                           cat_num
                           min_credits
                           max_credits
                           min_gpa
                           max_gpa
                           """)

Destination_Course = namedtuple('Destination_Course', """
                                course_id
                                offer_count
                                discipline
                                catalog_number
                                discipline_name
                                cuny_subject
                                cat_num
                                transfer_credits
                                credit_source
                                is_mesg
                                is_bkcr
                                """)


# andor_list()
# -------------------------------------------------------------------------------------------------
def andor_list(items, andor='and'):
  """ Join a list of stings into a comma-separated con/disjunction.
      Forms:
        a             a
        a and b       a or b
        a, b, and c   a, b, or c
  """
  return_str = ', '.join(items)
  k = return_str.rfind(',')
  if k > 0:
    k += 1
    return_str = return_str[:k] + f' {andor}' + return_str[k:]
  if return_str.count(',') == 1:
    return_str = return_str.replace(',', '')
  return return_str


# _grade()
# -------------------------------------------------------------------------------------------------
def _grade(min_gpa, max_gpa):
  """ Convert numerical gpa range to description of required grade in letter-grade form.
      The issue is that gpa values are not represented uniformly across campuses, and the strings
      used have to be floating point values, which lead to imprecise boundaries between letter
      names.
  """

  # Convert GPA values to letter grades by table lookup.
  # int(round(3×GPA)) gives the index into the letters table.
  # Index positions 0 and 1 aren't actually used.
  """
          GPA  3×GPA  Index  Letter
          4.3   12.9     13      A+
          4.0   12.0     12      A
          3.7   11.1     11      A-
          3.3    9.9     10      B+
          3.0    9.0      9      B
          2.7    8.1      8      B-
          2.3    6.9      7      C+
          2.0    6.0      6      C
          1.7    5.1      5      C-
          1.3    3.9      4      D+
          1.0    3.0      3      D
          0.7    2.1      2      D-
    """
  letters = ['F', 'F', 'D-', 'D', 'D+', 'C-', 'C', 'C+', 'B-', 'B', 'B+', 'A-', 'A', 'A+']

  assert min_gpa <= max_gpa, f'{min_gpa=} greater than {max_gpa=}'

  # Put gpa values into “canonical form” to deal with creative values found in CUNYfirst.

  # Courses transfer only if the student passed the course, so force the min acceptable grade
  # to be a passing (D-) grade.
  if min_gpa < 1.0:
    min_gpa = 0.7
  # Lots of values greater than 4.0 have been used to mean "no upper limit."
  if max_gpa > 4.0:
    max_gpa = 4.0

  # Generate the letter grade requirement string

  if min_gpa < 1.0 and max_gpa > 3.7:
    return 'Pass'

  if min_gpa >= 0.7 and max_gpa >= 3.7:
    letter = letters[int(round(min_gpa * 3))]
    return f'{letter} or above'

  if min_gpa > 0.7 and max_gpa < 3.7:
    return f'Between {letters[int(round(min_gpa * 3))]} and {letters[int(round(max_gpa * 3))]}'

  if max_gpa < 3.7:
    letter = letters[int(round(max_gpa * 3))]
    return 'Below ' + letter

  return 'Pass'


# format_rule_by_key()
# -------------------------------------------------------------------------------------------------
def format_rule_by_key(rule_key):
  """ Generate a Transfer_Rule tuple given the key.
  """
  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor(row_factory=namedtuple_row) as cursor:

      cursor.execute("""
      select * from transfer_rules
       where source_institution = %s
         and destination_institution = %s
         and subject_area = %s
         and group_number = %s
      """, rule_key.split(':'))

      rule = cursor.fetchone()
      """
          Source_Course
            course_id
            offer_count
            discipline
            catalog_number
            discipline_name
            cuny_subject
            cat_num
            min_credits
            max_credits
            min_gpa
            max_gpa

          Destination_Course
            course_id
            offer_count
            discipline
            catalog_number
            discipline_name
            cuny_subject
            cat_num
            transfer_credits
            credit_source
            is_mesg
            is_bkcr
      """
      cursor.execute("""
        select  sc.course_id,
                sc.offer_count,
                sc.discipline,
                sc.catalog_number,
                dn.discipline_name,
                sc.cuny_subject,
                sc.cat_num,
                sc.min_credits,
                sc.max_credits,
                sc.min_gpa,
                sc.max_gpa
        from source_courses sc, cuny_disciplines dn
        where sc.rule_id = %s
          and dn.institution = %s
          and dn.discipline = sc.discipline
        order by discipline, cat_num
        """, (rule.id, rule.source_institution))
      source_courses = [Source_Course._make(c) for c in cursor.fetchall()]

      cursor.execute("""
        select  dc.course_id,
                dc.offer_count,
                dc.discipline,
                dc.catalog_number,
                dn.discipline_name,
                dc.cuny_subject,
                dc.cat_num,
                dc.transfer_credits,
                dc.credit_source,
                dc.is_mesg,
                dc.is_bkcr
         from destination_courses dc, cuny_disciplines dn
        where dc.rule_id = %s
          and dn.institution = %s
          and dn.discipline = dc.discipline
        order by discipline, cat_num
        """, (rule.id, rule.destination_institution))
      # 'offer_count', 'discipline', 'catalog_number', 'discipline_name', 'cuny_subject', 'cat_num', 'transfer_credits', 'credit_source', 'is_mesg', and 'is_bkcr'
      destination_courses = [Destination_Course._make(c) for c in cursor.fetchall()]

      the_rule = Transfer_Rule._make(
          [rule.id,
           rule.source_institution,
           rule.destination_institution,
           rule.subject_area,
           rule.group_number,
           rule.source_disciplines,
           rule.source_subjects,
           rule.review_status,
           source_courses,
           destination_courses])

      conn.close()
      return format_rule(the_rule, rule_key)


# format_rule()
# -------------------------------------------------------------------------------------------------
def format_rule(rule, rule_key):
  """ Return a plain-text description of a rule. Unlike transfer-app, ignores cross-listed courses.
  """

  # Extract disciplines and courses from the rule
  source_disciplines = rule.source_disciplines.strip(':').split(':')
  source_courses = rule.source_courses
  destination_courses = rule.destination_courses

  # Check validity of Source and Destination course_ids
  source_course_ids = [course.course_id for course in rule.source_courses]
  # There should be no duplicates in source_course_ids for the rule
  assert len(set(source_course_ids)) == len(source_course_ids), \
      f'Duplcated source course id(s) for rule {rule_key}'
  source_course_id_str = ':'.join([f'{id}' for id in source_course_ids])

  destination_course_ids = [course.course_id for course in rule.destination_courses]
  # There should be no duplicates in destination_course_ids for the rule
  assert len(set(destination_course_ids)) == len(destination_course_ids), \
      f'Duplcated destination course id(s) for rule {rule_key}'
  destination_course_id_str = ':'.join([f'{id}' for id in destination_course_ids])

  source_class = ''  # for the HTML credit-mismatch indicator

  min_source_credits = 0.0
  max_source_credits = 0.0
  source_course_list = ''

  # Assumptions and Presumptions:
  # - All source courses do not necessarily have the same discipline.
  # - Grade requirement can chage as the list of courses is traversed.
  # - If course has cross-listings, list cross-listed course(s) in parens following the
  #   catalog number. AND-list within a list of courses having the same grade requirement. OR-list
  #   for cross-listed courses.
  # Examples:
  #   Passing grades in LCD 101 (=ANTH 101 or CMLIT 207) and LCD 102.
  #   Passing grades in LCD 101 (=ANTH 101) and LCD 102. C- or better in LCD 103.

  # First, group courses by grade requirement. Not sure there will ever be a mix for one rule, but
  # if it ever happens, we’ll be ready.
  courses_by_grade = dict()
  for course in source_courses:
    # Accumulate min/max credits for checking against destination credits
    min_source_credits += float(course.min_credits)
    max_source_credits += float(course.max_credits)
    if (course.min_gpa, course.max_gpa) not in courses_by_grade.keys():
      courses_by_grade[(course.min_gpa, course.max_gpa)] = []
    courses_by_grade[(course.min_gpa, course.max_gpa)].append(course)

  # For each grade requirement, sort by cat_num, and generate array of strings to AND-list together
  by_grade_keys = [key for key in courses_by_grade.keys()]
  by_grade_keys.sort()

  for key in by_grade_keys:
    grade_str = _grade(key[0], key[1])
    if grade_str != 'Pass':
      grade_str += ' in '
    courses = courses_by_grade[key]
    courses.sort(key=lambda c: c.cat_num)
    course_list = []
    for course in courses:
      course_list.append(f'{course.discipline} {course.catalog_number}')
    source_course_list += f'{grade_str} {andor_list(course_list, "and")}'

  # Build the destination part of the rule group
  #   If any of the destination courses has the BKCR attribute, the credits for that course will be
  #   whatever is needed to make the credits match the sum of the sending course credits.
  destination_credits = 0.0
  has_bkcr = False
  discipline = ''
  destination_course_list = ''
  for course in destination_courses:
    if course.is_bkcr:
      has_bkcr = True   # Number of credits will be computed to match source credits
    else:
      destination_credits += float(course.transfer_credits)
      cat_num_class = ''
    course_catalog_number = course.catalog_number
    if discipline != course.discipline:
      if destination_course_list != '':
        destination_course_list = destination_course_list.strip('/') + '; '
      destination_course_list = destination_course_list.strip('/ ') + course.discipline + '-'

    destination_course_list += course_catalog_number

  destination_course_list = destination_course_list.strip('/').replace(';', ' and ')

  # Credits match if there is BKCR, otherwise, check if in range.
  if destination_credits < min_source_credits and has_bkcr:
    destination_credits = min_source_credits

  if min_source_credits != max_source_credits:
    source_credits_str = f'{min_source_credits}-{max_source_credits}'
  else:
    source_credits_str = f'{min_source_credits}'

  description = (f'{source_course_list} at {institution_names[rule.source_institution]} '
                 f'({source_credits_str} cr) transfers to '
                 f'{institution_names[rule.destination_institution]} as {destination_course_list} '
                 f'({destination_credits} cr)')
  description = description.replace('Pass', 'Passing grade in')

  return description


if __name__ == '__main__':
  parser = argparse.ArgumentParser('Test build rule descriptions transfer rules')
  parser.add_argument('-b', '--build', action='store_true')
  parser.add_argument('-r', '--rule')
  args = parser.parse_args()

  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor(row_factory=namedtuple_row) as cursor:

      # Cache college names
      cursor.execute("select code, prompt from cuny_institutions order by lower(name)")
      institution_names = {row.code: row.prompt for row in cursor}

      if args.build:
        cursor.execute("""
        drop table if exists rule_descriptions;
        create table rule_descriptions (
        rule_key text primary key,
        description text)
        """)
        conn.commit()
        cursor.execute('select rule_key from transfer_rules')
        print(f'{cursor.rowcount:,}')
        with conn.cursor(row_factory=namedtuple_row) as insert_cursor:
          for row in cursor:
            print(f'\r{cursor.rownumber:,}', end='')
            insert_cursor.execute("""
            insert into rule_descriptions values (%s, %s)
            """, (row.rule_key, format_rule_by_key(row.rule_key)))

  if args.rule:
    description = format_rule_by_key(args.rule)
    print(f"{args.rule}: {description}")
