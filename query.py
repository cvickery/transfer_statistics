#! /usr/local/bin/python3
""" Format a transfer rule into a text string. Along the way, build a dict of the parts for possible
    use in building a table row with the same info:

      description         := sending_side 'at {college} transfers to {college} as' receiving_side
      sending_side        := requirement ['and {requirement}']*
      requirement         := grade course
      grade               := 'Passing grade'
                          | '{letter} or better'
                          | 'Less than {letter}'
                          | 'Between {letter} and {letter}'
      course              := discipline catalog_number (credits | flag*) alias_string
      credits             := '({float} cr)'
      alias_string        := '[Alias(es): ' alias_list
      alias_list          := alias_course [', and' alias_course]*
      alias_course        := '{discipline} {catalog_number}'
      receiving_side      := course+
      flag                := 'B' | 'I' | 'M'

      Within strings, {college} is the name of a college {letter} is a letter grade and {float} is a
      floating point number with 1 decimal place, the sum of credits for the sending courses.
      The B, I, and M flags are for blanket, inactive, and message course.
"""
import os
import psycopg
import sys
import time

from psycopg.rows import namedtuple_row


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
    return 'Passsing grade'

  if min_gpa >= 0.7 and max_gpa >= 3.7:
    letter = letters[int(round(min_gpa * 3))]
    return f'{letter} or above'

  if min_gpa > 0.7 and max_gpa < 3.7:
    return f'Between {letters[int(round(min_gpa * 3))]} and {letters[int(round(max_gpa * 3))]}'

  if max_gpa < 3.7:
    letter = letters[int(round(max_gpa * 3))]
    return 'Below ' + letter

  return 'Passing grade'


def elapsed(since: float):
  """ Show the hours, minutes, and seconds that have elapsed since since seconds ago.
  """
  h, ms = divmod(int(time.time() - since), 3600)
  m, s = divmod(ms, 60)
  return f'{h:02}:{m:02}:{s:02}'


# main()
# ------------------------------------------------------------------------------------------------
if __name__ == "__main__":
  session_start = time.time()
  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor(row_factory=namedtuple_row) as cursor:
      print('Lookup Rules')
      cursor.execute("""
      select r.*, array_agg(s.*) as src, array_agg(r.*) as dst
      from transfer_rules r, source_courses s, destination_courses d
      where s.rule_id = r.id
      and d.rule_id = r.id
      group by r.id
      """)

      print(f'{cursor.rowcount:,} Rules {elapsed(session_start)}')
      print('Format Rules')
      format_start = time.time()
      for rule in cursor:
        print(f'\r {cursor.rownumber:,}', end='')
        # Gather sending side

        # Gather receiving side

        # Put 'em together

      print(f'Formating Complete {elapsed(format_start)}')
