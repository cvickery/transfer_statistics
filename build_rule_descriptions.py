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
import psycopg
import time

from collections import namedtuple, defaultdict
from datetime import datetime
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
    return 'any passing grade'

  if min_gpa >= 0.7 and max_gpa >= 3.7:
    letter = letters[int(round(min_gpa * 3))]
    return f'{letter} or above'

  if min_gpa > 0.7 and max_gpa < 3.7:
    return f'between {letters[int(round(min_gpa * 3))]} and {letters[int(round(max_gpa * 3))]}'

  if max_gpa < 3.7:
    letter = letters[int(round(max_gpa * 3))]
    return 'below ' + letter

  return 'any passing grade'


def elapsed(since: float):
  """ Show the hours, minutes, and seconds that have elapsed since since seconds ago.
  """
  h, ms = divmod(int(time.time() - since), 3600)
  m, s = divmod(ms, 60)
  return f'{h:02}:{m:02}:{s:02}'


# and_list()
# -------------------------------------------------------------------------------------------------
def and_list(items):
  """ Create a comma-separated list of strings.
  """
  assert isinstance(items, list)
  match len(items):
    case 0:
      return ''
    case 1:
      return items[0]
    case 2:
      return f'{items[0]} and {items[1]}'
    case _:
      return ', '.join(items[0:-1]) + f', and {items[-1]}'


# main()
# -------------------------------------------------------------------------------------------------
if __name__ == "__main__":
  Alias = namedtuple('Alias', """ course_id
                                  offer_nbr
                                  institution
                                  discipline
                                  catalog_number
                                  cat_num cuny_subject
                                  min_credits
                                  max_credits
                                  course_status
                                  is_mesg
                                  is_bkcr
                              """)
  session_start = time.time()
  with psycopg.connect('dbname=cuny_curriculum') as conn:
    with conn.cursor(row_factory=namedtuple_row) as cursor:

      # Create namedtuples for the columns in the source and destination course lists
      cursor.execute("""
      select column_name
      from information_schema.columns
      where table_name = 'source_courses'
      """)

      cursor.execute("""
      select column_name
      from information_schema.columns
      where table_name = 'destination_courses'
      """)

      print('Lookup Sending Side')
      lookup_start = time.time()
      rules = defaultdict(str)
      cursor.execute("""
      select r.rule_key, json_agg(s.*) as src
      from transfer_rules r, source_courses s
      where s.rule_id = r.id
      group by rule_key
      """)

      print(f'{cursor.rowcount:,} Rules {elapsed(lookup_start)}')
      print('Format Sending Side')
      format_start = time.time()
      for rule in cursor:

        print(f'\r {cursor.rownumber:,}', end='')

        # Sending side
        sources = rule.src
        source_list = []
        sending_credits = 0.0
        for source in sorted(sources, key=lambda val: val['cat_num']):
          sending_credits += source['max_credits']
          alias_list = []
          for alias in source['aliases']:
            # Create namedtuple so we can access the needed fields by name
            alias_values = Alias._make(alias)
            alias_list.append(f'{alias_values.discipline} {alias_values.catalog_number}')
          source['aliases'] = alias_list
          grade_str = _grade(source['min_gpa'], source['max_gpa'])
          course_str = (f'{source["discipline"]} {source["catalog_number"]} '
                        f'({source["max_credits"]:0.1f} cr)')
          if len(alias_list) > 0:
            alias_str = and_list(alias_list)
            suffix = '' if len(alias_list) == 1 else 'es'
            alias_clause = f' (alias{suffix}: {alias_str})'
          else:
            alias_clause = ''

          source_list.append(f'{grade_str} in {course_str}{alias_clause}')

        total_credit_str = f' ({sending_credits:0.1f} cr)' if len(source_list) > 1 else ''
        rule_str = f'{" and ".join(source_list)}{total_credit_str} transfers as '
        rules[rule.rule_key] = rule_str
      print(f'{len(rules):,} Rules {elapsed(format_start)}')

      # Gather receiving side
      print('Lookup Receiving Side')
      lookup_start = time.time()
      cursor.execute("""
      select r.rule_key, json_agg(d.*) as dst
      from transfer_rules r, destination_courses d
      where d.rule_id = r.id
      group by rule_key
      """)
      print(f'\n{cursor.rowcount:,} Rules {elapsed(lookup_start)}')
      print('Format Receiving Side')
      format_start = time.time()
      for rule in cursor:
        print(f'\r {cursor.rownumber:,}', end='')
        dests = rule.dst
        dest_list = []
        dest_credits = 0.0
        for dest in sorted(dests, key=lambda val: val['cat_num']):
          if dest['is_mesg'] or dest['is_bkcr'] or dest['course_status'] != 'A':
            admins = []
            if dest['is_mesg']:
              admins.append('M')
            if dest['is_bkcr']:
              admins.append('B')
            if dest['course_status'] != 'A':
              assert dest['course_status'] == 'I', f"{dest['course_status']} is neither A nor I"
              admins.append('I')
            credit_str = f' ({"".join(admins)})'
          elif dest['transfer_credits'] == 99:
            credit_str = '(*)'
          else:
            credit_str = f' ({dest["transfer_credits"]:0.1f} cr)'
          dest_list.append(f'{dest["discipline"]} {dest["catalog_number"]}{credit_str}')
        rules[rule.rule_key] += ' and '.join(dest_list)

      print(f'\nFormating Complete {elapsed(format_start)}')
      print('Generate rule_descriptions table')
      generate_start = time.time()
      cursor.execute("""
        drop table if exists rule_descriptions;
        create table rule_descriptions (
        rule_key text primary key,
        description text)
        """)

      with cursor.copy("copy rule_descriptions (rule_key, description) from stdin") as descriptions:
        for k, v in rules.items():
          descriptions.write_row((k, f'{v[0].upper()+v[1:]}'))
      cursor.execute(f"""
      update updates
         set update_date = '{datetime.today().isoformat()[0:10]}'
       where table_name = 'rule_descriptions'
      """)
      print('Generate Complete', elapsed(generate_start))
  exit(elapsed(session_start))
