#! /usr/local/bin/python3

import sys

from adjustcolwidths import adjust_widths

from collections import defaultdict
from openpyxl import Workbook
from openpyxl.styles import Font

highlighted = Font(bold=True, color='800080')


def get_subject(course_str):
  """Extract subject from course string (e.g., 'ENGL 101' -> 'ENGL')"""
  return course_str.split()[0] if course_str else None


def create_sender_subject_workbook(dst_institution, institution_stats):
  """Create a workbook for a receiving institution with sheets organized by sender and subject"""
  from transfer_statistics import metadata

  wb = Workbook()

  # First, organize data by sending institution
  senders_dict = defaultdict(lambda: defaultdict(list))

  # Collect courses for this receiving institution
  for src_course, stats in institution_stats.items():
    try:
      sender = metadata[src_course].institution
      # Get all receiving course subjects for this transfer
      for dst_course_str in stats.courses:
        subject = get_subject(dst_course_str)
        if subject:
          course_data = {
            'sending_course': metadata[src_course].course_str,
            'students': len(stats.students_set),
            'repeats': stats.num_evaluations - len(stats.students_set),
            'sending_cr': stats.units_taken / stats.num_evaluations,
            'real': stats.real_credits / stats.num_evaluations,
            'bkcr': stats.bkcr_credits / stats.num_evaluations,
            'percent_real': (100.0 * stats.real_credits) /
            (stats.real_credits + stats.bkcr_credits +
              (stats.units_taken - (stats.real_credits + stats.bkcr_credits))),
            'receiving_courses': dst_course_str,
            'rule_descriptions': '\n'.join(rule.split('|')[0] for rule in stats.rules)
          }
          senders_dict[sender][subject].append(course_data)
    except KeyError:
      print(f'No metadata for {src_course}', file=sys.stderr)

  # Create sheets with naming convention: SendingCollege_Subject
  for sender in sorted(senders_dict.keys()):
    for subject in sorted(senders_dict[sender].keys()):
      sheet_name = f"{sender[:3]}_{subject}"  # Truncate sender name if needed
      ws = wb.create_sheet(title=sheet_name)

      # Set up headers
      headers = ['Sending Course', 'Students', 'Repeats', 'Sending Cr',
                 'Real', 'BKCR', '% Real', 'Receiving Courses', 'Rule Descriptions']
      for col, header in enumerate(headers, 1):
        ws.cell(1, col, header).style = 'center_top'

      # Add data
      courses = senders_dict[sender][subject]
      for row, course_data in enumerate(sorted(courses,
                                               key=lambda x: x['students'],
                                               reverse=True), 2):
        ws.cell(row, 1, course_data['sending_course']).style = 'left_top'
        ws.cell(row, 2, course_data['students']).style = 'counter_format'
        ws.cell(row, 3, course_data['repeats']).style = 'counter_format'
        ws.cell(row, 4, course_data['sending_cr']).style = 'decimal_format'
        ws.cell(row, 5, course_data['real']).style = 'decimal_format'
        ws.cell(row, 6, course_data['bkcr']).style = 'decimal_format'
        ws.cell(row, 7, course_data['percent_real']).style = 'decimal_format'
        ws.cell(row, 8, course_data['receiving_courses']).style = 'left_top'
        ws.cell(row, 9, course_data['rule_descriptions']).style = 'left_top'

        # Highlight rows with less than 50% real credits
        if course_data['percent_real'] < 50.0:
          for col in range(1, len(headers) + 1):
            ws.cell(row, col).font = highlighted

  # Remove default sheet and adjust column widths
  if 'Sheet' in wb.sheetnames:
    del wb['Sheet']

  adjust_widths(wb, [10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 20.0, 150.0])
  return wb
