#! /usr/local/bin/python3
from openpyxl.styles import Alignment, NamedStyle, Font

# Cell formatting options
highlighted = Font(bold=True, color='800080')

center_top = NamedStyle('center_top')
center_top.alignment = Alignment(horizontal='center', vertical='top', wrapText=True)
center_top.font = Font(bold=True)

left_top = NamedStyle('left_top')
left_top.alignment = Alignment(horizontal='left', vertical='top', wrapText=True)

counter_format = NamedStyle('counter_format')
counter_format.alignment = Alignment(vertical='top')
counter_format.number_format = '#,##0'

decimal_format = NamedStyle('decimal_format')
decimal_format.alignment = Alignment(vertical='top')
decimal_format.number_format = '0.0'
