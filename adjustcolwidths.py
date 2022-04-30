#! /usr/local/bin/python3

import openpyxl
from openpyxl.utils import get_column_letter


def adjust_widths(wb, widths: list = None) -> None:
  """ Adjust the widths of the columns in all sheets of workbook wb to the values in widths.
      Default widths are the ones used for the transfer statistics workbook.
  """
  if widths is None:
    widths = [8.0, 10.0, 10.0, 10.0, 20.0, 150.0]

  for sheet in wb.worksheets:
    for col, width in enumerate(widths, 1):
      sheet.column_dimensions[get_column_letter(col)].width = width
  return


if __name__ == '__main__':
  wb = openpyxl.open('reports/transfer_statistics.xlsx')
  adjust_widths(wb)
  wb.save('reports/transfer_statistics.xlsx')
