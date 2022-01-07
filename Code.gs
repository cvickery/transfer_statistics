/* Once the transfer_statistics.xlsx workbook is uploaded to Google Drive and converted to a
 * Google SpreadsheetsApp, this code can be attached to it and used to adjust the columnwidths of
 * all the columns in all the sheets.
 */
function setWidths()
{
  const wb = SpreadsheetApp.getActiveSpreadsheet();
  let sheets = wb.getSheets();
  for (sheet in sheets)
  {
    sheets[sheet].setColumnWidths(1, 1, 105);  // Sending College
    sheets[sheet].setColumnWidths(2, 1, 80);   // Sending Course
    sheets[sheet].setColumnWidths(3, 1, 60);  // Num Evaluations
    sheets[sheet].setColumnWidths(4, 1, 60);  // Num Students
    sheets[sheet].setColumnWidths(5, 1, 160);  // Receiving Courses
    sheets[sheet].setColumnWidths(6, 1, 950);  // Rules
  }
}
