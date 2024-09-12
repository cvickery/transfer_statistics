# Transfer Statistics
Statistics for how courses transfer at CUNY

Tabulate the way courses transfer as blanket credits between CUNY institutions.

## Setup
Run the query CV\_QNS\_XFER\_STATS on CUNYfirst and copy the resulting csv file to downloads.
The Criteria tab is where the articulation term range is hard-coded.

Run the local command (build\_rule\_descriptions.py* to update the *rule\_descriptions* table in the cuny_curriculum database.

## Generate Reports
**Usage:** `transfer_statistics.py [-b] [-c]`

Use the `-b` option to generate a database table of transfer rules where a course transfers only as
a blanket credit course

The `-c` option counts how often the courses identified by the `-b` option actually transferred, and
how they tranferred.

Two reports are generated in the *reports* dir: an Excel sheet with all the details about most-frequently transfered courses to each college, and a text file with the percentage of “real” courses transferred to each college.
