# Transfer Statistics
Statistics for how courses transfer at CUNY

Tabulate the way courses transfer as blanket credits between CUNY institutions.

## Usage: `transfer_statistics.py [-b] [-c]`

Use the `-b` option to generate a database table of transfer rules where a course transfers only as
a blanket credit course

The `-c` option counts how often the courses identified by the `-b` option actually transferred, and
how they tranferred.

## Explanation

CUNY policy dictates that all courses taken at any CUNY college must transfer without loss of
credits to any other CUNY college. But “without loss of credits” is a minimal requirement: there is
nothing in the policy that says that the credits transferred will count as anything other than as
free electives that can count towards the 120 credits needed for a bachelor’s degree or the 60
needed for an associate’s degree. These credits, which do not satisfy any General Education or
Major/Certificate requirements are known as _blanket credits_. The aim of this project is to
identify which courses transfer most often as blanket credits, with the goal of reviewing their
transfer policies to see if there might be a way to make them serve a better purpose for students.

There are currently 1,485,770 rules in CUNYfirst that govern how courses transfer from one college
to another within CUNY. 1,353,241 (91%) of these rules cover cases where a course transfers only as
blanket credits. Although significant, the problem may not be as dire as this statistic would seem
to indicate.

For one thing, there are many courses offered at CUNY colleges that are never actually
transferred from one college to another. Examples would be advanced courses within a major: very few
students transfer to another college when their major is already mostly-completed.

Another issue is that there are often multiple rules for the same course, so in addition to a
“blanket credit-only” rule, there may also be other rules that covers a combination of courses that
a student may have taken, resulting in a “real” course at the receiving college. An example would be
a science course where the lecture and laboratory components are offered as separate courses. Each
component, taken alone, might count only for blanket credit, but completing the combination might
count as a full course at the receiving college.

An additional issue is that once students transfer
and have had their credits evaluated according to the CUNYfirst transfer rules, they still have the
opportunity to have their courses re-evaluted by a program advisor, general education advisor, or
financial aid officer to override the built-in rules.

## Methodology

All the information used for this project comes from CUNYfirst. Automated scripts pull all the
information about all CUNY course and all transfer rules, and populate a local Postgres database
with the same information. The scripts identify various anomalies in the CUNYfirst information and
generate a cleaned-up representation in the local database. CUNYfirst also keeps a record each time
a student’s transfer courses are evaluated. For this project we used 2,055,131 transfer evaluaton
records covering all within-CUNY transfers between Spring 2019 and Spring 2022 semesters.

When run with the `-b` option, _transfer\_statistics_ identifies each rule where a single course
transfers as a single blanket-credit course at a receiving college. It stores information about all
rules governing how that course transfers to the receiving college, not just the one where only
blanket credit is given.

When run with the `-c` option, _transfer_statistics_ looks at all cases where a course for which a
blanket credit-only rule exists was actually transfered. In this case it looks at all the cases in
which the course transferred, which would include any of the rules governing that course as well as
any advisor overrides for the course.

## Results

As mentioned above, there are 1,353,241 transfer rules where a course transfers only as a single
blanket-credit course. Of those cases, 1,348,553 (99.7%) have no other rules for how the course
transfers.

  Num Rules | Count
 --:+--:
  1 |1,348,553
  2 |    4,419
  3 |      167
  4 |       38
  5 |        2
  6 |        4
  7 |        1
  8 |       57


