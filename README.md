OpenPresPoll Data
===================

This repository contains the public database of all votes
made through OpenPresPoll. You can use this data to derive your own
statistics and graphs. This repository is updated daily.

The data is organized by year and then month within the "diffs" directory.
Within each month directory, you'll find a "diff" file for each day. This
file represents the votes made through OpenPresPoll for that day.

Each diff file is a CSV file. Parsing should be relatively
straightforward. The "correlate_id" columns is an obfuscated ID for
the Twitter user who made the vote. If a user makes multiple votes,
you can update their latest vote using "correlate_id."

For convenience we've provided a script called
"create-snapshot-csv.py" that will work with stock Python 2 or 3. This
script will aggregate all the diff files into a single CSV that
represents the current snapshot of votes. The resultant CSV file is
ready for processing in Excel or your analysis tool of choice.
