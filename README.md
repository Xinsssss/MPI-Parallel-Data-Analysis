# CCC-Project-1

## Current steps going to take
1. Processor:

extract doc column, extract useful columns, process timestamp

process timestamp:
format yyyy-mm-ddT(time)
get the yyyy-mm-dd out
group by this
calcualte


# Idea:

main: executes everything
--splits data? here or after--

for each chunk of data:
data preprocessor: turn raw data into useful columns (username, id, time, sentiment)
data summariser: take in argument time or username and data then output the counted version
    (maybe get top 5 and last 5 here)