This is a download of the JavaScript program at 
[Scripts for creating truth.csv using flusight-csv-tools]( https://gist.github.com/lepisma/f7b92d7eadd19d1384834ed32af5d53a)
that creates the truth table for the https://github.com/FluSightNetwork/cdc-flusight-ensemble project . We use it to
create truth tables for the following Zoltar project. The directory was downloaded on 2018-05-17.

- make_cdc_flu_contests_project.py
  - const SEASONS = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]
  - const OUTPUT_FILE = 'truths-2010-through-2018.csv' // originally truths.csv

That output file was created by editing the variables SEASONS and OUTPUT_FILE variables as above, and then running
`node index.js`.
