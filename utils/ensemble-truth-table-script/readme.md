This is a download of the JavaScript program at 
[Scripts for creating truth.csv using flusight-csv-tools]( https://gist.github.com/lepisma/f7b92d7eadd19d1384834ed32af5d53a)
that creates the truth table for the https://github.com/FluSightNetwork/cdc-flusight-ensemble project . We use it to
create truth tables for the following three Zoltar projects. The directory was downloaded on 2018-05-17.

1. make_cdc_flusight_ensemble_project.py
   const SEASONS = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017]
   const OUTPUT_FILE = 'truths-2010-through-2017.csv' // originally truths.csv

2. make_2017_2018_flu_contest_project.py (truths-2017-2018-reichlab.csv)
   const SEASONS = [2017]
   const OUTPUT_FILE = 'truths-2017-2018-reichlab.csv'
  
3. make_2016_2017_flu_contest_project.py
   const SEASONS = [2016]
   const OUTPUT_FILE = 'truths-2016-2017-reichlab.csv'

These files were created by editing the variables SEASONS and OUTPUT_FILE as above, and then running `node index.js`.
