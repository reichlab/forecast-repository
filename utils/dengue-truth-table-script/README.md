## Truths Table for Dengue Data

#### Files:
* `dengue_truth_table.py`: Generates `truths.csv`
* `truths.csv`: Real case values to compare to forecasting
* `mapping_dates.csv`: Maps biweek numbers to timevalues


#### Description of: `dengue_truth_table.py`
1. Connects to `dengue_cases` database
2. Queries `aggregate_table()` and `thailand_provinces` for case data
3. Transforms biweek values to timezeros
4. Iterates through each province to define target values (0_biweek_ahead, etc...)
5. Creates `truth.csv`

#### Description of: `truths.csv`
Columns:
* `timezero`: biweek mapped to a timezero date in 'yyyymmdd' format
* `location`: Province
* `target`: ["-1_biweek_ahead", "0_biweek_ahead", "1_biweek_ahead", "2_biweek_ahead", "3_biweek_ahead"] 
* `value`: Number of cases

