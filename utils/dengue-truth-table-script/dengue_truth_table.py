###########################################
# Reich Lab ~ Katie House ~ 5/18/18
# Connects to sphhs-bioep01 Dengue DB
# Generates biweekly truths.csv 
###########################################

# ~~~ IMOPORT LIBRARIES ~~~
import sys
import psycopg2
import pandas.io.sql as pdsql
from tabulate import tabulate
import pandas as pd
import numpy as np
import getpass

# ~~~ FUNCTIONS ~~~
def connect_to_db():
    username = input("Username: ")
    password = getpass.getpass('Password:')
    conn_string = "host='sphhs-bioepi01.umass.edu' \
                    dbname='dengue_cases' user='%s' \
                    password='%s' port=6392" % (username,password)
    try:
        print("\nConnecting to database...")
        conn = psycopg2.connect(conn_string)
    except:
        print("Error connecting to the Database.")
        print("Check your username/password")
        sys.exit()
    print("Database connected.")
    return conn
    

# ~~~ MAIN FUNCTION ~~~
def main(): 
    conn = connect_to_db()
    # ~~ SQL QUERIES ~~
    # Select from aggregate_table() and store in Pandas DF
    sqlCode = """SELECT date_sick_year, time_in_year, geocode, cases 
                FROM aggregate_table() 
                    WHERE date_sick_year>2016;"""
    print("Reading in 'aggregate_table()' table...")
    print("(this will take a while)")
    df_truth = pdsql.read_sql_query(sqlCode, conn)

    # Select from thailand_provinces and store in Pandas DF
    sqlCode = """SELECT fips, geocode_province 
            FROM thailand_provinces"""
    print("Reading in 'thailand_provinces' table...")
    df_provinces = pdsql.read_sql_query(sqlCode, conn)
        
    # ~~ PANDAS PREPROCESSING ~~
    print("\nCreating truths.csv...")

    # Join 'geocode' values with Province Names
    df = df_truth.set_index('geocode')\
                    .join(df_provinces.set_index('geocode_province'))
    
    # Converting Biweekly values to timevalues in 'yyyymmdd' format
    df_dates = pd.read_csv('mapping_dates_metadata.csv')
    df_dates = df_dates[['delivery_year','biweek','date_delivered']]

    # Create a YYYY+biweek column to map time values to biweeks
    df['date_sick_year'] = df['date_sick_year'].astype(int)
    df['time_in_year'] = df['time_in_year'].astype(int)
    df['map_biweek'] = df['date_sick_year'].astype(str) + \
                        df['time_in_year'].astype(str)
    df_dates['map_biweek'] = df_dates['delivery_year'].astype(str) + \
                                df_dates['biweek'].astype(str)
    df = df.set_index('map_biweek').join(df_dates.set_index('map_biweek'))
    df['timezero'] = df['date_delivered']
    df = df[['timezero','fips','cases']]
    df = df.dropna()
    df['timezero'] = df.timezero\
                    .apply(lambda x: pd.to_datetime(x).strftime('%Y%m%d'))

    # Initialize target values as '0 Biweeks Ahead'
    df['target'] ='0_biweek_ahead'

    # ~~ GENERATE TARGETS ~~
    # Iterate through each province
    provinces = df.fips.unique()
    for i in range(len(provinces)):
        # Filter by province
        df_target =  df[(df.fips == provinces[i])]
        # loop through biweekly targets (-1 is 1 week ahead...)
        for j in [1,-1,-2,-3]:
            df_add = df_target.sort_values(by=['timezero'])
            df_add['target']='%i_biweek_ahead' % (j*-1)
            df_add.cases = df_add.cases.shift(j)
            df = df.append(df_add, ignore_index=True)

    # Format and preprocess truths.csv
    df['cases'].fillna("NA", inplace=True)
    df.columns = ['timezero', 'location', 'value', 'target']
    df = df[['timezero','location','target','value']]
    df = df.sort_values(by=['timezero','location','target'])
    df.to_csv('truths.csv', sep=',',index=False)
    # Uncomment below to print truth Dataframe:
    # print(tabulate(df, headers='keys', tablefmt='psql'))


if __name__ == "__main__":
    main()
    print("truths.csv is generated.")