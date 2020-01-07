###########################################
# Reich Lab ~ Katie House ~ 8/02/18
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
import math
from datetime import datetime

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

def preprocess_metadata(df_meta):
    df_meta['biweek'] = df_meta['week'].apply(lambda x: x / 2.0)\
                                .fillna(0).apply(roundup)
    df_meta['delivery_year'] = df_meta['delivery_year']\
                                .fillna(0).apply(roundup)
    df_meta['map_biweek'] = df_meta['biweek'].astype(str) +  '-' + \
                                df_meta['delivery_year'].astype(str)
    return df_meta

def preprocess_truthdata(df):
    df['time_in_year'] = df['time_in_year'].apply(roundup)
    df['date_sick_year'] = df['date_sick_year'].apply(roundup)

    df['map_biweek'] = df['time_in_year'].astype(str) +  '-' + \
                                df['date_sick_year'].astype(str)
    return df

def roundup(x):
    return int(math.ceil(x)) 

def subtract_year(df):
  if df['time_in_year'] == 0 : 
    return df['date_sick_year'] - 1
  else: 
    return df['date_sick_year']

# ~~~ MAIN FUNCTION ~~~
def main(): 
    conn = connect_to_db()
    
    # ~~ IMPORT DATA ~~
    # Import aggregate_table() and store in Pandas DF
    sqlCode = """SELECT date_sick_year, time_in_year, geocode, cases 
                FROM aggregate_table_complete() 
                    WHERE date_sick_year>2016;"""
    print("Reading in 'aggregate_table_complete()'...")
    print("(this will take a while)")
    df_truth = pdsql.read_sql_query(sqlCode, conn)
    df_truth['geocode'] = df_truth['geocode'].astype(int)

    # Import delivery_metadata and store in Pandas DF
    print("Reading in 'delivery_metadata' table...")
    sqlCode = """select * from delivery_metadata"""
    df_meta = pdsql.read_sql_query(sqlCode, conn)

    # Import thailand_provinces and store in Pandas DF
    print("Reading in 'thailand_provinces' table...")
    sqlCode = """SELECT fips, geocode_province 
            FROM thailand_provinces"""
    df_provinces = pdsql.read_sql_query(sqlCode, conn)
    df_provinces['geocode_province'] = df_provinces['geocode_province'].astype(int)

    # Import standard timezeros and store in Pandas DF
    df_dates = pd.read_csv('timezero_data.csv')
    df_tzmap = pd.read_csv('timezero_mapping.csv')

    # ~~ PANDAS PREPROCESSING ~~
    print("\nCreating truths.csv...")

    # Join 'geocode' values with Province Names
    df_truth = df_truth.set_index('geocode')\
                .join(df_provinces.set_index('geocode_province'))
                   
    # Join the metadata table and the truth table 
    # Map: biweek to date_delivered
    df_meta = preprocess_metadata(df_meta)
    df = preprocess_truthdata(df_truth)
    df = df.set_index('map_biweek')\
                        .join(df_meta.set_index('map_biweek'))
    df = df[['date_sick_year','time_in_year',\
                'cases','fips', 'date_delivered']]

    # Drop all non-existant delivery dates
    df['date_delivered'].replace('', np.nan, inplace=True)
    df.dropna(subset=['date_delivered'], inplace=True)

    # Convert to date values
    df.date_delivered = pd.to_datetime(df.date_delivered)
    df_dates.reg_yr_datestart = pd.to_datetime(df_dates.reg_yr_datestart) 
    
    for index, row in df.iterrows():
        date_delivered = row.date_delivered
        try:
            map_to_biweek = df_dates[(df_dates['reg_yr_datestart'] <= date_delivered)]
            biweek = map_to_biweek["biweek"][-1:]
            df.loc[index,'newbiweek'] = biweek.iloc[0]
        except:
            print("No date_delivered found for biweek: ", row.time_in_year)

    df['time_in_year'] = df['newbiweek'] - 2
    df['biweek'] = df['time_in_year'].apply(lambda x: 26 + x if x <  1 else x)
    
    
    # Format timezero date
    df = pd.merge(df, df_tzmap, on='biweek', how='right')
    
    
    df['date_sick_year'] = df.apply(subtract_year,axis=1)

    df['timezero'] =  df['date_sick_year'].astype(str) + "-" + \
                        df['reg_yr_month'].astype(str)  + "-" +  \
                        df['reg_yr_day'].astype(str)
                        
    df['timezero'] = df.timezero\
                    .apply(lambda x: pd.to_datetime(x).strftime('%Y%m%d'))
    df = df[['timezero','fips','cases']]

    # Combine TH81 with TH81 and TH17 (Bueng Kan and Nong Khai)
    df.fips[df.fips == 'TH81'] = 'TH17'
    df = df.groupby(['timezero', 'fips'], as_index=False).cases.sum()
    
    # Initialize target values as '2 Biweeks Ahead'
    df['target'] ='2_biweek_ahead'

    # ~~ GENERATE TARGETS ~~
    # Iterate through each province
    provinces = df.fips.unique()
    for i in range(len(provinces)):
        # Filter by province
        df_target =  df[(df.fips == provinces[i])]
        # loop through biweekly targets (-1 is 1 week ahead...)
        for j in [1,-1,-2,-3]:
            df_add = df_target.sort_values(by=['timezero'])
            df_add['target']='%i_biweek_ahead' % ((j-2)*-1)
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