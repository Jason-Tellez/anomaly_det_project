# imports
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn import metrics
import env
import os


def new_data():
    """
    Function creates new dataframe using by collecting data from mysql database
    and converts to pandas dataframe
    Returns dataframe
    """
    #url for mysql
    url = f'mysql+pymysql://{env.user}:{env.password}@{env.host}/curriculum_logs'
    # mysql query
    df = pd.read_sql("""
                    SELECT *
                    FROM logs
                    LEFT JOIN cohorts
                    ON (logs.cohort_id = cohorts.id);
                """, 
                 url
                )

    return df



def get_data():
    '''
    This function reads in titanic data from Codeup database, writes data to
    a csv file if a local file does not exist, and returns a df.
    '''
    if os.path.isfile('cohort_sql.csv'):
        
        # If csv file exists, read in data from csv file.
        df = pd.read_csv('cohort_sql.csv')
        
    else:
        
        # Read fresh data from db into a DataFrame.
        df = new_data()
        
        # Write DataFrame to a csv file.
        df.to_csv('cohort_sql.csv')
        
    return df


def variant_df(df):
    """
    Creates copy dataframe and removes images and useless searches from the 'path' column
    """
    df1 = df.copy()

    df1 = df1[~df1.path.str.endswith('jpg', na=False)]
    df1 = df1[~df1.path.str.endswith('jpeg', na=False)]
    df1 = df1[~df1.path.str.endswith('svg', na=False)]
    df1 = df1[(df1.path != '/') & (df1.path != 'search/search_index.json')]
    
    return df1


def to_datetimes(df, df1):
    """
    Function takes in original and copy dataframe, 
    converts date columns to datetime columns,
    and sets a new index for eacxh dataframe.
    
    """
    # create new index column
    df['timestamp'] = df.date + ' ' + df.time
    df1['timestamp'] = df1.date + ' ' + df1.time

    # convert columns to datetime
    df.timestamp = pd.to_datetime(df.timestamp)
    df.start_date = pd.to_datetime(df.start_date)
    df.end_date = pd.to_datetime(df.end_date)
    df.created_at = pd.to_datetime(df.created_at)
    df.updated_at = pd.to_datetime(df.updated_at)

    # convert columns to datetime
    df1.timestamp = pd.to_datetime(df1.timestamp)
    df1.start_date = pd.to_datetime(df1.start_date)
    df1.end_date = pd.to_datetime(df1.end_date)
    df1.created_at = pd.to_datetime(df1.created_at)
    df1.updated_at = pd.to_datetime(df1.updated_at)
    
    # set index for both dataframes
    df = df.set_index('timestamp').sort_index()
    df1 = df1.set_index('timestamp').sort_index()
    
    return df, df1

def prep_dfs(df, df1):
    """
    Function takes in two dataframes and creates 'hour', 'weekday', and 'month' columns,
    drops unused columns,
    creates multiple parsed path columns and drops path from originnal df
    """
    # original df
    df['hour'] = df.index.hour
    df['weekday'] = df.index.day_name()
    df['month'] = df.index.month_name()

    # copy df
    df1['hour'] = df1.index.hour
    df1['weekday'] = df1.index.day_name()
    df1['month'] = df1.index.month_name()
    
    # drop unused or null columns
    df = df.drop(columns=['Unnamed: 0', 'id', 'deleted_at'])
    df1 = df1.drop(columns=['Unnamed: 0', 'id', 'deleted_at'])
    
    # create multiple path columns and join to both dfs
    request_path_and_params = df.path.str.split('/', expand=True)
    request_path_and_params.columns = ['path_1', 'path_2', 'path_3', 'path_4', 'path_5', 'path_6', 'path_7', 'path_8']
    #drop original path columns in original dataframe only
    df = df.drop(columns='path').join(request_path_and_params)
    df1 = df1.join(request_path_and_params)
    
    return df, df1
    
    
    
def wrangle_data(df):
    """
    Function utilizes all functions in this module and makes life easy.
    Returns original dataframe and its variant.
    """
    df1 = variant_df(df)
    df, df1 = to_datetimes(df, df1)
    df, df1 = prep_dfs(df, df1)
    
    return df, df1