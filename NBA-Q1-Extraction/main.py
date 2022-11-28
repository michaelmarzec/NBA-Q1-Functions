## AWS Prep ##

import json 
import boto3

s3 = boto3.resource('s3')
bucket = s3.Bucket('nba-q1-data')
local_file_name = '/tmp/22_23_q1_data.csv'
file_key = '22_23_q1_data.csv'

## Imports ##

import datetime
import gspread
from bs4 import BeautifulSoup
import pandas as pd
from urllib.request import urlopen
import time
from local_gspread_dataframe import set_with_dataframe, get_as_dataframe

## Hard Codes ## 

workbook_name = "22_23_NBA-Q1 Data Collection"
sheet_name = "Q1_Scores_Upload"

sa = ## secret


def extract_date_parameters(date):
    day = date.day
    month = date.month
    year = date.year
    return day, month, year

def create_url(day, month, year, base_url="https://www.basketball-reference.com/boxscores/?"):
    url = base_url + "month=" + str(month) + "&day=" + str(day) + "&year=" + str(year)
    return url

def extract_soup(url):
    html = urlopen(url)
    soup = BeautifulSoup(html, 'html.parser')
    return soup

def q1_results(date):
    d, m, y = extract_date_parameters(date)
    url = create_url((d-1), m, y)
    soup = extract_soup(url)
    q1_df = pd.DataFrame()

    game_summaries = soup.find_all("div", {"class": "game_summary"})
    for game in game_summaries:
        teams = game.find_all("table", class_="teams")
        for matchup in teams:
            away_team = matchup.find_all('a')[0].contents[0]
            home_team = matchup.find_all('a')[2].contents[0]
        scores = game.find_all("td", class_="center")

        point_totals = []
        for quarterly_score in scores:
            for x in quarterly_score:
                point_totals += [x]
        away_index = 0
        home_index = int(len(point_totals)/2)
        away_q1 = point_totals[away_index]
        home_q1 = point_totals[home_index]

        yesterday = date - datetime.timedelta(days=1)
        new_row = {
            
            'Date':yesterday, #[datetime.datetime.now().date()],
            'Away_Team':[away_team],
            'Home_Team':[home_team],
            'Away_Score':[away_q1],
            'Home_Score':[home_q1]
            }

        new_row = pd.DataFrame(data=new_row)

        q1_df = pd.concat([q1_df, new_row], axis=0)
    return q1_df

def todays_results(today, q1_full_df):
    q1_full_df = q1_results(today)
    return q1_full_df


def google_spreadsheet_append(sa, workbook_name, sheet_name, q1_full_df):
     # gc = gspread.service_account(filename="./service_account.json")
    gc = gspread.service_account_from_dict(sa)
    sh = gc.open(workbook_name)
    worksheet = sh.worksheet(sheet_name)

    existing = get_as_dataframe(worksheet, parse_dates=True, usecols=[0,1,2,3,4], header=0)
    existing = existing.dropna()
    updated = existing.append(q1_full_df)
    set_with_dataframe(worksheet, updated)

    return updated


def main(filename='./22_23_q1_data.csv'):
    nba_start_date = datetime.datetime(2022, 10, 18).date()
    today = datetime.datetime.now().date()

    q1_full_df = pd.DataFrame()
    q1_full_df = todays_results(today, q1_full_df)

    q1_full_df = google_spreadsheet_append(sa, workbook_name, sheet_name, q1_full_df)   
    # q1_full_df.to_csv(filename, index=False)



# ## Local Runs ##
if __name__ == "__main__":
    print('execution started')
    main()
    print('execution complete')


######## lambda ########## 

def lambda_handler(event, context):        
    
    main(local_file_name)
    
    return {
        'message': 'Q1 Extracted + Data Uploaded to Google Spreadsheet = Success'
    }







