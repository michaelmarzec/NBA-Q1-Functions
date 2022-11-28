## AWS Prep ##

import json 
import boto3

s3 = boto3.resource('s3')
bucket = s3.Bucket('nba-q1-data')

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
sheet_name = "Q1_Spreads_Upload"

sa = ## secret

team_name_updates = {
    "L.A. Lakers": "LA Lakers",
    "L.A. Clippers": "LA Clippers"
}

def extract_soup(url):
    html = urlopen(url)
    soup = BeautifulSoup(html, 'html.parser')
    return soup

def q1_spreads(url="https://www.sportsbookreview.com/betting-odds/nba-basketball/pointspread/1st-quarter/"):
    soup = extract_soup(url)
    game_box = soup.find_all("div", {"id": "tbody-nba"})
    daily_spreads = pd.DataFrame()

    for item in game_box:
        for game in item:
            team = game.find_all("span", class_="GameRows_participantBox__0WCRz")
            counter = 0
            for i, teams in enumerate(team):
                if i == 1:
                    home_team = teams.contents[0]
            spread = game.find_all("div", class_="GameRows_columnsContainer__Y94VP")
            for matchup in spread:
                all_spreads = matchup.find_all("span", class_="OddsCells_adjust__hGhKV")
                for i, x in enumerate(all_spreads):
                    if i == 5:
                        try:
                            home_spread = x.contents[0]
                        except:
                            pass
                
            new_row = {
                'Home_Team':[home_team],
                'Home_Spread':[home_spread]
                }
            new_row = pd.DataFrame(data=new_row)
            daily_spreads = pd.concat([daily_spreads, new_row], axis=0)

        daily_spreads = daily_spreads.replace({"Home_Team":team_name_updates})
    
    return daily_spreads

def google_spreadsheet_append(sa, workbook_name, sheet_name, daily_spreads):
    gc = gspread.service_account_from_dict(sa)
    sh = gc.open(workbook_name)
    worksheet = sh.worksheet(sheet_name)
    set_with_dataframe(worksheet, daily_spreads)


def main():
    daily_spreads = q1_spreads()
    google_spreadsheet_append(sa, workbook_name, sheet_name, daily_spreads)   



# ## Local Runs ##
if __name__ == "__main__":
    print('execution started')
    main()
    print('execution complete')


######## lambda ########## 

def lambda_handler(event, context):        
    
    main()
    
    return {
        'message': 'Q1 Extracted + Data Uploaded to Google Spreadsheet = Success'
    }







