### Justin Parsons 2/3/2025 ###
### Adapted from functions written by Cliff Hunt for retrieving FMP data via their API ###

### references https://stackoverflow.com/questions/68186451/what-is-the-proper-way-of-using-python-requests-requests-requestget-o
###            https://itsrorymurphy.medium.com/api-rate-limits-a-beginners-guide-7f27cb3975cb
####All APIs in this project use a fixed window method to limit the number of API calls to 300 per minute.  


import unittest
import pandas as pd
import requests
from time import sleep
import certifi
import time
import re
import ssl
from urllib.request import urlopen
import json

###
#NOT CURRENTLY FUNCTIONING: Unit test for api call function 
###

class TestAPICall(unittest.TestCase):

    def test_api_call(self):
        # Replace with your actual API key and year
        API_KEY = "xIpuhIckPKHQGl9iyVkGoXl1pZWSMcs9"
        year = 2013
        test_df = pd.DataFrame()  # Create an empty DataFrame as input

        try:
            result_df = get_ESG_for_year(Summary_df, year, API_KEY)
            # Assert that the result is not empty 
            self.assertFalse(result_df.empty)
            print(result_df.head()) # Print the first few rows of the result
        except Exception as e:
            self.fail(f"API call failed: {e}")


    if __name__ == '__main__':
        unittest.main(argv=['first-arg-is-ignored'], exit=False)


###
# API call 
###
def get_jsonparsed_data(url):
    try:
        response = requests.get(url)
        if response.ok:
           data = response.json()
           return data
        else:
             print(f"Error retrieving data for {url.split('/')[-1]} ({response.status_code})", end="\r")
             return []
    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        return []

###
# ESG score data
###

def get_ESG_V2(mm_df, API_Key):
    ESG_df = pd.DataFrame(columns=['Symbol_1', 'date', 'environmentalScore', 'socialScore', 'governanceScore', 'ESGScore'])
    ESG_df['date'] = None
    ESG_df['environmentalScore'] = None
    ESG_df['socialScore'] = None
    ESG_df['governanceScore'] = None
    ESG_df['ESGScore'] = None
    
    # initialize API call counter and start time
    request_count = 0
    start_time = time.time()

    for ticker in mm_df['Symbol_1'].unique():  
        ESG_df = pd.concat([ESG_df, pd.DataFrame([{'Symbol_1': ticker}])], ignore_index=True)
        url = f"https://financialmodelingprep.com/api/v4/esg-environmental-social-governance-data?symbol={ticker}&apikey={API_Key}"
       
        # check if we've made 300 calls in the last minute
        if request_count >= 300 and time.time() - start_time <= 60:
            # sleep for the remaining time until the next minute
            sleep_time = 60 - (time.time() - start_time)
            time.sleep(sleep_time)
            # reset counter and start time
            request_count = 0
            start_time = time.time()

        # make the API call
        data = get_jsonparsed_data(url)
        request_count += 1

        # initialize score variables to None before the loop
        date = None
        environmentalScore = None
        socialScore = None
        governanceScore = None
        ESGScore = None
        for record in data:
            # retrieve scores for ticker, ensuring they are assigned
            date = record.get('date', None)
            environmentalScore = record.get('environmentalScore', None)
            socialScore = record.get('socialScore', None)
            governanceScore = record.get('governanceScore', None)
            ESGScore = record.get('ESGScore', None)
            break  # exit the loop after processing the first record
            
        # update df with the assigned (or default None) values
        ESG_df.loc[ESG_df['Symbol_1'] == ticker, 'date'] = date
        ESG_df.loc[ESG_df['Symbol_1'] == ticker, 'environmentalScore'] = environmentalScore
        ESG_df.loc[ESG_df['Symbol_1'] == ticker, 'socialScore'] = socialScore
        ESG_df.loc[ESG_df['Symbol_1'] == ticker, 'governanceScore'] = governanceScore
        ESG_df.loc[ESG_df['Symbol_1'] == ticker, 'ESGScore'] = ESGScore

    return ESG_df

###
# Get current employee count\
###

def get_employeeCount(mm_df, API_Key):
    EC_df = pd.DataFrame(columns=['Symbol_1', 'periodOfReport', 'employeeCount'])
    EC_df['periodOfReport'] = None
    EC_df['employeeCount'] = None
    
    # Initialize API call counter and start time
    request_count = 0
    start_time = time.time()

    for ticker in mm_df['Symbol_1'].unique():  
        EC_df = pd.concat([EC_df, pd.DataFrame([{'Symbol_1': ticker}])], ignore_index=True)
        url = f"https://financialmodelingprep.com/api/v4/employee_count?symbol={ticker}&apikey={API_Key}"
       
        # check if we've made 300 calls in the last minute
        if request_count >= 300 and time.time() - start_time <= 60:
            # sleep for the remaining time until the next minute
            sleep_time = 60 - (time.time() - start_time)
            time.sleep(sleep_time)
            # reset counter and start time
            request_count = 0
            start_time = time.time()

        # make the API call
        data = get_jsonparsed_data(url)
        request_count += 1

        # initialize score variables to None before the loop
        periodOfReport = None
        employeeCount = None
        
        for record in data:
            # Retrieve scores for ticker, ensuring they are assigned
            periodOfReport = record.get('periodOfReport', None)
            employeeCount = record.get('employeeCount', None)
            break  # Exit the loop after processing the first record
            
        # update df with the assigned (or default None) values
        EC_df.loc[EC_df['Symbol_1'] == ticker, 'periodOfReport'] = periodOfReport
        EC_df.loc[EC_df['Symbol_1'] == ticker, 'employeeCount'] = employeeCount

    return EC_df

###
#Employee count by year. Years are hardcoded within this function for now. I had to ask GPT for help...
###

def get_hist_employeeCount(mm_df, API_Key):
    EC_hist_data = []  # List to store extracted data
    request_count = 0
    start_time = time.time()

    # years to collect data over
    target_years = set(range(2013, 2024)) 


    for ticker in mm_df['Symbol_1'].unique():  # looping over all entries
        url = f"https://financialmodelingprep.com/api/v4/historical/employee_count?symbol={ticker}&apikey={API_Key}"
        
        # enforce API rate limit (300 requests per minute)
        if request_count >= 300 and time.time() - start_time <= 60:
            time.sleep(60 - (time.time() - start_time))  # Sleep until next minute
            request_count = 0
            start_time = time.time()

        data = get_jsonparsed_data(url)
        request_count += 1

        # extract employee counts for years 2013-2023
        for record in data:
            periodOfReport = record.get('periodOfReport', '')  # Extract year
            employeeCount = record.get('employeeCount', None)

            # ensure periodOfReport is a valid year and within our range
            try:
                year = int(periodOfReport[:4])  # Extract first 4 characters (year)
                if year in target_years:
                    EC_hist_data.append({'Symbol_1': ticker, 'year': year, 'employeeCount': employeeCount})
            except ValueError:
                continue  # Skip if year extraction fails

    # create DataFrame from collected data
    EC_hist_df = pd.DataFrame(EC_hist_data)

    return EC_hist_df

###
# Placeholder for log file output
###
    

