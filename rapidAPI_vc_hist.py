from datetime import date, timedelta
import requests
import pandas as pd
import io

""" ==============================================================================================
                                   vars
==============================================================================================="""
location = "kolkata"
key = "YOUR__KEY"
url = "https://visual-crossing-weather.p.rapidapi.com/history"

querystring = {"startDateTime":"2017-01-01T00:00:00","aggregateHours":"1","location":location,"endDateTime":"2017-01-30T00:00:00","unitGroup":"us","dayStartTime":"00:00:00","contentType":"csv","dayEndTime":"23:59:00","shortColumnNames":"0"}

headers = {
	"X-RapidAPI-Key": key,
	"X-RapidAPI-Host": "visual-crossing-weather.p.rapidapi.com"
}

start_date = date(2017, 1, 1)
end_date = date(2023,1,1)
df = None # data will be saved as pandas dataframe

""" ==============================================================================================
 A function to yeild max amount of daterange per iter, according to api restrictions (FREE tier)
==============================================================================================="""
def daterange(start_date, end_date, interval=19):
    for n in range(interval, int((end_date - start_date).days), interval):
        yield start_date + timedelta(n)
    yield end_date




""" ==============================================================================================
                                                main
==============================================================================================="""

i = 0
start_time = start_date
for end_time in daterange(start_date, end_date):
    print(i, "  ", start_time, end_time)  # each year would take around 21-22 iterations
    querystring["startDateTime"] = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    querystring["endDateTime"] = end_time.strftime("%Y-%m-%dT%H:%M:%S")
    response = requests.get(url, headers=headers, params=querystring)

    # print(response.text)

    if df is None:
        df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
    else:
        df = pd.concat([df, pd.read_csv(io.StringIO(response.content.decode('utf-8')))], ignore_index=True)

    i+=1
    
    start_time = end_time


df.to_csv("kolkata2017-2022.csv")