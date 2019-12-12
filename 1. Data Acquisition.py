# Packages that will be referred
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
import numpy as np

# City names, in list form, all small, hyphen for space
city = ['logan', 'north-logan', 'providence', 'smithfield', 'hyde-park', 'hyrum', 'mendon', 'nibley', 'richmond']

# Root url
url='https://www.zillow.com/homes/'

# City url
cityurl = ['https://www.zillow.com/homes/'+ str(c)+ '-UT_rb/' for c in city]

# Create an agent for request to avoid robot check 
headers = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko)'}

# To create different URLs
columns = ['price', 'address', 'details', 'sale_type']
df= []
aurl = []
for c in city:
   for k in range(2,10):
       downloadurl = 'https://www.zillow.com/'+ str(c) + '-ut/'+ str(k) + '_p/'
       aurl.append((downloadurl))
for x in cityurl + aurl:
    try:
        r = requests.get(x, headers = headers)
        soup = BeautifulSoup(r.text, 'html.parser')
        for o, p, q, r in zip(soup.find_all('div',class_="list-card-price"), soup.find_all('h3',class_="list-card-addr"), soup.find_all('ul',class_="list-card-details"), soup.find_all('div', class_="list-card-type")):
            # Remove any non-numeric characters
            g = int(re.sub("[^\d\.]", "", o.text))
            if g < 2000: 
                # interest rate 5% per year, 30-year mortgage
                g = np.pv(0.05/12, 30*12, -g)
            h = p.text
            i = q.text
            j = r.text
            df.append([g,h,i,j])
            time.sleep(0.4)
    except:
        continue
details = pd.DataFrame(df, columns = columns)
details

#Data Cleansing on address
address_columns = ["street", "city/area", "state/zipcode"]
address_list = []
for x in details['address']:
    ty = x.split(', ')
    address_list.append(ty)
address_df = pd.DataFrame(address_list, columns = address_columns)
details1 = pd.concat([details, address_df.reindex(details.index)], axis=1, sort=False)

#Data Cleansing on details
details_columns = ["beds", "baths", "sqft", 'acrelot']
details_list = []
for x in details['details']:
    try:
        # Cleansing the lotarea
        if 'lot' in x.lower(): 
            lot_num = float(re.sub("[^\d\.]", "", str(x)))
            if lot_num > 3000:
                lot_num = lot_num/43560
            lot = ['','','',lot_num]
            details_list.append(lot)
        # 
        elif 'bd' in x.lower():
            bbf = x.split(" bd")
            bf = bbf[1].split(" ba")
            bath = re.sub("[^\d\.]", "", bf[0])
            bed = bbf[0]
            sqft = re.sub("[^\d\.]", "", bf[1])
            residents = [int(bed), int(bath), int(sqft), ""]
            details_list.append(residents)
    except:
        details_list.append(["","","",""]) 
details_df = pd.DataFrame(details_list, columns = details_columns)
details2 = pd.concat([details1, details_df.reindex(details1.index)], axis=1, sort=False)

details3 = pd.DataFrame.drop_duplicates(details2)
details4 = pd.DataFrame.drop(details3, columns = ['address','details'])

# Drop columns address and details because they are composed with integers and strings, and non-categorical features

#To csv
details3.to_csv(r'C:\Users\Sean\Desktop\MIS 5400\Final Project\scraping.csv', index = True)

import pyodbc

conn = pyodbc.connect(           # create a connection with connection string
    r'DRIVER={ODBC Driver 13 for SQL server};'      # specify an ODBC driver
    r'SERVER=DESKTOP-736C5K5;'  # specify the MS SQL server address
    r'DATABASE=MIS 5400;'        # specify the database name on the server to which you want to connect
    r'Trusted_Connection=yes'            
    )         

cursor = conn.cursor()          #Upload the panda dataframe into SQL Server

for index,row in details4.iterrows():
    cursor.execute("INSERT INTO dbo.Zillow1([price],[sale_type],[street],[city/area],[state/zipcode],[beds],[baths],[sqft],[acrelot]) values(?,?,?,?,?,?,?,?,?)", row['price'], row['sale_type'],row['street'],row['city/area'],row['state/zipcode'],row['beds'],row['baths'],row['sqft'],row['acrelot'])
    conn.commit()
cursor.close()
conn.close()
