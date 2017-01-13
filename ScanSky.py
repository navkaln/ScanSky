import requests
import json
#from skyscanner import Flights
import os.path
import pandas as pd
from geopy.distance import great_circle
import numpy as np
from ast import literal_eval
import time
import csv

pd.set_option('display.width', 240)
pd.set_option('display.height', 150)
pd.set_option('display.memory_usage', True)

#default values
originplace = 'nyc'
country = 'US'
currency = 'USD'
locale = 'en-US'
locationSchema = 'iata'
grouppricing = 'on'
destinationplace = ''
outbounddate = ''
inbounddate = ''
adults = '1'
children = '0'
infants = '0'
carriernumber = ''
cabinclass = 'Economy'
price = '0'
carriername = ''
apikey = 'ni208835327091771542482984501719'
errors = 'graceful'
destinationplace = 'anywhere'

def get_cache(originplace, country = 'US', currency = 'USD', locale = 'en-US', destinationplace='anywhere'):
    #requests cached data on SkyScanner
    s = 'http://partners.api.skyscanner.net/apiservices/browsequotes/v1.0/' + country + '/' + currency + '/' + locale + '/' + originplace + '/' + destinationplace + '/anytime/anytime?apiKey=' + apikey
    print('requesting ' + s)
    r = requests.get(s)

    #reads json fields: Quotes, Carriers, Places
    quotesdump = pd.read_json(json.dumps(r.json()['Quotes']))
    # carriersdump = pd.read_json(json.dumps(r.json()['Carriers']))
    # placesdump = pd.read_json(json.dumps(r.json()['Places']))

    # writes to data dumps
    if os.path.isfile('dump.csv'):
        with open('dump.csv', 'a') as f:
            quotesdump.to_csv(f, header=False, encoding='ISO-8859-1')
    else:
        quotesdump.to_csv('dump.csv', header=True, encoding='ISO-8859-1')
    #
    # if os.path.isfile('carriers.csv'):
    #     with open('carriers.csv', 'a') as f:
    #         carriersdump.to_csv(f, header=False, encoding= 'ISO-8859-1')
    # else:
    #     carriersdump.to_csv('carriers.csv', header=True, encoding= 'ISO-8859-1')
    #
    # if os.path.isfile('places.csv'):
    #     with open('places.csv', 'a') as f:
    #         placesdump.to_csv(f, header=False, encoding= 'ISO-8859-1')
    # else:
    #     placesdump.to_csv('places.csv', header=True, encoding= 'ISO-8859-1')
    with open('countrydata.csv', 'a', newline='', encoding='ISO-8859-1') as f:
        w = csv.writer(f)
        w.writerow([originplace, destinationplace, len(quotesdump)])





def get_caches(popularity1 = 200):
    """
    Gets cache for airports in order of popularity.
    :param popularity1:
    """
    apdf = pd.read_csv('PopularAirportCoordsZones.csv',
                       usecols=[0,1,2,3,4],
                       names=['Airport','Popularity','Latitude','Longitude','Zone'],
                       encoding='ISO-8859-1')
    #gets series of countries
    countries = pd.read_csv('countries.csv',
 				encoding='ISO-8859-1')['SkyscannerCode']
    #drops airports below popularity threshold
    airports1 = apdf[apdf.Popularity > popularity1]['Airport']

    #gets caches from these airports
    for a1 in airports1:
        for country in countries:
            try:
                get_cache(a1,destinationplace=country)
            except:
                time.sleep(5)


def read_dump(filters = True):
    #read dump.csv into df with InboundLeg & OutboundLeg read as dictionaries
    df = pd.read_csv('dump.csv',
                     usecols=[
                         'MinPrice',
                         'Direct',
                         'InboundLeg',
                         'OutboundLeg',
                         'QuoteDateTime'
                         ],
                     converters={
                         'InboundLeg':literal_eval,
                         'OutboundLeg':literal_eval,
                         'QuoteDateTime':pd.to_datetime
                         },
                     encoding='ISO-8859-1')

    #parse dictionaries and clean
    df['Origin1'] = df['OutboundLeg'].apply(lambda x: x['OriginId'])
    df['Destination1'] = df['OutboundLeg'].apply(lambda x: x['DestinationId'])
    df['Carrier1'] = pd.to_numeric(df['OutboundLeg'].apply(lambda x: x['CarrierIds']).astype(str).apply(lambda x: x[1:-1]))
    df['DepartureDate1'] = df['OutboundLeg'].apply(lambda x: pd.to_datetime(x['DepartureDate']))

    df['Origin2'] = df['InboundLeg'].apply(lambda x: x['OriginId'])
    df['Destination2'] = df['InboundLeg'].apply(lambda x: x['DestinationId'])
    df['Carrier2'] = pd.to_numeric(df['InboundLeg'].apply(lambda x: x['CarrierIds']).astype(str).apply(lambda x: x[1:-1]))
    df['DepartureDate2'] = df['InboundLeg'].apply(lambda x: pd.to_datetime(x['DepartureDate']))

    #drop duplicates
    df = df[['Direct', 'MinPrice', 'QuoteDateTime', 'Origin1', 'Destination1', 'Carrier1', 'DepartureDate1', 'Origin2', 'Destination2', 'Carrier2', 'DepartureDate2']].drop_duplicates()

    #open jaw trips are filtered, result written to test.csv
    OJOrigin = (df[(df['Origin2'] != df['Destination1'])])
    OJDestination = (df[(df['Origin1'] != df['Destination2'])])
    OJOrigin.to_csv('OJOrigin.csv', encoding= 'ISO-8859-1')
    OJDestination.to_csv('OJDestination.csv', encoding= 'ISO-8859-1')

    df = df[(df['Origin2'] == df['Destination1'])]
    df = df[(df['Origin1'] == df['Destination2'])]
    df = places_merge(df)
    df = carrier_merge(df,filters)
    df = calc_cpm(df)
    df = add_links(df)
    print('read dump file')
    df.to_csv('Flights.csv', encoding= 'ISO-8859-1')


def places_merge(df):
    #joins places less than pandoribly
    places = pd.read_csv('places.csv',
                usecols=['PlaceId', 'IataCode'],
                encoding = 'ISO-8859-1')

    places = places[pd.notnull(places['IataCode'])]

    df = df.merge(places, how='left', left_on='Origin1',right_on='PlaceId')
    df = df.merge(places, how='left', left_on='Destination1',right_on='PlaceId')
    df = df[['Direct', 'MinPrice', 'IataCode_x', 'IataCode_y', 'DepartureDate1', 'Origin2','Destination2', 'DepartureDate2', 'QuoteDateTime','Carrier1','Carrier2']]
    df = df.rename(columns={'IataCode_x':'OutOrigin','IataCode_y':'OutDestination'})

    df = df.merge(places, how='left', left_on='Origin2',right_on='PlaceId')
    df = df.merge(places, how='left', left_on='Destination2',right_on='PlaceId')
    df = df.rename(columns={'IataCode_x': 'InboundOrigin', 'IataCode_y': 'InboundDestination'})
    df = df[['Direct', 'MinPrice', 'OutOrigin', 'OutDestination', 'DepartureDate1', 'InboundOrigin', 'InboundDestination', 'DepartureDate2', 'QuoteDateTime','Carrier1','Carrier2']]
    return df


def carrier_merge(df,filters):
    #joins carriers, renames CarrierId to OutboundCarrier and InboundCarrier
    carriers = pd.read_csv('carriers.csv',
                           usecols=['CarrierId','Name'],
                           encoding='ISO-8859-1')

    df = df.merge(carriers, how='left', left_on='Carrier1', right_on='CarrierId')
    df = df.merge(carriers, how='left', left_on='Carrier2', right_on='CarrierId')
    df = df[['Direct','MinPrice','OutOrigin','OutDestination','Name_x','Name_y','InboundOrigin','InboundDestination','DepartureDate1','DepartureDate2','QuoteDateTime']]
    df = df.rename(columns={'Name_x':'OutboundCarrier','Name_y':'InboundCarrier'})
    if filters == True:
        df = df[df['OutboundCarrier'] != 'Wizz Air']
        df = df[df['OutboundCarrier'] != 'Thomas Cook Airlines']
        df = df[df['OutboundCarrier'] != 'Blue Air']
    return df

def calc_cpm(df):
    #calculates distance using coordinates of airports, then cpm using MinPrice
    apdf = pd.read_csv('PopularAirportCoordsZones.csv',
                       usecols=[0, 2, 3],
                       names=['Airport', 'Latitude', 'Longitude'],
                       encoding='ISO-8859-1')

    df = df.merge(apdf, how='left', left_on='OutOrigin', right_on='Airport')
    df = df.merge(apdf, how='left', left_on='OutDestination', right_on = 'Airport')

    #calculate distance between coordinates
    df['Distance'] = df.apply(
        lambda x : great_circle(
                (x['Latitude_x'],x['Longitude_x']),
                (x['Latitude_y'],x['Longitude_y'])
            ).miles, axis=1)

    #calculate CPM
    df['CPM'] = df.apply( lambda x : 50 * x['MinPrice'] / x['Distance'],axis=1)
    df = df[df['CPM'] < 6]
    return df[['Direct', 'MinPrice','OutOrigin','OutDestination','OutboundCarrier','InboundCarrier','DepartureDate1','DepartureDate2','Distance','CPM','QuoteDateTime']]

def add_links(df):
    #formats link to go to momondo or google flights with search parameters of flight
    df['MomondoLink'] = df.apply(
            lambda x: 'http://www.momondo.com/flightsearch/?Search=true&TripType=2&SegNo=2&SO0=' +
                x['OutOrigin'] + '&SD0=' +
                x['OutDestination'] + '&SDP0=' +
                x['DepartureDate1'].strftime('%d-%m-%Y') + '&SO1=' +
                x['OutDestination'] + '&SD1=' +
                x['OutOrigin'] + '&SDP1=' +
                x['DepartureDate2'].strftime('%d-%m-%Y') + '&AD=1&TK=ECO&DO=false&NA=false'
                , axis=1)
    df['GoogleLink'] = df.apply(
            lambda x: 'https://www.google.com/flights/#search;f=' +
                  x['OutOrigin'] + ';t=' +
                  x['OutDestination'] + ';d=' +
                  x['DepartureDate1'].strftime('%Y-%m-%d') + ';r=' +
                  x['DepartureDate2'].strftime('%Y-%m-%d')
                  , axis=1)
    return df[['Direct', 'MinPrice','OutOrigin','OutDestination','OutboundCarrier','InboundCarrier','DepartureDate1','DepartureDate2','Distance','CPM','QuoteDateTime','MomondoLink','GoogleLink']]
