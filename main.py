
import json
import requests
import os
from bs4 import BeautifulSoup as bs
from pprint import pprint
from influxdb import InfluxDBClient
import schedule
import time

#LOAD .env locals
if os.path.exists('.env'):
     from dotenv import load_dotenv
     load_dotenv()

#GLOBALS
LIVE_CONN = bool(os.environ.get('LIVE_CONN', False))
INFLUX_HOST = os.environ.get('INFLUX_HOST','')
INFLUX_HOST_PORT = int(os.environ.get('INFLUX_HOST_PORT',''))
INFLUX_DATABASE = os.environ.get('INFLUX_DATABASE','')
RUNMINS =  int(os.environ.get('RUNMINS',''))
DATA_OUTPUT  = os.environ.get('JSON_OUTPUT','output')
INFLUX_CLIENT = InfluxDBClient(host=INFLUX_HOST, port=INFLUX_HOST_PORT, database=INFLUX_DATABASE)


def get_live_data():
    URI = 'https://www.yes-lease.co.uk/search?offset=0&vt=car&ft=ch0&st=full&pricesort=asc&fuel=C&months=36&mpa=8000'
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    resp = requests.get(URI, headers=headers)
    with open(DATA_OUTPUT,'wb') as fd:
        fd.write(resp.content)


def get_saved_data(*args):
    if LIVE_CONN == True:
        get_live_data()

    with open('output','rb') as fd:
        resp = fd.read()
        return resp


def write_to_influx(data_payload):
    INFLUX_CLIENT.write_points(data_payload)


def data_cleanse(working_data):
    soup = bs(working_data, features="html.parser")
    pricelist = []
    for row in soup.find_all("a", {'class':'row'}):
        car = {}
        make_model = row.find("div", {'class':'makemodel'})
        make_model = str(make_model.contents[1])
        make_model = make_model.lstrip()
        car.update({'make_model' : make_model})

        deriv = row.find("div", {'class':'deriv'})
        deriv = str(deriv.contents[0])
        car.update({'deriv' : deriv})

        model_type = row.find("div", {'class':'cell ext secondary'})
        model_type = str(model_type.contents[0])
        car.update({'model_type' : model_type})

        release = row.find("div", {'class':'cell ext'})
        release = str(release.contents[0])
        car.update({'release' : release})
        
        price = row.find_all("span", {'class':'price fg-red'})
        price = price[1].contents[0]
        price = float(price.lstrip("£"))
        car.update({'price' : price})
        pricelist.append(car)


    for car in pricelist:
        price = {'price' : car['price']}
        del car['price']
        base_dict = {'measurement' : 'ev_prices', 'tags' :car}
        time_stamp = time.time_ns()
        base_dict.update({'time': time_stamp})
        base_dict.update({'fields' : price})

        # Construct payload and insert
        data_payload = [base_dict]
        print("SUBMIT:" + str(data_payload))
        print('#'*30) 
        write_to_influx(data_payload)
    

def do_it():
    working_data = get_saved_data()
    data_cleanse(working_data)


def main():
    ''' Main entry point of the app '''
    do_it()
    schedule.every(RUNMINS).minutes.do(do_it)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    ''' This is executed when run from the command line '''
    main()