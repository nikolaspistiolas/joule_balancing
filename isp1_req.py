import requests
from datetime import datetime, timedelta
import pandas as pd
import pymongo


cl = pymongo.MongoClient('localhost',27017)
db = cl['admie']
col = db['dep_req']


def requirements(date: datetime):
    y1 = int((date-timedelta(days=1)).year)
    m1 = int((date-timedelta(days=1)).month)
    y = int(date.year)
    m = int(date.month)
    d = int(date.day)
    if m1 < 10:
        m1 = '0' + str(m1)
    if y1 < 10:
        y1 = '0' + str(y1)
    if y < 10:
        y = '0' + str(y)
    if m < 10:
        m = '0' + str(m)
    if d < 10:
        d = '0' + str(d)
    rest_url =f'https://www.admie.gr/sites/default/files/attached-files/type-file/{y1}/{m1}/{y}{m}{d}_ISP1Requirements_01.xlsx'
    data = requests.get(rest_url).content
    capacity_reserves = pd.read_excel(data, header=32, skipfooter=3).transpose()
    capacity_reserves = capacity_reserves.to_numpy()[2:]
    for i,val in enumerate(capacity_reserves[0]):
        try:
            if (float(val) >= 46 and float(val) <= 49):
                break
        except:
            continue
    capacity_reserves = capacity_reserves[:,i:]

    mandatory_hydro = pd.read_excel(data, skiprows=28, nrows=1).transpose()
    mandatory_hydro = mandatory_hydro.index.to_numpy()[2:]

    for i, val in enumerate(mandatory_hydro):
        mandatory_hydro[i] = float(str(val).split('.')[0])

    non_dispatcable = pd.read_excel(data, skiprows=6, nrows=1).transpose()
    non_dispatcable_load = non_dispatcable.index.to_numpy()[2:]
    non_dispatcable_loses = non_dispatcable.to_numpy()[2:]

    res = pd.read_excel(data, skiprows=4, nrows=1).transpose().index.to_numpy()[2:]
    ret = {
        'date' : date,
        'capacity_reserves' : capacity_reserves[2:].tolist(),
        'mandatory_hydro' : mandatory_hydro.tolist(),
        'non_dispatcable_loses' : non_dispatcable_loses.squeeze().tolist(),
        'non_dispatcable_load' : non_dispatcable_load.tolist(),
        'res' : res.tolist()
    }
    return ret

start_date = datetime(2021,2,15)
for i in range(9*30):
    d = requirements(start_date)
    print(d)
    col.insert_one(d)
    start_date += timedelta(days=1)