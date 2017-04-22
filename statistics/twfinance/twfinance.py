import os
import time
import asyncio
import aiohttp
import requests
import pandas as pd
from bs4 import BeautifulSoup as BS
from datetime import date, timedelta

def str_rocdate_to_AD(d):
    dt = [int(x) for x in d.split('/')] 
    dt[0] = dt[0]+1911
    return date(dt[0],dt[1],dt[2])

async def async_post(url, payload, encoding, q_list):
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=payload) as res:
            data = await res.text(encoding=encoding)
    q_list.append(data)

async def twse_process(start_date, ohlc_list, vol_list):
    start = time.time()
    if os.path.exists('dataset/finance/twse.h5'):
        pass
    else:
        dateso = start_date
    nodatel = []
    while True:
        dateso = dateso.replace(day=15)
        ym = [dateso.year, str(dateso).split('-')[1]]
        nodatel.append(ym)
        if dateso.year == date.today().year and dateso.month == date.today().month:
            break
        dateso += timedelta(30)
        
    ohlc_url = 'http://www.twse.com.tw/ch/trading/indices/MI_5MINS_HIST/MI_5MINS_HIST.php'
    vol_url = 'http://www.twse.com.tw/ch/trading/exchange/FMTQIK/FMTQIK.php'
    
    await asyncio.wait([async_post(ohlc_url, {'myear':d[0]-1911, 'mmon':d[1]}, 'big5', ohlc_list) for d in nodatel] + 
                       [async_post(vol_url, dict(zip(('query_year', 'query_month'), d)), 'utf8', vol_list) for d in nodatel])
    end = time.time()
    print('Request complete in {} second(s)'.format(end-start))
    return None
    

def twse_daily(sdate=date(2005,1,15)):
    ohlc_list, vol_list = ([], [])
    #if 
    #pd.read_hdf('dataset/finance/twse_daily.h5', key='twse_daily')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(twse_process(sdate, ohlc_list, vol_list))
    
    ohlc_df = pd.concat([pd.read_html(str(BS(rtext, 'lxml').select('.board_trad')[0]),header=1)[0] for rtext in ohlc_list])
    ohlc_df.columns = ['Date', 'Open', 'High', 'Low', 'Close']
    ohlc_df['Date'] = ohlc_df['Date'].map(lambda x:str_rocdate_to_AD(x))
    vol_df = pd.concat([pd.read_html(rtext)[0] for rtext in vol_list])
    vol_df.columns = ['Date', 'Share', 'Volume', 'Trans', 'Price', 'UD']
    vol_df['Date'] = vol_df['Date'].map(lambda x:str_rocdate_to_AD(x))
    df = pd.merge(ohlc_df, vol_df, on='Date')
    df['Date'] = pd.to_datetime(df.Date)
    df = df.sort_values('Date').reset_index(drop=True)
    df.to_hdf('dataset/finance/twse_daily.h5', key='twse_daily', 
               mode='w', format='table', data_columns=True)
    return df
    
    
    
    
    
    
    
       
        
