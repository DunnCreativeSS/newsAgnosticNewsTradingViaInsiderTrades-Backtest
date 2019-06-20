import pandas as pd
import requests
import urllib
from urllib.parse import urlsplit, parse_qs
from bs4 import BeautifulSoup
import datetime as dt
import json
import time
from datetime import datetime
import backtrader as bt
import backtrader
import math
from datetime import datetime
from ib.ext.Contract import Contract
from ib.ext.Order import Order
cashbal = 0
netliq = 0
import gspread
from oauth2client.service_account import ServiceAccountCredentials
orders2 = []
with open('orders.txt') as f:
    orders2 = f.read().splitlines()
ocount = 0
import calendar
from datetime import datetime, timedelta
def add_months(sourcedate,months):
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month / 12
    month = month % 12 + 1 - 1
    month = int(month)
    year = int(year)
    day = min(sourcedate.day,calendar.monthrange(year,month)[1])
    return datetime.date(datetime(year,month,day))
from pandas.tseries.offsets import BDay
orders = []
for o in orders2:
    print(ocount)
    
    past = datetime.now() - timedelta(days=2)
    # pd.datetime is an alias for datetime
    x=(orders2[ocount].split(':')[2])
    past = datetime.date(past)
    x = add_months(datetime(*[int(item) for item in x.split('-')]), 1)
    if x > past:
        print(past)
        print(x)
        orders.append(orders2[ocount].split(':')[0] + ':' +  orders2[ocount].split(':')[1])
    ocount = ocount + 1
print(orders)

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# jar138du

# DU1531456
SAMPLE_SPREADSHEET_ID = '1lty76K58tiFL0f00V2jXGtCqgJLc7O7vMKa4-4tPSTE'

from ib.opt import Connection, message
def error_handler(msg):
    """Handles the capturing of error messages"""
    print("Server Error: %s" % msg)
oid = 0
def reply_handler(msg):
    global oid
    global netliq
    global cashbal
    global boughts
    global solds
    """Handles of server replies"""
    
    if msg.typeName == 'accountSummary' and msg.account == 'DU1531456' and msg.tag == 'NetLiquidationByCurrency':
        netliq = (msg.value)
        print(netliq)
        
        perc = ((float(netliq) / 1000052) - 1  ) * 100
        print ('percent change!!')
        print (perc)
        scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

        creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)

        gc = gspread.authorize(creds)

        # Call the Sheets API
        wks = gc.open_by_key('1lty76K58tiFL0f00V2jXGtCqgJLc7O7vMKa4-4tPSTE').sheet1
        if len(boughts) != 0:
            bought = ""
            for b in boughts:
                bought = bought + b + ","
            row = [str(datetime.now().strftime('%d-%m-%y %H:%M:%S')), (perc), str(datetime.now().strftime('%d-%m-%y %H:%M:%S'))]
            boughts = []
        elif len(solds) != 0:
        
            sold = ""
            for b in solds:
                sold = sold + b + ","
            row = [str(datetime.now().strftime('%d-%m-%y %H:%M:%S')), (perc), "", str(datetime.now().strftime('%d-%m-%y %H:%M:%S'))]
            solds = []
        else:
            row = [(datetime.now().strftime('%d-%m-%y %H:%M:%S')), (perc)]
        wks.append_row(row)
    if msg.typeName == 'accountSummary' and msg.account == 'DU1531456' and msg.tag == 'CashBalance':
        cashbal = (msg.value)
    if msg.typeName == 'nextValidId':
        oid = msg.orderId
        print ('oid')
        print (oid)
    #print ("Server Response: %s, %s" % (msg.typeName, msg))
def create_contract(symbol, sec_type, exch, prim_exch, curr):
    """Create a Contract object defining what will
    be purchased, at which exchange and in which currency.

    symbol - The ticker symbol for the contract
    sec_type - The security type for the contract ('STK' is 'stock')
    exch - The exchange to carry out the contract on
    prim_exch - The primary exchange to carry out the contract on
    curr - The currency in which to purchase the contract"""
    contract = Contract()
    contract.m_symbol = symbol
    contract.m_secType = sec_type
    contract.m_exchange = exch
    contract.m_primaryExch = prim_exch
    contract.m_currency = curr
    return contract

def create_order(order_type, quantity, action):
    """Create an Order object (Market/Limit) to go long/short.

    order_type - 'MKT', 'LMT' for Market or Limit orders
    quantity - Integral number of assets to order
    action - 'BUY' or 'SELL'"""
    order = Order()
    order.m_orderType = order_type
    order.m_totalQuantity = quantity
    order.m_action = action
    return order
def acct_update(msg):
    print(msg)
tws_conn = Connection.create(port=4001, clientId=1)

tws_conn.connect()
# Assign the error handling function defined above
# to the TWS connection
tws_conn.register(error_handler, 'Error')

# Assign all of the server reply messages to the
# reply_handler function defined above

def price_handler(msg):
    if msg.field == 1:
        print("bid price = %s" % msg.price)
    elif msg.field == 2:
        print("ask price = %s" % msg.price)
tws_conn.registerAll(reply_handler)
tws_conn.reqMarketDataType( 3 )

tws_conn.register(price_handler, message.tickPrice)
tws_conn.register(acct_update,
             message.updateAccountValue,
             message.updateAccountTime,
             message.updatePortfolio)
          

times = 120  
perc = ((float(netliq) / 1396001.4524) - 1  ) * 100
print ('percent change!!')
print (perc)
times = times + 1
creds = None
# The file token.pickle stores the user's access and refresh tokens, and is
# created automatically when the authorization flow completes for the first
# time.
import os
import pickle
boughts = []
solds = []
while True:
    try:
        tws_conn.reqAccountSummary(True, 'All', '$LEDGER')


        

        URL = "http://www.insider-sleuth.com/insider/screener"
        session = requests.session()
        r1 = session.get(URL)
        soup1 = BeautifulSoup(r1.text, "html.parser")

        value1 = soup1.find('input', {'name': 'csrfmiddlewaretoken'}).get('value')
        print(value1)
        # BDay is business day, not birthday...
        from pandas.tseries.offsets import BDay

        # pd.datetime is an alias for datetime
        today = pd.datetime.today()
        bday2 = today - BDay(2)
        today = today.strftime('%m-%d-%Y')
        bday2 = bday2.strftime('%m-%d-%Y')
        today = str(today).replace('-', '%2F')
        bday2 = str(bday2).replace('-', '%2F')
        print(today)
        print(bday2)
        data = "csrfmiddlewaretoken=" + value1 + "&startDate=" + bday2 + "&endDate=" + today + "&symbol=&min_val=&max_val=&aq_disp=Both&sorting=Date&officerTitle=+&isOfficer=+&isDirector=+"

        params = parse_qs(data)
        data2 = dict(params)
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.90 Safari/537.36'}

        r = session.post(URL, data=data2, headers=headers)
        soup = BeautifulSoup(r.text, "html.parser")
        l = []
        table = soup.find('table')
        table_rows = table.find_all('tr')


        for tr in table_rows:
            td = tr.find_all('td')
            row = [tr.text for tr in td]
            l.append(row)
        df = pd.DataFrame(l, columns=["Date", "Issuer Name", "Issuer Trading Symbol", "Rpt Owner Name", "Officer Title", "Acquired Disposed Code", "Transaction Shares", "Price per Share", "Value Transacted"])
        df['Date'] = df['Date'].apply(pd.to_datetime)
        df = df.set_index(df['Date'])
        per = df['Date'].dt.to_period("D")
        agg = df.groupby([per])
        dfile = 'rg_unificado.csv'
        dfs = {}
        done = []
        adja = {}
        adjd = {}
        sym = {}
        value = {}
        aord = {}
        price = {}
        for day, group in agg:
            #this simple save the data
            datep =  str(day)
            filename = '%s_%s.csv' % (dfile.replace('.csv', ''), datep)
            group.to_csv(filename, sep=',', quotechar='"', encoding='latin-1', index=False, header=True)
            dfs[filename] = pd.read_csv(filename) 
            sym[str(day)] = dfs[filename]['Issuer Trading Symbol'].values
            value[str(day)] = dfs[filename]['Value Transacted'].values
            aord[str(day)] = dfs[filename]['Acquired Disposed Code'].values
            price[str(day)] = dfs[filename]["Price per Share"].values
            acquired = {}
            disposed = {}
        for day in sym:
            for s in sym[day]:
                if s != 'DHCP'  and s != 'GXP'and s != 'LBC'  and s != 'FMSA'and s != 'COTV'and s != 'OMED' and s !='ANCX'and s !='CCT'and s != 'HEI, HEI.A'and s !=  'IDTI' and s != 'P' and s != 'TSRO'and s != 'IVTY'and s != 'FMI'and s != 'ECYT' and  s != 'BLMT' and  s != 'ipas' and  s != '(CALX)' and  s != '(SIRI)' and  s != 'QTM' and  s != 'IMDZ':
                   
                    acquired[s] = {'v': 0, 'c': 0}
                    disposed[s] = {'v': 0, 'c': 0}
        for day in sym:
            count = 0
            
            for s in sym[day]:
                if s != 'DHCP' and s != 'GXP' and s != 'LBC' and s != 'FMSA'and s != 'COTV'and s != 'OMED' and s !='ANCX'and s !='CCT'and s != 'HEI, HEI.A'and s !=  'IDTI' and s != 'P' and s != 'TSRO'and s != 'IVTY'and s != 'FMI'and s != 'ECYT' and  s != 'BLMT' and  s != 'ipas' and  s != '(CALX)' and  s != '(SIRI)' and  s != 'QTM' and  s != 'IMDZ':
                    
                    if aord[day][count] == "A":
                        acquired[s] = {'date': datep, 'price': price[day][count], 'c':  acquired[s]['c'] + 1, 'v': acquired[s]['v'] + float(value[day][count])}
                        
                        if s == 'OPK':
                            print('OPK')
                            print(acquired[s])
                    else:
                        disposed[s] = {'date': datep, 'price': price[day][count], 'c':  disposed[s]['c'] + 1,  'v': disposed[s]['v'] + float(value[day][count])}
                count = count + 1
        ta = 0
        ca = 0
        td = 0
        cd = 0
        for a in acquired:
            ta = ta + acquired[a]['v']
            ca = ca + 1
        for d in disposed:
            td = td + disposed[d]['v']
            cd = cd + 1
        aa = ta / ca
        ad = td / cd 
        print(ad)
        print(aa)
        for a in acquired:
            if acquired[a]['v'] > aa * 6 or acquired[a]['c'] > 2:
                print(acquired[a])
                if (a) == 'OPK':
                    print ('ack opk')
                adja[a] = {'date': acquired[a]['date'], 'price': acquired[a]['price'], 'c': acquired[a]['c'], 'v': acquired[a]['v']}
                
        for d in disposed:
            if disposed[d]['v'] > ad * 6 or disposed[d]['c'] > 2:
                print(disposed[d])
                adjd[d] = {'date': disposed[a]['date'], 'price': disposed[d]['price'],'c': disposed[d]['c'], 'v': disposed[d]['v']}
        print (adjd)
        print (adja)
        for a in adja:
            print(adja[a]['v'])
        for d in adjd:
            print(str(adjd[d]['v']))


        for d in adjd:
            if d not in done:
                done.append(d)

        for a in adja:
            if a not in done:
                done.append(a)
        print(done)
        print(oid)
        tick = 0
        print (adjd)
        print (adja)
        from yahoofinancials import YahooFinancials
        if times >= 0:#600:
            times = 0
                
            for d in adjd:
                if d + ':' + str(adjd[d]['c']) not in orders:
                    print('adjd')
                    print(adjd)
                    # Create an order ID which is 'global' for this session. This
                    # will need incrementing once new orders are submitted.
                    order_id = oid

                    # Create a contract in GOOG stock via SMART order routing
                    goog_contract = create_contract(d.split(':')[0], 'STK', 'SMART', 'SMART', 'USD')
                    yahoo_financials = YahooFinancials(d.split(':')[0])
                    p = yahoo_financials.get_stock_price_data()
                    

                    p= (p[d.split(':')[0]]['regularMarketPrice'])
                    # Go long 100 shares of Google
                    amt = int(float(cashbal)) / 200 / p
                    amt = amt * int(str(adjd[d]['c']))
                    amt = int(amt)
                    goog_order = create_order('MKT', amt, 'BUY')
                    goog_order.m_account = 'DU1531456'
                    goog_order.m_tif = 'GTC'
                    tws_conn.placeOrder(order_id, goog_contract, goog_order)
                    oid = oid + 1
                    order_id = order_id + 1
                    boughts.append(d.split(':')[0])
                    
                    goog_order2 = create_order('TRAIL', amt, 'SELL')
                    goog_order2.m_trailingPercent = 5
                    goog_order2.m_account = 'DU1531456'
                    goog_order2.m_tif = 'GTC'
                    tws_conn.placeOrder(order_id, goog_contract, goog_order2)
                    orders.append(d + ':' + str(adjd[d]['c']) + ':' + str(adjd[d]['date']))
                    with open('orders.txt', 'w') as f:
                        for item in orders:
                            f.write("%s\n" % item)

                    # Use the connection to the send the order to IB
                    oid = oid + 1
            for d in adja:
                if d + ':' + str(adja[d]['c']) not in orders:
                    print('adja')
                    print(adja)
                    # Create an order ID which is 'global' for this session. This
                    # will need incrementing once new orders are submitted.
                    order_id = oid

                    # Create a contract in GOOG stock via SMART order routing
                    goog_contract = create_contract(d.split(':')[0], 'STK', 'SMART', 'SMART', 'USD')
                    yahoo_financials = YahooFinancials(d.split(':')[0])
                    p = yahoo_financials.get_stock_price_data()
                    

                    p= (p[d.split(':')[0]]['regularMarketPrice'])
                    # Go long 100 shares of Google
                    amt = int(float(cashbal)) / 200 / p
                    amt = amt * int(str(adja[d]['c']))
                    amt = int(amt)
                    goog_order = create_order('MKT', amt, 'SELL')
                    goog_order.m_tif = 'GTC'
                    goog_order.m_account = 'DU1531456'
                    tws_conn.placeOrder(order_id, goog_contract, goog_order)
                    oid = oid + 1
                    order_id = order_id + 1
                    goog_order2 = create_order('TRAIL', amt, 'BUY')
                    goog_order2.m_tif = 'GTC'
                    goog_order2.m_trailingPercent = 5  
                    goog_order2.m_account = 'DU1531456'
                    tws_conn.placeOrder(order_id, goog_contract, goog_order2)
                    solds.append(d.split(':')[0])
                    orders.append(d + ':' + str(adja[d]['c']) + ':' + str(adja[d]['date']))
                    with open('orders.txt', 'w') as f:
                        for item in orders:
                            f.write("%s\n" % item)
                    # Use the connection to the send the order to IB
                    oid = oid + 1
    except Exception as e:  
        print (e)
        times = times + 1   

    time.sleep(60 * 15)