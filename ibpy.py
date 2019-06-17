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
from ib.opt import Connection, message
def error_handler(msg):
    """Handles the capturing of error messages"""
    print("Server Error: %s" % msg)
oid = 0
def reply_handler(msg):
    global oid
    global cashbal
    """Handles of server replies"""
    if msg.typeName == 'accountSummary' and msg.account == 'DUC00074' and msg.tag == 'CashBalance':
        cashbal = (msg.value)
    if msg.typeName == 'nextValidId':
        oid = msg.orderId
        print ('oid')
        print (oid)
    print ("Server Response: %s, %s" % (msg.typeName, msg))
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
             
             
while True:
    tws_conn.reqAccountSummary(True, 'All', '$LEDGER')




    URL = "http://www.insider-sleuth.com/insider/screener"
    session = requests.session()
    r1 = session.get(URL)
    soup1 = BeautifulSoup(r1.text, "html.parser")

    value1 = soup1.find('input', {'name': 'csrfmiddlewaretoken'}).get('value')
    print(value1)
    # BDay is business day, not birthday...
    from pandas.tseries.offsets import BDay

    # pd.datetime is an alias for datetime.datetime
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
    for day, group in agg:
        #this simple save the data
        datep =  str(day)
        filename = '%s_%s.csv' % (dfile.replace('.csv', ''), datep)
        group.to_csv(filename, sep=',', quotechar='"', encoding='latin-1', index=False, header=True)
        dfs[filename] = pd.read_csv(filename) 
        sym = dfs[filename]['Issuer Trading Symbol'].values
        value = dfs[filename]['Value Transacted'].values
        aord = dfs[filename]['Acquired Disposed Code'].values
        price = dfs[filename]["Price per Share"].values
        acquired = {}
        disposed = {}
        count = 0
        for s in sym:
            if s != 'DHCP'  and s != 'GXP'and s != 'LBC'  and s != 'FMSA'and s != 'COTV'and s != 'OMED' and s !='ANCX'and s !='CCT'and s != 'HEI, HEI.A'and s !=  'IDTI' and s != 'P' and s != 'TSRO'and s != 'IVTY'and s != 'FMI'and s != 'ECYT' and  s != 'BLMT' and  s != 'ipas' and  s != '(CALX)' and  s != '(SIRI)' and  s != 'QTM' and  s != 'IMDZ':

                acquired[s + ':' + datep] = {'v': 0, 'c': 0}
                disposed[s + ':' + datep] = {'v': 0, 'c': 0}
        for s in sym:
            if s != 'DHCP' and s != 'GXP' and s != 'LBC' and s != 'FMSA'and s != 'COTV'and s != 'OMED' and s !='ANCX'and s !='CCT'and s != 'HEI, HEI.A'and s !=  'IDTI' and s != 'P' and s != 'TSRO'and s != 'IVTY'and s != 'FMI'and s != 'ECYT' and  s != 'BLMT' and  s != 'ipas' and  s != '(CALX)' and  s != '(SIRI)' and  s != 'QTM' and  s != 'IMDZ':

                if aord[count] == "A":
                    if float(value[count]) is not 0:
                        acquired[s + ':' + datep] = {'price': price[count], 'c':  acquired[s + ':' + datep]['c'] + 1, 'v': acquired[s + ':' + datep]['v'] + float(value[count])}
                else:
                    if float(value[count]) is not 0:
                        disposed[s + ':' + datep] = {'price': price[count], 'c':  disposed[s + ':' + datep]['c'] + 1,  'v': disposed[s + ':' + datep]['v'] + float(value[count])}
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
        for a in acquired:
            if acquired[a]['v'] > aa * 2 and acquired[a]['c'] > 1:
                adja[a] = {'price': acquired[a]['price'], 'c': acquired[a]['c'], 'v': acquired[a]['v']}
                
        for d in disposed:
            if disposed[d]['v'] > ad * 2 and disposed[d]['c'] > 1:
                adjd[d] = {'price': disposed[d]['price'],'c': disposed[d]['c'], 'v': disposed[d]['v']}
    for a in adja:
        print(adja[a]['c'])
    for d in adjd:
        print(adjd[d]['c'])


    for d in adjd:
        if d not in done:
            done.append(d)

    for a in adja:
        if a not in done:
            done.append(a)
    print(done)
    print(oid)
    orders = []
    tick = 0
    from yahoofinancials import YahooFinancials
    for d in adjd:
        if d not in orders:
            # Create an order ID which is 'global' for this session. This
            # will need incrementing once new orders are submitted.
            order_id = oid

            # Create a contract in GOOG stock via SMART order routing
            goog_contract = create_contract(d.split(':')[0], 'STK', 'SMART', 'SMART', 'USD')
            yahoo_financials = YahooFinancials(d.split(':')[0])
            p = yahoo_financials.get_stock_price_data()
            

            p= (p[d.split(':')[0]]['regularMarketPrice'])
            # Go long 100 shares of Google
            amt = int(float(cashbal)) / 1000 / p
            amt = int(amt)
            goog_order = create_order('TRAIL', amt, 'SELL')
            goog_order.m_training_stop_percent = 0.1  
            goog_order.m_account = 'DU1531456'
            orders.append(d)
            # Use the connection to the send the order to IB
            tws_conn.placeOrder(order_id, goog_contract, goog_order)
            oid = oid + 1
    for d in adja:
        if d not in orders:
            # Create an order ID which is 'global' for this session. This
            # will need incrementing once new orders are submitted.
            order_id = oid

            # Create a contract in GOOG stock via SMART order routing
            goog_contract = create_contract(d.split(':')[0], 'STK', 'SMART', 'SMART', 'USD')
            yahoo_financials = YahooFinancials(d.split(':')[0])
            p = yahoo_financials.get_stock_price_data()
            

            p= (p[d.split(':')[0]]['regularMarketPrice'])
            # Go long 100 shares of Google
            amt = int(float(cashbal)) / 1000 / p
            amt = int(amt)
            goog_order = create_order('TRAIL', amt, 'BUY')
            goog_order.m_training_stop_percent = 0.1  
            goog_order.m_account = 'DU1531456'
            orders.append(d)
            # Use the connection to the send the order to IB
            tws_conn.placeOrder(order_id, goog_contract, goog_order)
            oid = oid + 1
    time.sleep(60*4)