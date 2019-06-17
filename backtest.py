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
class CommInfo_CFD(bt.CommInfoBase):
    params = (('stocklike', True), ('commtype', bt.CommInfoBase.COMM_FIXED),)

    def _getcommission(self, size, price, pseudoexec):
        return self.p.commission
                
cerebro = bt.Cerebro()
cerebro.broker.setcash(10000.0)
comminfo = CommInfo_CFD(commission=9.95) # 1$

cerebro.broker.addcommissioninfo(comminfo)

URL = "http://www.insider-sleuth.com/insider/screener"
session = requests.session()
r1 = session.get(URL)
soup1 = BeautifulSoup(r1.text, "html.parser")

value1 = soup1.find('input', {'name': 'csrfmiddlewaretoken'}).get('value')
print(value1)
data = "csrfmiddlewaretoken=" + value1 + "&startDate=01%2F01%2F2017&endDate=06%2F06%2F2019&symbol=&min_val=&max_val=&aq_disp=Both&sorting=Date&officerTitle=+&isOfficer=+&isDirector=+"

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
        if acquired[a]['v'] > aa * 4 and acquired[a]['c'] > 1:
            adja[a] = {'price': acquired[a]['price'], 'c': acquired[a]['c'], 'v': acquired[a]['v']}
            
    for d in disposed:
        if disposed[d]['v'] > ad * 4 and disposed[d]['c'] > 1:
            adjd[d] = {'price': disposed[d]['price'],'c': disposed[d]['c'], 'v': disposed[d]['v']}
for a in adja:
    print(adja[a]['c'])
for d in adjd:
    print(adjd[d]['c'])

class maxRiskSizer(bt.Sizer):
    '''
    Returns the number of shares rounded down that can be purchased for the
    max rish tolerance
    '''
    params = (('risk', 0.03),)

    def __init__(self):
        if self.p.risk > 1 or self.p.risk < 0:
            raise ValueError('The risk parameter is a percentage which must be'
                'entered as a float. e.g. 0.5')

    def _getsizing(self, comminfo, cash, data, isbuy):
        if isbuy == True:
            size = math.floor((cash * self.p.risk) / data[0])
        else:
            size = math.floor((cash * self.p.risk) / data[0]) * -1
        return size

class Strategy(bt.SignalStrategy):
    def __init__(self):
        self.index = 0
        self.trades = 0
    def next(self):
        self.index = self.index + 1
        if self.index > 0:
            curdate = str(self.datetime.date(ago=0))
            print(curdate)
            print(cerebro.broker.getvalue())
            if cerebro.broker.getvalue() > 0:
                
                for da in self.getdatanames():
                #self.getdatabyname(da)
                    for a in adja:
                        if a.split(':')[0] == da:
                            if curdate == a.split(':')[1]:
                                self.trades = self.trades + 9.95*0.075
                                print(self.trades)
                                self.buy(data=self.getdatabyname(da), exectype=backtrader.Order.StopTrail, trailpercent=0.05)
                    for d in adjd:
                        if d.split(':')[0] == da:
                            if curdate == d.split(':')[1]:
                                self.trades = self.trades + 9.95*0.075
                                print(self.trades)
                                self.sell(data=self.getdatabyname(da), exectype=backtrader.Order.StopTrail, trailpercent=0.05)

cerebro.addstrategy(Strategy)
cerebro.addsizer(maxRiskSizer, risk=0.05)

for d in adjd:
    if d not in done:
        done.append(d)

for a in adja:
    if a not in done:
        done.append(a)
datadata = {}
for d in done:
    datadata[d] = bt.feeds.YahooFinanceData(dataname=d.split(':')[0], fromdate=datetime(2018, 4, 1),
                                  todate=datetime(2019, 5, 31))
                                  
for d in datadata:
    cerebro.adddata(datadata[d])
    datadata[d].plotinfo.plot = False

start = cerebro.broker.getvalue()
start2 = cerebro.broker.getcash()
cerebro.addobservermulti(bt.observers.BuySell)

cerebro.run()
print('Start portfolio value: %.2f' % start)
print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
print('Start cash value: %.2f' % start2)
print('Final cash Value: %.2f' % cerebro.broker.getcash())

cerebro.plot()