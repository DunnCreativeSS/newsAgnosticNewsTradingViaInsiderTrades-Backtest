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
        if s != 'DHCP' and s != 'FMSA'and s != 'COTV' and s !='ANCX'and s !='CCT'and s != 'HEI, HEI.A'and s !=  'IDTI' and s != 'P' and s != 'TSRO'and s != 'IVTY'and s != 'FMI'and s != 'ECYT' and  s != 'BLMT' and  s != 'ipas' and  s != '(CALX)' and  s != '(SIRI)' and  s != 'QTM' and  s != 'IMDZ':

            acquired[s + ':' + datep] = {'v': 0, 'c': 0}
            disposed[s + ':' + datep] = {'v': 0, 'c': 0}
    for s in sym:
        if s != 'DHCP' and s != 'FMSA'and s != 'COTV' and s !='ANCX'and s !='CCT'and s != 'HEI, HEI.A'and s !=  'IDTI' and s != 'P' and s != 'TSRO'and s != 'IVTY'and s != 'FMI'and s != 'ECYT' and  s != 'BLMT' and  s != 'ipas' and  s != '(CALX)' and  s != '(SIRI)' and  s != 'QTM' and  s != 'IMDZ':

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
        if acquired[a]['v'] > aa * 4 and acquired[a]['c'] > 3:
            adja[a] = {'price': acquired[a]['price'], 'c': acquired[a]['c'], 'v': acquired[a]['v']}
            
    for d in disposed:
        if disposed[d]['v'] > ad * 4 and disposed[d]['c'] > 3:
            adjd[d] = {'price': disposed[d]['price'],'c': disposed[d]['c'], 'v': disposed[d]['v']}
for a in adja:
    print(adja[a]['c'])
for d in adjd:
    print(adjd[d]['c'])
class Strategy(bt.SignalStrategy):
    def __init__(self):
        self.index = 0
    def next(self):
        self.index = self.index + 1
        if self.index > 0:
            curdate = str(self.datetime.date(ago=0))
            print(curdate)
            print(cerebro.broker.getcash())
            for da in self.getdatanames():
                #self.getdatabyname(da)
                for a in adja:
                    if a.split(':')[0] == da:
                        if curdate == a.split(':')[1]:
                            print(adja[a]['c'])
                            print(((float(cerebro.broker.getcash()) / 100)/(float(adja[a]['c'])*float(adja[a]['price']))/(1/float(adja[a]['c']))) * float(adja[a]['c']))
                            self.buy(size=(((float(cerebro.broker.getcash()) / 100)/(float(adja[a]['c'])*float(adja[a]['price']))/(1/float(adja[a]['c']))) * float(adja[a]['c'])),data=self.getdatabyname(da), exectype=backtrader.Order.StopTrail, trailpercent=0.075)
                for d in adjd:
                    if d.split(':')[0] == da:
                        if curdate == d.split(':')[1]:
                            print(adjd[d]['c'])
                            print(((float(cerebro.broker.getcash()) / 100)/(float(adjd[d]['c'])*float(adjd[d]['price']))/(1/float(adjd[d]['c']))) * float(adjd[d]['c']))
                            self.sell(size=(((float(cerebro.broker.getcash()) / 100)/(float(adjd[d]['c'])*float(adjd[d]['price']))/(1/float(adjd[d]['c']))) * float(adjd[d]['c'])),data=self.getdatabyname(da), exectype=backtrader.Order.StopTrail, trailpercent=0.075)

cerebro = bt.Cerebro()
cerebro.addstrategy(Strategy)

for d in adjd:
    if d not in done:
        done.append(d)

for a in adja:
    if a not in done:
        done.append(a)
datadata = {}
for d in done:
    datadata[d] = bt.feeds.YahooFinanceData(dataname=d.split(':')[0], fromdate=datetime(2017, 1, 1),
                                  todate=datetime(2019, 6, 6))
for d in datadata:
    cerebro.adddata(datadata[d])
cerebro.broker.setcash(10000.0)
start = cerebro.broker.getvalue()
cerebro.run()
print('Start portfolio value: %.2f' % start)
print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

