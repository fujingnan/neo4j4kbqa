import pandas as pd
from tqdm import tqdm

datas = pd.read_csv('/Users/fujingnan/公司/xueqiu/飞书文档文件/第五版板块挖词.csv', keep_default_na=False)
datas['delete'] = datas['index_symbol']+'&'+datas['symbol']

datas.drop_duplicates('delete', inplace=True)
datas['keywords'] = datas['mining_res'] + '&' + datas['block_mining']

bn = set(datas['index_symbol'].tolist())
newdatas = pd.DataFrame(datas.loc[0, :]).T

for b in bn:
    newdatas = pd.concat([datas[datas['index_symbol']==b][:5], newdatas])

blocks = []
stocks = []
kw_block = []
kw_stock = []
keywords = []
block_stock = []
block_records = {}
kw_records = set()
records = {}
wid = 0
for _, row in tqdm(newdatas.iterrows(), total=len(newdatas)):
    tmp = {}
    tmp['bid'] = row['index_symbol']
    tmp['bname'] = row['index_name']
    blocks.append(tmp)
    tmp = {}
    tmp['sid'] = row['symbol']
    tmp['sname'] = row['name']
    stocks.append(tmp)
    tmp = {}
    tmp['sid'] = row['symbol']
    tmp['bid'] = row['index_symbol']
    block_stock.append(tmp)
    kws = list(set([str(x) for x in row['keywords'].split('&') if x]))[:10]
    for w in kws:
        if not w in records:
            kw_records.add((wid, w))
            records[w] = wid
            kw_stock.append({'sid': row['symbol'], 'wid': wid})
            block_records.setdefault(row['index_symbol'], set()).add(wid)
            wid += 1
        else:
            kw_stock.append({'sid': row['symbol'], 'wid': records[w]})
            block_records.setdefault(row['index_symbol'], set()).add(records[w])

for k, v in block_records.items():
    for wid in v:
        kw_block.append({'bid': k, 'wid': wid})
for wid, w in kw_records:
    keywords.append({'wid': wid, 'wname': w})
df1 = pd.DataFrame(blocks)
df1.drop_duplicates('bid', inplace=True)
df1.to_csv('板块.csv', index=False)

df2 = pd.DataFrame(stocks).to_csv('股票.csv', index=False)
df3 = pd.DataFrame(block_stock).to_csv('板块-股票.csv', index=False)
df4 = pd.DataFrame(kw_stock).to_csv('关键词-股票.csv', index=False)
df5 = pd.DataFrame(kw_block).to_csv('关键词-板块.csv', index=False)
df6 = pd.DataFrame(keywords).to_csv('关键词.csv', index=False)