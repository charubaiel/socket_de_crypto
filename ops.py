import os 
import requests as r 
import numpy as np
from dagster import op,Out
import pandas as pd


def query(q,url='http://85.193.83.20:8123'):
    return r.post(url=url,
            data=q.encode('utf-8'),
            auth=(os.getenv('CRYPTO_ETL_USER'),
                    os.getenv('CRYPTO_ETL_PASSWORD')))


name_dict = {
  "E": 'event_time',
  "s": 'symbol',
  "t": 'trade_id',
  "p": "price",     
  "q": "quantity",       
  "b": 'buyer_order_id',
  "a": 'seller_order_id', 
  "T": 'trade_time', 
  "m": 'is_marker_maker'
}

@op(description='read_agg data',out={'du_data':Out(pd.DataFrame),
                                'trade_data':Out(pd.DataFrame),
                                'stream_consuming_files':Out(list)})
def read_stream_data():
    list_data = os.listdir('stream_data')
    list_data.sort()
    df_list = pd.DataFrame([pd.DataFrame(f'stream_data/{i}') for i in list_data[:-1]])
    du_data = df_list.query('e=="depthUpdate"').dropna(axis=1)
    trade_data = df_list.query('e!="depthUpdate"').dropna(axis=1)
    return (du_data,trade_data,list_data[:-1])


@op(description = 'du : transform and load data to clh')
def du_featurize_data(tmp:pd.DataFrame)->str:
    tmp['b_first_q']=tmp['b'].explode().dropna().apply(lambda x: x[1] if x[1]!='0.00000000' else np.nan).groupby(level=0).first().astype(float)
    tmp['a_first_q']=tmp['a'].explode().dropna().apply(lambda x: x[1] if x[1]!='0.00000000' else np.nan).groupby(level=0).first().astype(float)

    tmp['b_first_p']=tmp['b'].explode().dropna().apply(lambda x: x[0] if x[1]!='0.00000000' else np.nan).groupby(level=0).first().astype(float)
    tmp['a_first_p']=tmp['a'].explode().dropna().apply(lambda x: x[0] if x[1]!='0.00000000' else np.nan).groupby(level=0).first().astype(float)

    tmp['diff_p']=tmp[['b_first_p','a_first_p']].diff(axis=1).iloc[:,-1]
    tmp['diff_q']=tmp[['b_first_q','a_first_q']].diff(axis=1).iloc[:,-1]

    tmp['is_seller_pres_bid']=tmp['b_first_q'].diff().apply(lambda x: x if x>0 else 0)
    tmp['is_buyer_pres_bid']=tmp[['b_first_q','b_first_p']].diff().apply(lambda x: abs(x['b_first_q']) *-1  if x['b_first_p'] <0 else 0,axis=1)

    tmp['is_seller_pres_ask']=tmp['a_first_q'].diff().apply(lambda x: x if x>0 else 0)
    tmp['is_buyer_pres_ask']=tmp[['a_first_q','a_first_p']].diff().apply(lambda x: abs(x['a_first_q']) *-1  if x['a_first_p'] <0 else 0,axis=1)
    tmp.drop(['e','u','U','b','a'],axis=1,inplace=True)
    tmp=tmp.rename(columns=name_dict)
    
    sample = ','.join(tuple(tmp.apply(lambda x: str(tuple(x)),axis=1).to_list()))
    return sample


@op(description = 'trade_data : transform and load data to clh')
def trade_featurize_data(tmp:pd.DataFrame)->str:
    tmp=tmp.drop(['e','M'],axis=1)
    tmp['type'] = tmp['p'].astype(float).diff().where(lambda x: x!=0).fillna(method='ffill').mul(100).clip(-1,1).map({-1:'sell',1:'buy'})
    tmp=tmp.rename(columns=name_dict)
    
    sample = ','.join(tuple(tmp.apply(lambda x: str(tuple(x)),axis=1).to_list()))
    return sample



@op(description='post data')
def post_data_to_clh(insert_du_string_data:str,insert_trade_string_data)->str:
    db_du='crypto.du_data'
    db_trade='crypto.trade_data'
    status_du = query(f'INSERT INTO {db_du} VALUES {insert_du_string_data}').status_code
    status_trade = query(f'INSERT INTO {db_trade} VALUES {insert_trade_string_data}').status_code
    return '{status_du} | {status_trade}'

@op(description='remove used streamed files')
def del_consumed_files(stream_consuming_files:list,post_data_to_clh_status_code:str)->None:
    if post_data_to_clh_status_code == '200 | 200':
        [os.remove(f'stream_data/{file_}') for file_ in stream_consuming_files]