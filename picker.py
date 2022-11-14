import pandas as pd
import time
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-f', default='./')
args = parser.parse_args()

filepath = args.f

def some_metric_num1(row):
    return row['a']['BNBETH'] - row['b']['GMTETH']*row['b']['BNBETH']
    
def some_metric_num2(row):
    return row['b']['BNBETH'] - row['a']['GMTETH']*row['a']['BNBETH']

# Отправляет данные куда то, по хорошему тут должен быть процессинг + отправка в кх
# Функцию заменить
def custom_metrics(filepath):
    dd = pd.read_json(filepath).astype({'key':'datetime64[ms]'})
    df = dd.groupby([pd.Grouper(key='key',freq='10s'),'s'])[['b','a']].mean().unstack()
    m1 = df.apply(some_metric_num1,axis=1).rename('metric_1')
    m2 = df.apply(some_metric_num2,axis=1).rename('metric_2')
    tmp = pd.concat([m1,m2],axis=1)

    tmp.to_csv(f'{args.f}fix_data.csv',mode='a')



if __name__ == '__main__':
# бесконечный луп по поиску нужных файликов.

    while 1==1:

        time.sleep(2)
        listdir = [i for i in os.listdir(filepath) if 'stream' in i]
        # Как только появляется файлик который не юзается основным скриптом, то мы его сразу процессим и тянем в бд
        if len(listdir)>1:
            listdir.sort()
            for i in listdir[:-1]:
                try:
                    custom_metrics(filepath+i) # Вот эта функция может быть совсем любой
                    os.remove(filepath+i) # Удаляем файлик, чтобы не жрать много места. Т.о. у нас максимум будет всегда 2 файла, а в идеале 1
                    print(f'get_{i}')
                except:
                    print('oops')
