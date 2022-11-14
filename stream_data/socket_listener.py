from binance.client import Client
from datetime import datetime
import websocket, json
import pandas as pd
import ssl


cc = "gmteth"
cc1 = "gmtbnb"
cc2 = "bnbeth"
# file_path = 'data.txt' #choose your file path

############################################################

api_keys_file = open('api_keys', 'r')
api_keys = []
for line in api_keys_file:
    api_keys.append(line[:-2])

client = Client(api_keys[0], api_keys[1])
##################################################################

my_context = ssl.create_default_context()
my_context.load_verify_locations('/Library/Frameworks/Python.framework/Versions/3.9/lib/python3.9/site-packages/certifi/cacert.pem')
socket = f"wss://stream.binance.com:9443/ws/{cc}@bookTicker/{cc1}@bookTicker/{cc2}@bookTicker" #combinated streams

########################################################################





def on_message_new(ws, message):
    '''
    Пачкообразная загрузка стрима\n
    pipeline:  
    --------

    1) Принимает сообщение  
    2) Добавляет его к листу ( в буффер по сути)  
    3) Как только скапливается N сообщений со стрима сбрасывает это в файлик  
    
    '''

    json_message = json.loads(message)
    dt = datetime.now()
    json_message.setdefault('key', str(dt))

    global BUFFER_OBJECT

    if len(BUFFER_OBJECT) < 10000: # Кол-во сообщений которые он держит в буффере. CLH любит от 100к, для теста можно и меньше
        BUFFER_OBJECT.append(json_message)
    else:
        pd.DataFrame(BUFFER_OBJECT).to_json(f'stream_{dt.timestamp()}.json') # Название stream обязательное
                                                                            # тк соседний скрипт ищет именно файлики с этим именем для обработки
        BUFFER_OBJECT = []
   






def on_close(ws):
	print("### close ###")

def on_error(ws, err):
    print("Got a an error: ", err)

def on_ping(wsapp, message):
    print("Got a ping! A pong reply has already been automatically sent.")

def on_pong(wsapp, message):
    print("Got a pong! No need to respond")

if __name__ == '__main__':
    #для парсинга
    BUFFER_OBJECT = []
    ws = websocket.WebSocketApp(socket,
                                on_message=on_message_new,
                                on_close=on_close,
                                on_error=on_error,
                                on_pong=on_pong,
                                on_ping=on_ping)
    # ws.run_forever(sslopt={'context': my_context})
        
    ws.run_forever()





