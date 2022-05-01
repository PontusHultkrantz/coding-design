import websocket
import bitmex_websocket as bm_ws
from bitmex_websocket import Instrument
from bitmex_websocket.constants import InstrumentChannels
from threading import Thread
import time
import pickle

class BitmexSubscriber:

    def __init__(self):
        self.subscriptions = {}

        self._message_handlers = {'trade': self._log_trade, 'quote': self._log_quote}

    def ingest_data(self, filename, data_dict):
        print('dumping')
        with open(f'{filename}.pkl', 'ab') as f:
            pickle.dump(data_dict, f, pickle.HIGHEST_PROTOCOL)

    def _log_trade(self, msg):
        #print(msg)
        for item in msg['data']:
            self.ingest_data(f"{item['symbol']}-trades", item)

    def _log_quote(self, msg):
        print(msg)
        for item in msg['data']:
            self.ingest_data(f"{item['symbol']}-quotes", item)

    def on_message(self, msg):
        table = msg['table']
        handler = self._message_handlers[table]
        handler(msg)

    def subscribe(self, symbol, channels):

        instrument = bm_ws.Instrument(symbol=symbol, channels=channels)
        instrument.on('action', lambda msg: self.on_message(msg))
        thread = Thread(target=instrument.run_forever)
        self.subscriptions.update({symbol: {'channels': channels, 'thread': thread}})
        thread.start()


def bitmex_subscribe():

    websocket.enableTrace(True)

    channels = [
        InstrumentChannels.quote,
        InstrumentChannels.trade
    ]

    subscriber = BitmexSubscriber()
    subscriber.subscribe('XBTUSD', channels)

bitmex_subscribe()