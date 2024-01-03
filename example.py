from datetime import datetime

import MetaTrader5 as mt5
import pandas as pd

from mt5quant.quant import MT5Quant, STRATEGY_STATUES


class MyStrategy(MT5Quant):
    """
    If last bar is bullish candle, send buy order
    else close buy order
    """

    def __init__(self):
        # config
        super().__init__(**{
            # the main symbols
            # OnTick if run in this symbols
            # in this example, it's just like the EA you drag to chat of GOLD# in MT5
            "symbols":      "GOLD#",

            # Your account, password and server
            "account":      95238578,
            "password":     "123456",
            "server":       "XMGlobal-MT5 5",

            # magic number, it's usually set to distinguish different strategy.
            # see https://www.mql5.com/en/forum/263565
            "magic":        1000,

            # the slippage you want
            # in this example, when you sent order, if slippage is more than 88, the order may not deal.
            "slippage":     88,

            # if you don't know where your MT5 is, you can set to None
            # it will find all MT5 you run now
            # and when you do some operator in python, all MT5 will change,
            # if you open more than one MT5, just be careful this
            # so, if you have one MT5 only, you can just to set 'None' for simple.
            # example:
            #       "MT5Path": None,
            "MT5Path": r"C:\Program Files\XM Global MT5\terminal64.exe",

            # log file path, if set to None, it will print in screen.
            "logfile": None,
        })

    def OnInit(self) -> int:
        return 0

    def OnDeinit(self, reason: STRATEGY_STATUES) -> None:
        return None

    def OnTick(self):
        pos = self.get_position()
        rates = mt5.copy_rates_from("EURUSD", mt5.TIMEFRAME_H4, datetime.now(), 10)
        rates_frame = pd.DataFrame(rates)
        rates_frame['time'] = pd.to_datetime(rates_frame['time'], unit='s')
        rate = rates_frame.iloc[1]
        if pos <= 0:
            if rate["open"] < rate["close"]:
                self.trade.buy_open(symbol="GOLD#",
                                    lots=0.01,
                                    profit=0,
                                    stoploss=0,
                                    comment="b")

        else:
            if rate["open"] > rate["close"]:
                self.trade.buy_close(symbol="GOLD#",
                                     comment="b")

        # other function
        # self.trade.sell_open
        # self.trade.sell_close


if __name__ == '__main__':
    strategy = MyStrategy()
    strategy.run()

    # you can run this for debug(if run, remember to delete "strategy.run()")
    # strategy.OnTick()
