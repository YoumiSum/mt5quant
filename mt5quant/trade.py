import logging
from datetime import datetime

import MetaTrader5 as mt5
import pandas as pd

from .error import DataMissingError


class Trade:
    def __init__(self,
                 magic: int = 0,
                 slippage: int = 88,
                 logger: logging.Logger=None):
        self._MAGIC_ = magic
        self._SLIPPAGE_ = slippage
        self.logger = logger

    def buy_open(self,
                 symbol: str,
                 lots: float,
                 profit: float,
                 stoploss: float,
                 comment: str = "buy open",
                 use_point: bool = True):

        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            raise DataMissingError(f"can not find {symbol} info")

        # max volume and min volume fix
        if lots > symbol_info.volume_max:
            self.logger.warning(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Order[{symbol}] has send {lots} lots, \
                                but {symbol} only can send {symbol_info.volume_max} in max")
            return -1

        if lots < symbol_info.volume_min:
            self.logger.warning(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Order[{symbol}] has send {lots} lots, \
                                            but {symbol} only can send {symbol_info.volume_min} in minimum")
            return -2

        # fix lots' decimal places
        lots //= symbol_info.volume_min
        lots *= symbol_info.volume_min

        # profit and stoploss config
        if profit <= 0: profit = 0
        else:
            if use_point is True: profit = symbol_info.ask + profit*symbol_info.point
            else: profit = profit

        profit = float(profit)

        if stoploss <= 0: stoploss = 0
        else:
            if use_point is True: stoploss = symbol_info.ask - stoploss*symbol_info.point
            else: stoploss = stoploss

        stoploss = float(stoploss)

        # check the order rough in
        orders = mt5.orders_get()
        if not (orders is None or len(orders) <= 0):
            orders = pd.DataFrame(list(orders), columns=orders[0]._asdict().keys())

            # magic filter
            orders = orders[orders['type'] == mt5.ORDER_TYPE_BUY]
            orders = orders[orders['magic'] == self._MAGIC_]
            orders = orders[orders['comment'] == comment]

            if len(orders) > 0:
                return 0

        pos = mt5.positions_get()
        if not (pos is None or len(pos) <= 0):
            pos = pd.DataFrame(list(pos), columns=pos[0]._asdict().keys())

            pos = pos[pos['type'] == mt5.POSITION_TYPE_BUY]
            pos = pos[pos['magic'] == self._MAGIC_]
            pos = pos[pos['comment'] == comment]

            if len(pos) > 0:
                return 0

        # order send
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": lots,
            "type": mt5.ORDER_TYPE_BUY,
            "price": symbol_info.ask,
            "sl": stoploss,
            "tp": profit,
            "deviation": self._SLIPPAGE_,
            "magic": self._MAGIC_,
            "comment": comment,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        result = mt5.order_send(request)
        self.logger.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {result}")

        return result

    def sell_open(self,
                 symbol: str,
                 lots: float,
                 profit: float,
                 stoploss: float,
                 comment: str = "sell open",
                 use_point: bool = True):

        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            raise DataMissingError(f"can not find {symbol} info")

        # max volume and min volume fix
        if lots > symbol_info.volume_max:
            self.logger.warning(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Order[{symbol}] has send {lots} lots, \
                                        but {symbol} only can send {symbol_info.volume_max} in max")
            return -1

        if lots < symbol_info.volume_min:
            self.logger.warning(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Order[{symbol}] has send {lots} lots, \
                                                    but {symbol} only can send {symbol_info.volume_min} in minimum")
            return -2

        # fix lots' decimal places
        lots //= symbol_info.volume_min
        lots *= symbol_info.volume_min

        # profit and stoploss config
        if profit <= 0: profit = 0
        else:
            if use_point is True: profit = symbol_info.bid - profit * symbol_info.point
            else: profit = profit

        profit = float(profit)

        if stoploss <= 0: stoploss = 0
        else:
            if use_point is True: stoploss = symbol_info.bid + stoploss * symbol_info.point
            else: stoploss = stoploss

        stoploss = float(stoploss)

        # check the order rough in
        orders = mt5.orders_get()
        if not (orders is None or len(orders) <= 0):
            orders = pd.DataFrame(list(orders), columns=orders[0]._asdict().keys())

            # magic filter
            orders = orders[orders['type'] == mt5.ORDER_TYPE_SELL]
            orders = orders[orders['magic'] == self._MAGIC_]
            orders = orders[orders['comment'] == comment]

            if len(orders) > 0:
                return 0

        pos = mt5.positions_get()
        if not (pos is None or len(pos) <= 0):
            pos = pd.DataFrame(list(pos), columns=pos[0]._asdict().keys())

            pos = pos[pos['type'] == mt5.POSITION_TYPE_SELL]
            pos = pos[pos['magic'] == self._MAGIC_]
            pos = pos[pos['comment'] == comment]

            if len(pos) > 0:
                return 0

            # order send
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": symbol,
                "volume": lots,
                "type": mt5.ORDER_TYPE_SELL,
                "price": symbol_info.bid,
                "sl": stoploss,
                "tp": profit,
                "deviation": self._SLIPPAGE_,
                "magic": self._MAGIC_,
                "comment": comment,
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            result = mt5.order_send(request)
            self.logger.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {result}")

            return result

    def buy_close(self,
                  comment: str = None,
                  symbol: str = None,
                  fuzzy: str = False):

        pos = mt5.positions_get()
        if not (pos is None or len(pos) <= 0):
            pos = pd.DataFrame(list(pos), columns=pos[0]._asdict().keys())

            pos = pos[pos['type'] == mt5.POSITION_TYPE_BUY]
            if symbol is not None:
                pos = pos[pos['symbol'] == symbol]

            if self._MAGIC_ != 0:
                pos = pos[pos['magic'] == self._MAGIC_]

            if comment is not None:
                if fuzzy is True:
                    pos = pos[pos['comment'].str.contains(comment, case=False)]

                else:
                    pos = pos[pos['comment'] == comment]

            # close pos
            pos['retcode'] = pos.apply(self._b_close_, axis=1)

        return pos

    def _b_close_(self, item_pos):
        symbol_info = mt5.symbol_info(item_pos.symbol)
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": item_pos.symbol,
            "volume": item_pos.volume,
            "type": mt5.ORDER_TYPE_SELL,
            "position": item_pos.ticket,
            "price": symbol_info.bid,
            "deviation": self._SLIPPAGE_,
            "magic": self._MAGIC_,
            "comment": item_pos.comment + " -> close",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        result = mt5.order_send(request)
        self.logger.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {result}")
        return result.retcode

    def sell_close(self,
                  comment: str = None,
                  symbol: str = None,
                  fuzzy: str = False):

        pos = mt5.positions_get()
        if not (pos is None or len(pos) <= 0):
            pos = pd.DataFrame(list(pos), columns=pos[0]._asdict().keys())

            pos = pos[pos['type'] == mt5.POSITION_TYPE_SELL]
            if symbol is not None:
                pos = pos[pos['symbol'] == symbol]

            if self._MAGIC_ != 0:
                pos = pos[pos['magic'] == self._MAGIC_]

            if comment is not None:
                if fuzzy is True:
                    pos = pos[pos['comment'].str.contains(comment, case=False)]

                else:
                    pos = pos[pos['comment'] == comment]

            # close pos
            pos['retcode'] = pos.apply(self._s_close_, axis=1)

        return pos

    def _s_close_(self, item_pos):
        symbol_info = mt5.symbol_info(item_pos.symbol)
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": item_pos.symbol,
            "volume": item_pos.volume,
            "type": mt5.ORDER_TYPE_BUY,
            "position": item_pos.ticket,
            "price": symbol_info.ask,
            "deviation": self._SLIPPAGE_,
            "magic": self._MAGIC_,
            "comment": item_pos.comment + " -> close",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        result = mt5.order_send(request)
        self.logger.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {result}")
        return result.retcode
