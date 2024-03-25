import logging
from datetime import datetime

import MetaTrader5 as mt5
import pandas as pd

from mt5quant.error import DataMissingError
from mt5quant.position import get_pos


class Trade:
    def __init__(self,
                 magic: int = 0,
                 slippage: int = 88,
                 logger: logging.Logger=None):
        self._MAGIC_ = magic
        self._SLIPPAGE_ = slippage
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
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
        """
        see self.buy_close
        """

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
                  fuzzy: str = False,
                  magic: bool=True):
        """
        comment: close the buy orders that include this comment
        symbol: close the buy orders that include symbol comment
        fuzzy:  if set True, close the buy orders if these orders's COMMENT include comment(the parameter).
                for detail, it will find "*{comment}*"
                it work when comment is not set to None.
        magic: True: you will close the orders that include self._MAGIC_
                False: close all kinds of magic's orders
        """

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
            if len(pos) == 0:
                return pos
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
            if len(pos) == 0:
                return pos
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

    def s_sub(self, symbol, volume, ticket=0):
        symbol_info = mt5.symbol_info(symbol)

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(volume),
            "type": mt5.ORDER_TYPE_SELL,
            "price": symbol_info.bid,
            "deviation": self._SLIPPAGE_,
            "magic": self._MAGIC_,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        if ticket != 0:
            request["position"] = ticket

        result = mt5.order_send(request)
        if result is None:
            return 3

        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [{result.retcode}] {symbol} -> {volume}")

        return result.retcode

    def s(self, symbol, volume):
        # 获取所有的多单，并按照持仓量从小到大排序
        # 先将小的平掉，后平大的
        volume = round(volume, 2)
        positions, _ = get_pos()
        positions = positions[positions["type"] == mt5.POSITION_TYPE_BUY]
        positions = positions[positions["symbol"] == symbol]
        positions = positions.sort_values(by="volume", ascending=True)
        positions = positions.reset_index(drop=True)
        positions = positions.to_dict(orient='index')
        for key, pos in positions.items():
            if volume < 0:
                print(f"{symbol} 可能平多了，请检查仓位...")
                return 2

            if volume == 0:
                return mt5.TRADE_RETCODE_DONE

            if pos["volume"] <= volume * 1.0001:
                retcode = self.s_sub(symbol, pos['volume'], pos["ticket"])
                if retcode != mt5.TRADE_RETCODE_DONE:
                    print(f"{symbol}[{pos['volume']}] 平仓失败..., error code: {retcode}")
                    return retcode

                volume -= pos["volume"]
                volume = round(volume, 2)
                continue

            if pos["volume"] > volume * 0.9999:
                retcode = self.s_sub(symbol, volume, pos["ticket"])
                if retcode != mt5.TRADE_RETCODE_DONE:
                    print(f"{symbol}[{volume}] 平仓失败..., error code: {retcode}")

                # 已经平完了，所以直接返回，如果出异常也是直接返回
                return retcode

        if volume < 0:
            print(f"{symbol} 可能平多了，请检查仓位...")
            return 2

        if volume == 0:
            return mt5.TRADE_RETCODE_DONE

        # 开新的单子
        return self.s_sub(symbol, volume)

    def b_sub(self, symbol, volume, ticket=0):
        symbol_info = mt5.symbol_info(symbol)

        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(volume),
            "type": mt5.ORDER_TYPE_BUY,
            "price": symbol_info.ask,
            "deviation": self._SLIPPAGE_,
            "magic": self._MAGIC_,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        if ticket != 0:
            request["position"] = ticket

        result = mt5.order_send(request)
        print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [{result.retcode}] {symbol} -> {volume}")
        return result.retcode

    def b(self, symbol, volume):
        # 获取所有的空单，并按照持仓量从小到大排序
        # 先将小的平掉，后平大的
        volume = round(volume, 2)
        positions, _ = get_pos()
        positions = positions[positions["type"] == mt5.POSITION_TYPE_SELL]
        positions = positions[positions["symbol"] == symbol]
        positions = positions.sort_values(by="volume", ascending=True)
        positions = positions.reset_index(drop=True)
        positions = positions.to_dict(orient='index')
        for key, pos in positions.items():
            if volume < 0:
                print(f"{symbol} 可能平多了，请检查仓位...")
                return 2

            if volume == 0:
                return mt5.TRADE_RETCODE_DONE

            if pos["volume"] <= volume * 1.0001:
                retcode = self.b_sub(symbol, pos['volume'], pos["ticket"])
                if retcode != mt5.TRADE_RETCODE_DONE:
                    print(f"{symbol}[{pos['volume']}] 平仓失败..., error code: {retcode}")
                    return retcode

                volume -= pos['volume']
                volume = round(volume, 2)
                continue

            if pos["volume"] > volume * 0.9999:
                retcode = self.b_sub(symbol, volume, pos["ticket"])
                if retcode != mt5.TRADE_RETCODE_DONE:
                    print(f"{symbol}[{volume}] 平仓失败..., error code: {retcode}")

                # 已经平完了，所以直接返回，如果出异常也是直接返回
                return retcode

        if volume < 0:
            print(f"{symbol} 可能平多了，请检查仓位...")
            return 2

        if volume == 0:
            return mt5.TRADE_RETCODE_DONE

        # 开新的单子
        return self.b_sub(symbol, volume)

    def trade(self, symbol, volume):
        if volume == 0:
            b = self.buy_close(symbol=symbol)
            s = self.sell_close(symbol=symbol)
            res = pd.concat([b, s], axis=0)
            success = res[res["retcode"] != 10009]
            if len(success) <= 0:
                return 10009

            else:
                return success

        if volume < 0:
            return self.s(symbol, volume*-1)
        else:
            return self.b(symbol, volume)

    def set_pos(self, symbol, volume):
        _, pos = get_pos()
        try:
            lots = pos.loc[symbol].values[0]
            if 0.9999 * lots <= volume <= 1.0001 * lots:
                return 10009

            new_pos = volume - lots
            res = self.trade(symbol, new_pos)

        except:
            res = self.trade(symbol, volume)

        return res


if __name__ == '__main__':
    symbol = "USDJPY#"
    mt5config = {
        "MT5Path": r"C:\Program Files\MetaTrader 5\terminal64.exe",
        "account": 305247031,
        "password": "Yum123456.",
        "server": "XMGlobal-MT5 6",
        "magic": 1000,
        "slippage": 88,
    }
    initial_result = mt5.initialize(path=mt5config["MT5Path"])
    if not initial_result:
        print(f"initialize() failed, error code = {mt5.last_error()}")
        quit()

    # initial account
    account = mt5config["account"]
    password = mt5config["password"]
    server = mt5config["server"]
    authorized = mt5.login(mt5config["account"], password=mt5config["password"],
                           server=mt5config["server"])
    if authorized:
        # display trading account info
        print(mt5.account_info()._asdict())

    _MAGIC_ = mt5config["magic"]
    _SLIPPAGE_ = mt5config["slippage"]
    trade = Trade(mt5config["magic"], mt5config["slippage"])

    trade.trade(symbol, 0)
