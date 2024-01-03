import signal
import logging
from datetime import datetime

from abc import ABC, abstractmethod
from enum import Enum
from typing import Union, Iterable

import MetaTrader5 as mt5

from .trade import Trade


class STRATEGY_STATUES(Enum):
    CLOSE = 0,
    OPEN = 1,


class MT5Quant(ABC):

    def OnInit(self) -> int: ...

    def OnDeinit(self, reason: STRATEGY_STATUES) -> None: ...

    @abstractmethod
    def OnTick(self): ...

    def __init__(self,  symbols: Union[str, Iterable] = None,
                        account=None,
                        password=None,
                        server=None,
                        magic=0,
                        slippage=88,
                        logfile=None,
                        MT5Path=None):
        # logging config
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(message)s',
            handlers=[
                logging.FileHandler(logfile),
                logging.StreamHandler()
            ] if logfile is not None else [
                logging.StreamHandler()
            ]
        )

        self.logger = logging.getLogger("MT5Quant")

        # init signal handel
        # when you plus ctrl+c in terminal, it work
        signal.signal(signal.SIGINT, self.signal_handler)

        # init STRATEGY STATUE
        self._STRATEGY_STATUE_ = STRATEGY_STATUES.OPEN

        # establish connection to the MetaTrader 5 terminal
        self.logger.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} establish connection to the MetaTrader 5 terminal")
        if MT5Path is not None:     initial_result = mt5.initialize(path=MT5Path)
        else:                       initial_result = mt5.initialize()
        if not initial_result:
            self.logger.info(f"initialize() failed, error code = {mt5.last_error()}")
            quit()

        # initial self.symbols, it's a constant var, and must be str, list or tuple
        # self.symbols: indicate the stock this strategy will trade
        if isinstance(symbols, str):
            self.symbols = (symbols,)

        elif isinstance(symbols, (list, tuple)):
            self.symbols = set(symbols)

        else:
            raise TypeError("symbols must be str, list or tuple")

        # initial account
        self.account = account
        self.password = password
        self.server = server
        authorized = mt5.login(account, password=password, server=server)
        if authorized:
            # display trading account info
            self.logger.info(mt5.account_info()._asdict())

        else:
            self.logger.info(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} failed to connect at account #{account}, error code: {mt5.last_error()}")
            quit()

        # initial trade tool
        self._MAGIC_ = magic
        self._SLIPPAGE_ = slippage
        self.trade = Trade(magic, slippage, self.logger)

    def signal_handler(self, sig, frame):
        self._STRATEGY_STATUE_ = STRATEGY_STATUES.CLOSE

    def run(self):
        init_status = self.OnInit()
        if not (init_status == 0 or init_status is None):
            return init_status

        # check STRATEGY STATUE, if open run continue, else close
        # this statue will change by ctrl+c in terminal, or may be change by other reason in future
        last_time = None
        while self._STRATEGY_STATUE_ == STRATEGY_STATUES.OPEN:
            last_tick = mt5.symbol_info_tick(self.symbols[0])
            if last_tick.time == last_time:
                continue
            last_time = last_tick.time

            # log print
            # self.logger.info(f"{self.symbols[0]}: {last_tick}")

            self.OnTick()

        self.OnDeinit(self._STRATEGY_STATUE_)

        # shut down connection to the MetaTrader 5 terminal
        mt5.shutdown()
        self.logger.info(
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} shut down connection to the MetaTrader 5 terminal")


    ###################### plug-in ######################
    from .quant_plug_in import get_net_position
    from .quant_plug_in import get_position
    #####################################################


# testing
if __name__ == '__main__':
    class A(MT5Quant):
        def __init__(self):
            super().__init__(**{
                "symbols": "GOLD#",
                "logfile": None,

                "account": 95238568,
                "password": "Tepeng560101?",
                 "server": "XMGlobal-MT5 5",
            })

        def OnTick(self):
            pass

    a = A()
    a.run()