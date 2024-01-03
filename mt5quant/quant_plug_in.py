import MetaTrader5 as mt5
import pandas as pd

from .position import get_net_pos, get_pos


def get_net_position(self):
    return get_net_pos(self._MAGIC_)


def get_position(self):
    return get_pos(self._MAGIC_)

