import copy

import pandas as pd
import MetaTrader5 as mt5


def get_net_pos(test=False):
    """
    get net position
    :param magic: see https://www.mql5.com/en/forum/263565
    :return:
    """
    # fetch position info
    # columns:
    #   type:
    #       0: Market buy order
    #       1: Market sell order
    pos = mt5.positions_get()
    if pos is None or len(pos) <= 0:
        return pd.DataFrame(columns=["volume"])

    pos = pd.DataFrame(list(pos), columns=pos[0]._asdict().keys())

    # filter position by magic
    if not test:
        pos = pos[pos['magic'] != 0]

    # transform type's value to -1 and 1
    #       -1: Market sell order
    #        1: Market buy order
    pos['type'] = pos['type'] * -2 + 1

    # transform volume
    # if volume less than 0, means 'sell' volume
    # if volume more than 0, means 'buy' volume
    # if volume equal 0, means Pairs Trading
    pos['volume'] = pos['volume'] * pos['type']

    # aggregation to net position
    net_pos = pos.groupby('symbol').agg({
        'volume': 'sum',
    })

    return net_pos


def get_ticket(test=False) -> list:
    pos = mt5.positions_get()
    if pos is None or len(pos) <= 0:
        return []

    pos = pd.DataFrame(list(pos), columns=pos[0]._asdict().keys())

    # filter position by magic
    if not test:
        pos = pos[pos['magic'] != 0]

    return list(pos["ticket"])


def get_pos(test=False):
    pos = mt5.positions_get()
    if pos is None or len(pos) <= 0:
        return pd.DataFrame(columns=["ticket", "symbol", "type", "volume"]), pd.DataFrame(columns=["volume"])

    pos = pd.DataFrame(list(pos), columns=pos[0]._asdict().keys())

    # filter position by magic
    if not test:
        pos = pos[pos['magic'] != 0]

    # transform type's value to -1 and 1
    #       -1: Market sell order
    #        1: Market buy order
    net_pos = copy.deepcopy(pos)
    net_pos['type'] = net_pos['type'] * -2 + 1

    # transform volume
    # if volume less than 0, means 'sell' volume
    # if volume more than 0, means 'buy' volume
    # if volume equal 0, means Pairs Trading
    net_pos['volume'] = net_pos['volume'] * net_pos['type']

    # aggregation to net position
    net_pos = net_pos.groupby('symbol').agg({
        'volume': 'sum',
    })

    return pos[["ticket", "symbol", "type", "volume"]], net_pos

