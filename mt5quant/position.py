import pandas as pd
import MetaTrader5 as mt5


def get_net_pos(magic=0):
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
        return pd.DataFrame(columns=["volume", "profit", "swap"])

    pos = pd.DataFrame(list(pos), columns=pos[0]._asdict().keys())

    # filter position by magic
    if magic != 0:
        pos = pos[pos['magic'] == magic]

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
        'profit': 'sum',
        'swap': 'sum',
    })

    return net_pos


def get_pos(magic=0):
    pos = mt5.positions_get()
    if pos is None or len(pos) <= 0:
        return pd.DataFrame(columns=["volume", "profit", "swap"])

    pos = pd.DataFrame(list(pos), columns=pos[0]._asdict().keys())

    # filter position by magic
    if magic != 0:
        pos = pos[pos['magic'] == magic]

    return pos
