from datetime import datetime
from datetime import time

import backtrader as bt
import backtrader.indicators as btind
import mariadb
import pandas as pd


# A rule to open a position early in the day if MA crossed premarket
# Also I can experiment with closing in an extended session without stop losses in the postmarket

"""
class CustomWMA(bt.Indicator):
    alias = ('WMA', 'CustomWMA')
    lines = ('wma',)
    params = (('period', 110),)

    def __init__(self):
        self.addminperiod(self.p.period)

    def next(self):
        period = self.p.period

        prices = np.array(self.data.get(size=period, ago=0))[::-1]

        weights = (period - np.arange(period)) * period

        # Calculating WMA
        total_weight = np.sum(weights)
        weighted_sum = np.sum(prices * weights)
        self.lines.wma[0] = weighted_sum / total_weight
"""

def debug(msg, enabled=True):
    if enabled:
        print(msg)

# noinspection PyArgumentList
class MyStrategy(bt.Strategy):
    params = dict(
        debug = False,
        show_signals=False,
        sma_period=10,
        wma_period=110,
        stop_loss=0.99,  # stop-loss 1%
        limit_valid_bars=3  # cancel limit orders if not filled after N bars
    )

    def __init__(self):
        # Declaring indicators
        self.sma = btind.MovingAverageSimple(self.data.close, period=self.p.sma_period)
        self.wma = btind.WMA(self.data.close, period=self.p.wma_period)

        # For tracking order and entry price
        self.order = None
        self.entry_price = None
        self.order_submit_bar = None  # bar index when the order was placed

    def next(self):
        # Get current time
        current_time = self.data.datetime.time()

        market_open = time(hour = 9, minute = 30, second = 0)
        market_close = time(hour = 16, minute = 00, second = 0)

        if self.order:
            # If the order is still active and hasn't filled yet,
            # check how many bars have passed since submission
            bars_alive = (len(self) - self.order_submit_bar)
            if bars_alive > self.p.limit_valid_bars:
                # Cancel the limit order if it hasn't been filled after N bars
                self.cancel(self.order)
                if self.p.show_signals:
                    debug(f"Canceling limit order at {self.datetime.datetime(0)} after {bars_alive} bars.", self.p.debug)
                self.order = None
                return  # Don't place new orders in the same bar

            # 2) If we have an order in process, let notify_order handle it
        if self.order:
            return

        if round(self.data.close[0],2) == round(self.data.open[0],2):
            return

        # Is there a position in the market?
        in_position = (self.position.size != 0)

        # Get the current values of the indicators
        sma = round(self.sma[0], 3)
        sma_prev = round(self.sma[-1], 3)
        wma = round(self.wma[0], 3)
        wma_prev = round(self.wma[-1], 3)

        # Entry logic
        if not in_position:
            if not (market_open <= current_time <= market_close):
                return  # Skip if not in the main trading session
            #print(f"Check not in position, date: {self.datetime.datetime(0)}  sma: {sma}, wma: {wma}, open: {self.data.open[0]}, close: {self.data.close[0]}, high: {self.data.high[0]}, low: {self.data.low[0]}")
            cond_cross_up = (sma_prev < wma_prev) and (sma - wma > 0.0)

            if cond_cross_up:
                debug(f"Entry: {self.datetime.datetime(0)}", self.p.show_signals)
                debug(
                    f"Entry params: sma: {sma}, wma: {wma}, sma_prev: {sma_prev}, wma_prev: {wma_prev}, open: {self.data.open[0]}, close: {self.data.close[0]}, high: {self.data.high[0]}, low: {self.data.low[0]}",
                    self.p.debug)
                limit_price = self.data.close[0]

                # Submit a limit BUY order
                self.order = self.buy(
                    exectype=bt.Order.Limit,
                    price=limit_price
                )
                self.entry_price = limit_price
                self.order_submit_bar = len(self)

        else:
            # If we are in a position, check for exit conditions
            #print(f"Check in position, date: {self.datetime.datetime(0)}  sma: {sma}, wma: {wma}, sma_prev: {sma_prev}, wma_prev: {wma_prev}, open: {self.data.open[0]}, close: {self.data.close[0]}, high: {self.data.high[0]}, low: {self.data.low[0]}")
            cond_cross_down = (sma_prev >= wma_prev) and (sma < wma)
            cond_stop = (self.data.close[0] < self.entry_price * self.p.stop_loss)

            if cond_cross_down and not cond_stop:
                debug(f"Exit: {self.datetime.datetime(0)}", self.p.show_signals)
                self.order = self.close()  # Закрываем позицию
                self.entry_price = None

            if cond_stop and not cond_cross_down:
                debug(f"Stop loss: {self.datetime.datetime(0)}",self.p.show_signals)
                self.order = self.close()  # Закрываем позицию
                self.entry_price = None

    def notify_order(self, order):
        """Called when the order changes status."""
        debug("=== Execution Debug ===", self.p.debug)
        debug(f"Bar datetime: {self.data.datetime.datetime(0)}", self.p.debug)
        debug(f"Bar open: {self.data.open[0]}, Bar high: {self.data.high[0]}, Bar low: {self.data.low[0]}, Bar close: {self.data.close[0]}", self.p.debug)
        debug(f"""
              Order size: {order.created.size}, 
              Order price: {order.created.price}, 
              Order status: {order.getstatusname()}, 
              Order type: {order.getordername()}, 
              Execution type: {order.exectype},
              Cash available: {self.broker.get_cash()}""", self.p.debug)
        debug("=======================", self.p.debug)
        if order.status in [order.Completed]:
            self.order = None  # Reset the order  reference
        if order.status == order.Margin:
            debug(f"Margin Error: Entry price={self.entry_price}, Current price={self.data.close[0]}, Cash={self.broker.get_cash()}", self.p.debug)
            self.order = None


    #def notify_trade(self, trade):
    #   """Called when the trade is changed the status""" ini 762, next 765
    #   print(f" trade size {trade.size} trade value {trade.value}")
    #   if trade.isclosed:
    #      print(f"entry: {trade.open_datetime()}, exit: {trade.close_datetime()} trade return: {trade.pnl}")

class AllInSizer(bt.Sizer):
    def _getsizing(self, comminfo, cash, data, isbuy):
        size = int(cash / data.close[0])
        current_dt = data.datetime.datetime(0)
        #print(f"sizer cash: {cash}, date: {current_dt}  close: {data.close[0]}, isbuy: {isbuy}, size: {size}")
        return size if size > 0 else 0

def fetch_historical_data(symbol="OKLO"):
    try:
        conn = mariadb.connect(
            user="root",
            password="password",
            host="127.0.0.1",
            port=3306,
            database="analysis"
        )
        cursor = conn.cursor()

        # SQL-запрос
        query = """
        SELECT datetime, open, high, low, close, volume
        FROM historical_data WHERE symbol = ?
        ORDER BY datetime
        """

        query = query.replace("?", f"'{symbol}'")

        # load data into DataFrame
        df = pd.read_sql(query, conn)

        # close connection
        cursor.close()
        conn.close()

        # Convert datetime column to datetime format
        df["datetime"] = pd.to_datetime(df["datetime"], format="%Y%m%d %H:%M:%S")

        return df

    except mariadb.Error as e:
        print(f"DB error: {e}")
        return None

symbol = "RGTI"

# 1. Create a cerebro engine
cerebro = bt.Cerebro()

# 2. Set the cash deposit
cerebro.broker.set_cash(7000)

# 3. Loading the DataFrame
df = fetch_historical_data(symbol)

# Creating the data feed
# noinspection PyArgumentList
datafeed = bt.feeds.PandasData(dataname=df,
                               timeframe=bt.TimeFrame.Minutes,
                               datetime=0, open=1, high=2, low=3, close=4, volume=5, openinterest=-1)

# 4. Add the data feed to cerebro
cerebro.adddata(datafeed)

# 5.Add the strategy to cerebro
cerebro.addstrategy(MyStrategy,
                    debug=False,
                    show_signals=False,
                    sma_period=10,
                    wma_period=110,
                    stop_loss=0.99  # stop-loss 1%
)

# 6. Run the backtest
cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="ta")
cerebro.addsizer(AllInSizer)

results = cerebro.run()

ta = results[0].analyzers.ta.get_analysis()

wins = ta.won.total
losses = ta.lost.total

print(f"Asset: {symbol}")
print(f"Initial deposit: {cerebro.broker.startingcash}")
print(f"Final deposit: {cerebro.broker.getvalue():.2f}")
print(f"Profitability: {((cerebro.broker.getvalue() / cerebro.broker.startingcash) - 1):.2%}")
print(f"Overall trades: {ta.total.closed}")
print(f"Longest losses streak: {ta.streak.lost.longest}")
print(f"Risk/reward: {ta.won.pnl.average/(-1 * ta.lost.pnl.average):.2}")
print(f"Winrate: {wins/(wins+losses):.2%}")