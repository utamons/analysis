from datetime import datetime
from datetime import time

import backtrader as bt
import backtrader.indicators as btind
import mariadb
import pandas as pd

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

        # Рассчитываем WMA
        total_weight = np.sum(weights)
        weighted_sum = np.sum(prices * weights)
        self.lines.wma[0] = weighted_sum / total_weight
"""

# noinspection PyArgumentList
class MyStrategy(bt.Strategy):
    params = dict(
        show_signals=False,
        sma_period=10,
        wma_period=110,
        stop_loss=0.99,  # стоп-лосс 1% от цены входа
    )

    def __init__(self):
        # Объявляем индикаторы
        self.sma = btind.MovingAverageSimple(self.data.close, period=self.p.sma_period)
        self.wma = btind.WMA(self.data.close, period=self.p.wma_period)

        # Для отслеживания цены входа
        self.order = None
        self.entry_price = None

    def next(self):
        # Получаем текущее время бара
        current_time = self.data.datetime.time()

        market_open = time(hour = 9, minute = 30, second = 0)
        market_close = time(hour = 16, minute = 00, second = 0)


        if self.order and self.order.status == self.order.Margin:
            print("Ордер не может быть выполнен - Margin!")
            self.order = None
        elif self.order:
            print(f"Ордер в процессе выполнения - {self.order.getstatusname()}")
            return

        if round(self.data.close[0],2) == round(self.data.open[0],2):
            return

        # Есть ли уже позиция?
        in_position = (self.position.size != 0)

        # Получаем текущие значения индикаторов
        sma = round(self.sma[0], 2)
        sma_prev = round(self.sma[-1], 2)
        wma = round(self.wma[0], 2)
        wma_prev = round(self.wma[-1], 2)

        # Вход в позицию
        if not in_position:
            if not (market_open <= current_time <= market_close):
                return  # Выходим, если время не торговое
            #print(f"Check not in position, date: {self.datetime.datetime(0)}  sma: {sma}, wma: {wma}, open: {self.data.open[0]}, close: {self.data.close[0]}, high: {self.data.high[0]}, low: {self.data.low[0]}")
            cond_cross_up = (sma_prev < wma_prev) and (sma > wma)

            if cond_cross_up:
                print(f"Вход: {self.datetime.datetime(0)}") if self.p.show_signals else None
                # Покупка на весь available cash
                self.order = self.buy()
                # Зафиксируем цену входа
                self.entry_price = self.data.close[0]

        else:
            # Если в позиции — проверяем выход
            #print(f"Check in position, date: {self.datetime.datetime(0)}  sma: {sma}, wma: {wma}, sma_prev: {sma_prev}, wma_prev: {wma_prev}, open: {self.data.open[0]}, close: {self.data.close[0]}, high: {self.data.high[0]}, low: {self.data.low[0]}")
            cond_cross_down = (sma_prev >= wma_prev) and (sma < wma)
            cond_stop = (self.data.close[0] < self.entry_price * self.p.stop_loss)

            if cond_cross_down and not cond_stop:
                print(f"Выход: {self.datetime.datetime(0)}") if self.p.show_signals else None
                self.order = self.close()  # Закрываем позицию
                self.entry_price = None

            if cond_stop and not cond_cross_down:
                print(f"Stop loss: {self.datetime.datetime(0)}") if self.p.show_signals else None
                self.order = self.close()  # Закрываем позицию
                self.entry_price = None

    def notify_order(self, order):
        """Вызывается при изменении статуса ордера"""
        #print(f"Статус ордера {order.getstatusname()}")
        if order.status in [order.Completed]:
            self.order = None  # Сбрасываем ссылку на ордер


    #def notify_trade(self, trade):
    #   """Вызывается при завершении сделки (trade)"""
    #   print(f" trade size {trade.size} trade value {trade.value}")
    #   if trade.isclosed:
    #      print(f"entry: {trade.open_datetime()}, exit: {trade.close_datetime()} trade return: {trade.pnl}")

class AllInSizer(bt.Sizer):
    def _getsizing(self, comminfo, cash, data, isbuy):
        size = int(round(cash, 2) / round(data.close[0], 2)) - 2
        current_dt = data.datetime.datetime(0)
#        print(f"cash: {cash}, date: {current_dt}  close: {data.close[0]}, isbuy: {isbuy}, size: {size}")
        return size if size > 0 else 0

# Подключение к БД
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

        # Загружаем данные в DataFrame
        df = pd.read_sql(query, conn)

        # Закрываем соединение
        cursor.close()
        conn.close()

        # Преобразуем колонку datetime в формат datetime
        df["datetime"] = pd.to_datetime(df["datetime"], format="%Y%m%d %H:%M:%S")

        return df

    except mariadb.Error as e:
        print(f"Ошибка подключения к MariaDB: {e}")
        return None

# 1. Создаём объект "движка"
cerebro = bt.Cerebro()

# 2. Устанавливаем стартовый депозит
cerebro.broker.set_cash(7000)

# 3. Грузим ваш DataFrame
df = fetch_historical_data('OKLO')

# Создаём фид из df
# noinspection PyArgumentList
datafeed = bt.feeds.PandasData(dataname=df,
                               timeframe=bt.TimeFrame.Minutes,
                               datetime=0, open=1, high=2, low=3, close=4, volume=5, openinterest=-1)

# 4. Добавляем фид
cerebro.adddata(datafeed)

# 5. Добавляем стратегию
cerebro.addstrategy(MyStrategy,
    show_signals=False,
    sma_period=5,
    wma_period=107,
    stop_loss=0.99  # стоп-лосс 1%
)

# 6. Запускаем бэктест

cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="ta")
cerebro.addsizer(AllInSizer)

results = cerebro.run()

ta = results[0].analyzers.ta.get_analysis()

wins = ta.won.total
losses = ta.lost.total

print(f"Начальный депозит: {cerebro.broker.startingcash}")
print(f"Финальный депозит: {cerebro.broker.getvalue():.2f}")
print(f"Доходность: {((cerebro.broker.getvalue() / cerebro.broker.startingcash) - 1):.2%}")
print("Общее кол-во сделок:", ta.total.closed)
print(f"Максимальная серия убыточных сделок: {ta.streak.lost.longest}")
print(f"Risk/reward: {ta.won.pnl.average/(-1 * ta.lost.pnl.average):.2}")
print(f"Winrate: {wins/(wins+losses):.2%}")