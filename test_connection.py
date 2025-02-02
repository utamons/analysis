from datetime import datetime

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import mariadb
from ibapi.common import BarData
from threading import Thread
import time

symbol = "RGTI"

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "password",
    "database": "analysis"
}

## SMA 10, WMA 120, WMA 400, SMA 4000

class HistoricalDataApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.data = [] # Список для хранения свечей
        self.data_received = False  # Флаг для завершения работы

    def historicalData(self, reqId, bar: BarData):
        """Получаем свечные данные"""
        ##print(f"Получены данные: {bar.date}, O:{bar.open}, H:{bar.high}, L:{bar.low}, C:{bar.close}, V:{bar.volume}")
        date_str = bar.date
        date_str_clean = " ".join(date_str.split()[:2])
        self.data.append({
            "datetime": datetime.strptime(date_str_clean, "%Y%m%d %H:%M:%S"),
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume
        })

    def historicalDataEnd(self, reqId, start, end):
        """Когда TWS сообщает, что данные загружены"""
        print("✅ Данные загружены.")
        self.store_data_in_db()  # Сохраняем данные в БД
        self.data_received = True  # Завершаем программу

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        """Вывод ошибок и сообщений от TWS API"""
        print(f"Ошибка {errorCode}: {errorString}")

    def store_data_in_db(self):
        """Записывает полученные данные в MariaDB."""
        global cursor, conn
        if not self.data:
            print("Нет данных для сохранения.")
            return

        try:
            conn = mariadb.connect(**DB_CONFIG)
            cursor = conn.cursor()

            insert_query = """
            INSERT INTO historical_data (symbol, datetime, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                open=VALUES(open), high=VALUES(high), 
                low=VALUES(low), close=VALUES(close), volume=VALUES(volume)
            """

            data_to_insert = [(symbol, row["datetime"], row["open"], row["high"], row["low"], row["close"], row["volume"]) for row in self.data]

            cursor.executemany(insert_query, data_to_insert)
            conn.commit()
            print(f"Сохранено {len(self.data)} строк в БД.")

        except mariadb.Error as err:
            print(f"Ошибка БД: {err}")

        finally:
            if 'cursor' in locals() and cursor:
                cursor.close()
            if 'conn' in locals() and conn:
                conn.close()


def run_loop(app):
    """Фоновый поток для обработки сообщений от TWS API"""
    app.run()


if __name__ == "__main__":
    app = HistoricalDataApp()
    app.connect("127.0.0.1", 7497, clientId=0)  # Подключаемся к TWS

    # Запуск фонового потока обработки сообщений
    api_thread = Thread(target=run_loop, args=(app,), daemon=True)
    api_thread.start()

    # Ждём соединения
    time.sleep(2)

    # Создаём контракт для AAPL (или любой другой акции)
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.currency = "USD"
    contract.exchange = "SMART"

    # Запрашиваем минутные свечи за 1 день
    app.reqHistoricalData(
        reqId=1,
        contract=contract,
        endDateTime="",
        durationStr="11 W",
        barSizeSetting="1 min",
        whatToShow="TRADES",
        useRTH=0,
        formatDate=1,
        keepUpToDate=False,
        chartOptions=[]
    )

    # Ждём завершения загрузки данных
    while not app.data_received:
        time.sleep(1)

    # Завершаем соединение
    app.disconnect()
