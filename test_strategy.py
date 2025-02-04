import pandas as pd
import mariadb

trades = []

# Подключение к БД
def fetch_historical_data():
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
        FROM historical_data WHERE symbol = 'RGTI'
        ORDER BY datetime ASC
        """

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

def calculate_moving_averages(df):
    # Простая скользящая средняя (SMA)
    df["SMA_10"] = df["close"].rolling(window=10).mean()
    df["SMA_4000"] = df["close"].rolling(window=4000).mean()

    # Взвешенная скользящая средняя (WMA)
    def weighted_moving_average(series, period):
        weights = range(1, period + 1)
        return series.rolling(period).apply(lambda x: (x * weights).sum() / sum(weights), raw=True)

    df["WMA_120"] = weighted_moving_average(df["close"], 110)
    df["WMA_400"] = weighted_moving_average(df["close"], 400)

    return df


def iterative_backtest(df):
    # Готовим списки для сигналов (по умолчанию False)
    entry_signal = [False] * len(df)
    exit_signal = [False] * len(df)
    entry_price = [None] * len(df)  # Цена входа для каждой строки (если вход был)

    in_position = False
    current_entry_price = None

    for i in range(2, len(df)):
        # Текущая и предыдущие строки (для удобства)
        row = df.iloc[i]
        row_1 = df.iloc[i - 1]
        row_2 = df.iloc[i - 2]

        # Извлекаем нужные значения
        sma10_t = row["SMA_10"]
        sma10_t1 = row_1["SMA_10"]
        sma10_t2 = row_2["SMA_10"]

        wma400_t = row["WMA_400"]
        wma400_t1 = row_1["WMA_400"]

        sma4000_t = row["SMA_4000"]
        sma4000_t1 = row_1["SMA_4000"]

        wma120_t = row["WMA_120"]
        wma120_t1 = row_1["WMA_120"]
        wma120_t2 = row_2["WMA_120"]

        close_t = row["close"]

        # ---- Логика входа ----
        if not in_position:
            # Проверяем условия входа
            cond_wma_vs_sma = True #(wma400_t > sma4000_t)
            cond_both_up = True #(wma400_t > wma400_t1) and (sma4000_t > sma4000_t1)
            cond_cross = (sma10_t1 < wma120_t1) and (sma10_t > wma120_t)

            if cond_wma_vs_sma and cond_both_up and cond_cross:
                in_position = True
                current_entry_price = row["close"]
                entry_signal[i] = True
                entry_price[i] = current_entry_price  # Фиксируем цену входа в эту строку
                trade = {
                    "entry_index": i,
                    "entry_time": row["datetime"],
                    "entry_price":current_entry_price
                }
                trades.append(trade)

        else:
            # ---- Логика выхода ----
            # Условие разворота SMA_10 на 3 бара
            cond_sma_down_3bars = (sma10_t2 > sma10_t1) and (sma10_t1 > sma10_t)
            cond_cross = (sma10_t1 > wma120_t1) and (sma10_t < wma120_t)

            # Условие стоп-лосса
            cond_stop_loss = (close_t < current_entry_price * 0.99)

            if cond_cross and not cond_stop_loss:
                in_position = False
                exit_signal[i] = True
                # Можем сразу обнулить current_entry_price,
                # если хотим защититься от повторных проверок:
                current_entry_price = None
                trade = trades[-1]  # последняя
                trade["exit_index"] = i
                trade["exit_time"] = row["datetime"]
                trade["exit_price"] = row["close"]


            if cond_stop_loss and not cond_cross:
                in_position = False
                exit_signal[i] = True
                trade = trades[-1]  # последняя
                trade["exit_index"] = i
                trade["exit_time"] = row["datetime"]
                trade["exit_price"] = current_entry_price * 0.99

                # Можем сразу обнулить current_entry_price,
                # если хотим защититься от повторных проверок:
                current_entry_price = None

    # Превращаем наши списки в столбцы DataFrame
    df["entry_signal_iter"] = entry_signal
    df["exit_signal_iter"] = exit_signal
    df["entry_price_iter"] = entry_price

    return df


# Тестируем загрузку данных
df = fetch_historical_data()

# Фильтруем данные, оставляя только основную торговую сессию (9:30 - 16:00)

# Вычисляем MA и выводим первые строки
df = calculate_moving_averages(df)
##print(df[["datetime", "close", "SMA_10", "WMA_120", "WMA_400", "SMA_4000"]].head(4000))  # Проверяем 15 строк

df = df.dropna().reset_index(drop=True)  # Удаляем строки с NaN и сбрасываем индексы

df = iterative_backtest(df)

# Посмотреть, где появились сигналы:
df_signals = df[(df["entry_signal_iter"] | df["exit_signal_iter"])]

#print(df_signals[["datetime", "close", "entry_signal_iter", "exit_signal_iter", "entry_price_iter"]])

trades_df = pd.DataFrame(trades)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 1200)
#print(trades_df[["entry_time", "entry_price", "exit_time", "exit_price"]])

initial_deposit = 7000
deposit = initial_deposit

max_loss_streak = 0  # Максимальная серия убыточных сделок
current_loss_streak = 0  # Текущая серия убыточных сделок
good_trades = 0  # Количество удачных сделок
overal_trades = 0
wins = 0
winSum = 0
lossSum = 0

# Проходим по всем трейдам и обновляем депозит
for index, trade in trades_df.iterrows():
    overal_trades += 1
    entry_price = trade["entry_price"]
    exit_price = trade["exit_price"]

    # Доля изменения (прибыль или убыток)
    trade_return = (exit_price - entry_price) / entry_price

    prev_deposit = deposit

    # Обновляем депозит
    deposit *= (1 + trade_return)

    result = deposit - prev_deposit

    # Проверяем, была ли сделка убыточной
    if trade_return < 0:
        lossSum += abs(result)
        current_loss_streak += 1  # Увеличиваем серию проигрышей
        max_loss_streak = max(max_loss_streak, current_loss_streak)  # Обновляем максимум
    else:
        winSum += result  # Суммируем доходные сделки
        wins += 1
        current_loss_streak = 0  # Сброс серии убыточных сделок

#    if trade_return > 0.03:
#        good_trades += 1
    print(f"entry {trade["entry_time"]}, exit {trade["exit_time"]} trade_return {trade_return:.2%}, deposit {deposit:.2f}")


# Рассчитываем win/loss ratio с защитой от деления на 0
winloss_ratio = ((winSum/wins)/(lossSum/(overal_trades-wins))) if lossSum > 0 else 0

# Рассчитываем процент прибыльных сделок
win_rate = (wins / overal_trades) if overal_trades > 0 else 0

# Вывод результатов
print(f"Начальный депозит: {initial_deposit}")
print(f"Финальный депозит: {deposit:.2f}")
print(f"Доходность: {((deposit / initial_deposit) - 1) * 100:.2f}%")
print(f"Максимальная серия убыточных сделок: {max_loss_streak}")
print(f"Winloss ratio: {winloss_ratio:.2}")

print(f"Общее количество сделок: {overal_trades}")
print(f"Количество хороших сделок: {good_trades}")
print(f"Процент прибыльных сделок: {win_rate:.2%}")


