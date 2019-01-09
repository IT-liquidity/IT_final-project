import os
import mysql.connector
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import decimal
import csv
from collections import Counter
from pylab import figure, axes, pie, title, show

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="qwerty"
)
cur = conn.cursor()

cur.execute("""USE dbaseorderlog;""")

# БЛОК 3. СОЗДАНИЕ КНИГИ ЛИМИТИРОВАННЫХ ЗАЯВОК
# ШАГ 1: Создаем возможность пользователю задавать параметры (тикер, дату и момент времени)
print('Введите Тикер:')
SEC_CODE = input()
print('Введите время (9 знаков):')
timestamp = input()
print ('Ведите дату (07.12.2015 или 22.12.2015) в формате ддммгггг:')
day = ()
# ШАГ 2: Выводим поток заявок на продажу по введенным пользователем данным:
query1 = f'select PRICE, VOLUME from Orderlog where SECCODE="{SEC_CODE}" and BUYSELL="S" and time <={timestamp} ' \
    f'and PRICE > 0 and DATE="{day}" group by PRICE order by PRICE desc'
cursor.execute(query1)
result1 = cursor.fetchmany(15000)
print('Поток заявок на продажу по тикеру',result1)
# ШАГ 3: Выводим поток заявок на покупку по введенным пользователем данным:
query2 = f'select PRICE, VOLUME from Orderlog where SECCODE="{SEC_CODE}" and BUYSELL="B" and time <={timestamp} ' \
    f'and PRICE > 0 group by PRICE order by PRICE desc'
cursor.execute(query2)
result2 = cursor.fetchmany(15000)
print('Поток заявок на покупку по тикеру',result2)
# ШАГ 4: Строим книгу заявок с визуазизацией в виде таблицы + сохранение
best_asks = sorted(result1, key=lambda n: n[0])[:20]
best_bids = sorted(result2, key=lambda n: -n[0])[:20]
print('best_bids:', best_bids)
print('best_asks:', best_asks)
from prettytable import PrettyTable
Order_book = PrettyTable()
Bid_price, Bid_volume = zip(*best_bids)
Ask_price, Ask_volume = zip(*best_asks)
Order_book.add_column("Bid volume", Bid_volume)
Order_book.add_column("Bid_price", Bid_price)
Order_book.add_column("Ask_price", Ask_price)
Order_book.add_column("Ask_volume", Ask_volume)
print(Order_book)
plt.savefig('Table_Order_book.png')
# Визуализация в виде графика + сохранение:
import matplotlib.pyplot as plt
plt.bar(*zip(*best_asks), label='Ask', color='red')
plt.bar(*zip(*best_bids), label='Bid', color='green')
plt.ylabel('Volume')
plt.xlabel('Price')
plt.title('Depth of Market')
plt.legend(loc='upper right')
plt.show()
plt.savefig('Graph_Order_book.png')


# БЛОК 4. ОЦЕНКА РИСКА ЛИКВИДНОСТИ ПО ДАННЫМ РАЗНОЙ ЧАСТОТЫ
# ШАГ 1: Вод даты, тикера, шага, периода и объема:
tickers_text = open('ListingSecurityList.csv', encoding = "cp1251").read()
tickers = {}
for elem in tickers_text.split('\n')[:-1]:
    t = elem.split(',')
    tickers[t[7].replace("\"", "")] = t[5].replace("\"", "")

ticker = input("Введите тикер: ")
step = input("Введите шаг: ")
day = input("Введите дату 07.12.2015 или 22.12.2015: ")
time_lower = int(input("Введите начало периода с 9.30 по 19.00: "))
time_upper = int(input("Введите конец периода с 9.30 по 19.00: "))
volume = int(input("Введите торговый объем сделки : "))
deal_type = input("Введите тип сделки : ")

# ШАГ 2: Анализ средней цены, сжатости и глубины:
# Перевод шага в формат, позволяющий суммировать время:
step = datetime.strptime(step, '%H%M%S%f')
time_zero = datetime.strptime('000000000', '%H%M%S%f')

# Создаем списки для переменных, графики которых нужно изобразить:
mid_price = []
bid_ask_spread = []
depth_ask = []
depth_bid = []
av_prices = []
# Моменты времени:
time_list = []
time = time_lower

# Выгрузка данных о заявках в указанном временном интервале в зависимости от вида введенной ценной бумаги:
while time<=time_upper:
	date=day
    time_list.append(int(str(time)[:-3]))
    if tickers[ticker] == 'Акция обыкновенная':
        t = f"""SELECT orderno, action, buysell, price, volume, time FROM CommonStock
where TIME<= '%s' and seccode = '%s';""" % (int(str(time)[:-3]), ticker)
        cur.execute(t)
        rows = cur.fetchall()
    elif tickers[ticker] == 'Акция привилегированная':
       t = f"""SELECT orderno, action, buysell, price, volume, time FROM PreferredStock
where TIME<= '%s' and seccode = '%s';""" % (int(str(time)[:-3]), ticker)
       cur.execute(t)
       rows = cur.fetchall()
    elif 'облигац' in tickers[ticker].lower():
       t = f"""SELECT orderno, action, buysell, price, volume, time FROM Bonds
where TIME<= '%s' and seccode = '%s';""" % (int(str(time)[:-3]), ticker)
       cur.execute(t)
       rows = cur.fetchall()

# Создание списка для заявок:
    glass = [] #(buysell, price, volume)
    # создаем временный спискок для цен, по которым был реализован объем (указанный пользователем) до данного момента времени
    prices = []

    for elem in rows:
        # если action==1 (размещение заявки)
        if elem[1] == 1:
            # если объем не равен нулю
            if elem[4] != 0:
                # если список непустой
                if len(glass)>0:
                    flag = False
                    for j in glass:
                        # если в списке уже есть заявка такого же типа (buy/sell) с такой же ценой
                        if j[1] == elem[3] and j[0] == elem[2]:
                            # добавляем к исходному объему объем данной заявки
                            j[2] += elem[4]
                            flag = True
                            # выходим из цикла
                            break
                    # в противном случае добавляем заявку в стакан
                    if not flag:
                        glass.append([elem[2], elem[3], elem[4]])
                # если список пустой, добавляем заявку (первая заявка)
                else:
                    glass.append([elem[2], elem[3], elem[4]])
            else:
                pass
        # если action=0 (снятие заявки)
        elif elem[1] == 0:
            # ищем в стакане нужную заявку
            for j in glass:
                # если тип заявки (buy/sell) и цена совпадают, значит мы нашли нужную заявку
                if j[1] == elem[3] and j[0] == elem[2]:
                    # вычитаем соответствущий объем
                    j[2] += -elem[4]
                    # если оставшийся объем равен нулю, удаляем заявку
                    if j[2] <= 0:
                        del(j)
                    # так как мы нашли нужную заявку, выходим из цикла
                    break
        # если action=2 (сделка)
        elif elem[1] == 2:
            # ищем в стакане нужную заявку
            for j in glass:
                # если тип заявки (buy/sell) и цена совпадают, значит мы нашли нужную заявку
                if j[1] == elem[3] and j[0] == elem[2]:
                    # вычитаем соответствущий объем
                    j[2] += -elem[4]
                    # если оставшийся объем меньше или равен нулю, удаляем заявку
                    if j[2] <= 0:
                        del(j)
                    # если тип сделки и объем совпадают с теми, которые указывал пользователь, добавляем цену сделки в соответствующий список
                    if deal_type == elem[2] and volume == elem[4]:
                        prices.append(elem[3])
                    # выходим из цикла
                    break

    # если мы нашли сделки, у которых тип и объем совпадали с теми, которые указывал пользователь, добавляем усредненное значение цен в соответствующий список
    if len(prices)>0:
        av_prices.append(np.mean(np.array(prices)))
    else:
        # в противном случае добавляем ноль
        av_prices.append(0)

    # "очищаем" стакан от заявок с нулевым объемом
    glass_new = []
    for j in range(0,len(glass)):
        if glass[j][2]!=0:
            glass_new.append(glass[j])

    # Из списка glass_new создадим датафрейм
    df = pd.DataFrame.from_records(glass_new, columns=['time','buy/sell', 'price', 'volume'])
    # Создадим отдельные колонки для объема ask и bid (ноль в i-ой строке означает, что по данной цене была отправлена заявка противоположного типа)
    df['buy_volume'] = np.where(df['buy/sell'] == 'B', df['volume'], 0)
    df['sell_volume'] = np.where(df['buy/sell'] == 'S', df['volume'], 0)
    del df['buy/sell']
    del df['volume']
    df.sort_values('price', inplace=True, ascending=False)
    df = df.reindex(columns=['buy_volume', 'price', 'sell_volume'])
    # Таблица, визуализирующая стакан
    print(df)

    # оставляем только ненулевые значения
    buy = df.loc[(df['buy_volume'] > 0)]
    sell = df.loc[(df['sell_volume'] > 0)]

    # Создаем  соответствующие списки
    bid_price = buy['price']
    bid_volume = buy['buy_volume']
    ask_price = sell['price']
    ask_volume = sell['sell_volume']

    # добавляем новые значения в списки соответствующих переменных
    mid_price.append(round((float(ask_price[-1:])+float(bid_price[:1]))/2, 3))
    bid_ask_spread.append(round(float(ask_price[-1:])-float(bid_price[:1]), 3))
    depth_ask.append(int(ask_volume[-1:]))
    depth_bid.append(int(bid_volume[:1]))

    # прибавляем к предыдущему моменту времени шаг, заданный пользователем
    time = datetime.strptime(str(time), '%H%M%S%f')
    time = int((time - time_zero + step).time().strftime('%H%M%S%f'))

# Выводим полученные значения показателей ликвидности:
print(mid_price)
print(bid_ask_spread)
print(depth_ask)
print(depth_bid)

# ШАГ 3: Линейные графики и гистограммы, сохранение результатов
# Строим линейные графики средней цены, бид-аск спреда с указанием 5 и 95% квантилей, средней и медианы ряда:
# Средняя цена:
fig, ax = plt.subplots()
plt.plot(time_list, mid_price, color='black', label='Mid_price')
plt.plot(time_list, [np.mean(mid_price) for i in range(len(mid_price))], label='Mean', color='green', linestyle='--')
plt.plot(time_list, [np.median(mid_price) for i in range(len(mid_price))], label='Median', color='red', linestyle='--')
plt.plot(time_list, [np.quantile((mid_price), 0.05) for i in range(len(mid_price))], label='Quantile 5%', color='blue', linestyle='dashed')
plt.plot(time_list, [np.quantile((mid_price), 0.95) for i in range(len(mid_price))], label='Quantile 95%', color='orange', linestyle='dashed')
ax.set(xlabel='Time', ylabel='Rub')
plt.legend()
plt.show()
savefig('Linear_graph_mid_price.png')
# BID-ASK спред:
fig, ax = plt.subplots()
plt.plot(time_list, bid_ask_spread, color='black', label='Bid_ask_spread')
plt.plot(time_list, [np.mean(bid_ask_spread) for i in range(len(bid_ask_spread))], label='Mean', color='green', linestyle='--')
plt.plot(time_list, [np.median(bid_ask_spread) for i in range(len(bid_ask_spread))], label='Median', color='red', linestyle='--')
plt.plot(time_list, [np.quantile((bid_ask_spread), 0.05) for i in range(len(bid_ask_spread))], label='Quantile 5%', color='blue', linestyle='dashed')
plt.plot(time_list, [np.quantile((bid_ask_spread), 0.95) for i in range(len(bid_ask_spread))], label='Quantile 95%', color='orange', linestyle='dashed')
ax.set(xlabel='Time', ylabel='Rub')
plt.legend()
plt.show()
savefig('Linear_graph_bid_ask_spread.png')
# Строим гистограммы средней цены, бид-аск спреда с указанием 99% квантиля:
# Средняя цена:
fig, ax = plt.subplots()
plt.hist(mid_price, bins=20, rwidth=0.9,
                   color='c')
plt.axvline(np.quantile((mid_price), 0.99), color='k', linestyle='dashed', linewidth=1, label='Quantile 99%')
plt.xlabel('Mid_price')
plt.legend()
plt.show()
savefig('Histogram_mid_price.png')
# BID-ASK спред:
fig, ax = plt.subplots()
plt.hist(bid_ask_spread, bins=20, rwidth=0.9,
                   color='c')
plt.axvline(np.quantile((bid_ask_spread), 0.99), color='k', linestyle='dashed', linewidth=1, label='Quantile 99%')
plt.xlabel('Bid_ask_spread')
plt.legend()
plt.show()
savefig('Histogram_bid_ask_spread.png')
