#Создание базы данных
create database exercise1;

#Создание таблицы Orderlog(20151222) и загрузка данных из файла в таблицу
use exercise1;
create table Orderlog(
NO BIGINT PRIMARY KEY not null, 
SECCODE text, 
BUYSELL CHAR(1), 
TIME INT, 
ORDERNO BIGINT unsigned,
ACTION BIGINT unsigned,
PRICE FLOAT,
VOLUME BIGINT unsigned,
TRADENO TEXT,
TRADEPRICE TEXT);

SET autocommit =0;
SET unique_checks = 0;
SET foreign_key_checks = 0;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/OrderLog20151222.csv' 
INTO TABLE Orderlog
FIELDS TERMINATED BY ','
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(NO,SECCODE,BUYSELL,TIME,ORDERNO,ACTION,PRICE,VOLUME,TRADENO,TRADEPRICE);

COMMIT;

SET unique_checks = 1;
SET foreign_key_checks = 1;

#Создание таблицы Orderlog2(20151207) и загрузка данных из файла в таблицу
use exercise1;
create table Orderlog2(
NO BIGINT PRIMARY KEY not null, 
SECCODE text, 
BUYSELL CHAR(1), 
TIME INT, 
ORDERNO BIGINT unsigned,
ACTION BIGINT unsigned,
PRICE FLOAT,
VOLUME BIGINT unsigned,
TRADENO TEXT,
TRADEPRICE TEXT);

SET autocommit =0;
SET unique_checks = 0;
SET foreign_key_checks = 0;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/OrderLog20151207.csv' 
INTO TABLE Orderlog2
FIELDS TERMINATED BY ','
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES
(NO,SECCODE,BUYSELL,TIME,ORDERNO,ACTION,PRICE,VOLUME,TRADENO,TRADEPRICE);

COMMIT;

SET unique_checks = 1;
SET foreign_key_checks = 1;

#Создание таблицы классификатора
use exercise1;
create table list(
Instrument_id bigint, 
Instrument_type varchar(100),
TRADE_CODE varchar(30) not NULL);
LOAD DATA Infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ListingSecurityList.csv'
INTO TABLE list
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

#Создание колонки DATA в таблице Orderlog2 с пустыми значениями даты
use EXERCISE1;
alter table ORDERLOG2
ADD COLUMN DATA bigint;

#Заполнение последней колонки таблицы Orderlog2 датой 20151207
SET SQL_SAFE_UPDATES = 0;
use exercise1;
update orderlog set DATA ='20151207';

#Создание колонки DATA в таблице Orderlog с пустыми значениями даты
use EXERCISE1;
alter table ORDERLOG
ADD COLUMN DATA bigint;

#Заполнение последней колонки таблицы Orderlog датой 20151222
SET SQL_SAFE_UPDATES = 0;
use exercise1;
update orderlog set DATA ='20151222';

#Объединение таблиц Orderlog и Orderlog2 в одну с добавлением колонок дат
use exercise1;
CREATE TABLE Union_table AS
SELECT * FROM orderlog
UNION
SELECT * FROM orderlog2 

#Создание таблицы обыкновенных акций за 20151222
use exercise1;
create table if not exists Ordinary_shares1
SELECT SECCODE, BUYSELL,TIME,ORDERNO,ACTION,PRICE,VOLUME,TRADENO,TRADEPRICE,DATA 
FROM Orderlog JOIN list
ON SECCODE=TRADE_CODE
WHERE INSTRUMENT_TYPE = 'Акция обыкновенная'

#Создание таблицы привилегированных акций за 20151222
use exercise1;
create table if not exists Preferred_shares1
SELECT SECCODE, BUYSELL,TIME,ORDERNO,ACTION,PRICE,VOLUME,TRADENO,TRADEPRICE,DATA 
FROM Orderlog JOIN list
ON SECCODE=TRADE_CODE
WHERE INSTRUMENT_TYPE = 'Акция привилегированная'

#Создание таблицы обыкновенных акций за 20151207
use exercise1;
create table if not exists Ordinary_shares2
SELECT SECCODE, BUYSELL,TIME,ORDERNO,ACTION,PRICE,VOLUME,TRADENO,TRADEPRICE,DATA 
FROM Orderlog2 JOIN list
ON SECCODE=TRADE_CODE
WHERE INSTRUMENT_TYPE = 'Акция обыкновенная'

#Создание таблицы привилегированных акций за 20151207
use exercise1;
create table if not exists Preferred_shares1
SELECT SECCODE, BUYSELL,TIME,ORDERNO,ACTION,PRICE,VOLUME,TRADENO,TRADEPRICE,DATA 
FROM Orderlog2 JOIN list
ON SECCODE=TRADE_CODE
WHERE INSTRUMENT_TYPE = 'Акция привилегированная'

#Проверка Orderlog (20151222) на отрицательные значения
use exercise1;
SELECT *,
CASE WHEN NO < 0 or SECCODE < 0 or BUYSELL <0 or TIME < 0 or ORDERNO < 0 or ACTION < 0 or PRICE < 0 or VOLUME < 0
THEN 'NEGATIVE VALUES'
else 'NO NEGATIVE VALUES'
END AS `NEGATIVE NUMBERS CHECK` 
from Orderlog;

#Проверка Orderlog2 (20151207) на отрицательные значения
use exercise1;
SELECT *,
CASE WHEN NO < 0 or SECCODE < 0 or BUYSELL <0 or TIME < 0 or ORDERNO < 0 or ACTION < 0 or PRICE < 0 or VOLUME < 0
THEN 'NEGATIVE VALUES'
else 'NO NEGATIVE VALUES'
END AS `NEGATIVE NUMBERS CHECK` 
from Orderlog2;

#Проверка Orderlog на наличие нулевого объема
use exercise1;
select *
from Orderlog
where VOLUME IS NULL;

#Проверка Orderlog2 на наличие нулевого объема
use exercise1;
select *
from Orderlog2
where VOLUME IS NULL;

#Проверка на наличие нулевых и пропущенных значений Orderlog(20151222)
SELECT *,
If (
NO is NULL or NO =''
or
SECCODE is NULL or SECCODE =''
or
BUYSELL is NULL or BUYSELL =''
or
TIME is NULL or TIME =''
or
ORDERNO is NULL or ORDERNO =''
or
ACTION is NULL or ACTION =''
or
PRICE is NULL or PRICE =''
or
VOLUME is NULL or VOLUME = '', 'DATA IS REQUIRED','COMPLETE DATA' 
) as `NULL AND GAPS CHECK` from exercise1.Orderlog;

#Проверка на наличие нулевых и пропущенных значений Orderlog2(20151207)
SELECT *,
If (
NO is NULL or NO =''
or
SECCODE is NULL or SECCODE =''
or
BUYSELL is NULL or BUYSELL =''
or
TIME is NULL or TIME =''
or
ORDERNO is NULL or ORDERNO =''
or
ACTION is NULL or ACTION =''
or
PRICE is NULL or PRICE =''
or
VOLUME is NULL or VOLUME = '', 'DATA IS REQUIRED','COMPLETE DATA' 
) as `NULL AND GAPS CHECK` from exercise1.Orderlog2;

#вывод по таблице облигаций за 20151222 общего объема торгов 
use exercise1;
select sum(volume) from preferred_shares;

#вывод количества уникальных облигаций за 20152212 
use exercise1;
select SECCODE from preferred_shares group by SECCODE having count(*)=1;

#вывод общего количества сделок по привилегированным акциям за 20151222 
use exercise1;
select count(SECCODE) from preferred_shares order by TRADENO>0 AND TRADEPRICE>0;

#вывод максимальной цены по привилегированным акциям за день 20151222 
use exercise1;
select max(PRICE) from preferred_shares;

#вывод минимальной цены по привилегированным акциям за день 20151222 
use exercise1;
select min(PRICE) from preferred_shares;

#вывод максимальной цены по сделкам с привилегированными акциями за 20151222 
use exercise1;
select MAX(TRADEPRICE) from preferred_shares;

#Выведем описательные статистики по отдельно взятой бумаге (к примеру акциям Газпрома) за 20151222. 
#Найдем общий объем торгов за выбранный день по акциям Газпрома 
use exercise1;
select SUM(VOLUME) from ordinary_shares1 WHERE SECCODE= 'GAZP';

#Найдем общее количество сделок по акциям Газпрома за 20151222 с помощью следующего запроса: 
use exercise1;
select count(SECCODE) from ordinary_shares1 where SECCODE ='GAZP' order by TRADENO>0 AND TRADEPRICE>0;

#Найдем максимальную цену сделки по акциям Газпрома за 20151222 с помощью запроса: 
use exercise1;
select MAX(TRADEPRICE) from ordinary_shares1 where SECCODE='GAZP';

#Найдем минимальную цену сделки по акциям Газпрома за 20151222 с помощью запроса: 
use exercise1;
select MIN(TRADEPRICE) from ordinary_shares1 where SECCODE='GAZP';

#Найдем минимальный объем исполненной рыночной заявки за 20151222. 
#Данные заявки имеют цену равную 0.
use exercise1;
select MIN(VOLUME) from ordinary_shares1 where SECCODE='GAZP' ORDER BY PRICE=0;

#Найдем максимальный объем исполненной рыночной заявки за 20151222. 
#Данные заявки имеют цену равную 0. 
use exercise1;
select MAX(VOLUME) from ordinary_shares1 where SECCODE='GAZP' ORDER BY PRICE=0;

#Найдем максимальный объем лимитированной заявки по Газпрому за 20151222. 
#Данные заявки имеют цену больше 0. 
use exercise1;
select MAX(VOLUME) from ordinary_shares1 where SECCODE='GAZP' ORDER BY PRICE>0 AND TRADEPRICE>0 AND TRADENO>0;

#Найдем минимальный объем исполненной лимитированной заявки по Газпрому за 20151222. 
#Данные заявки имеют цену больше 0.
use exercise1;
select min(VOLUME) from ordinary_shares1 where SECCODE='GAZP' ORDER BY PRICE>0 AND TRADEPRICE>0 AND TRADENO>0;



















