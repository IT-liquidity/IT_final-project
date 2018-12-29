#проводим подготовительные процедуры по работе с базой данных SQL Alchemy с установкой необходимых пакетов/библиотек:

from sqlalchemy import create_engine, event, text
from sqlalchemy import Column, Index, Boolean, Integer, Float, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.pool import Pool


#Создание базы данных 
engine = create_engine(
    'sqlite:///liquidity-analysis.db',
    isolation_level='SERIALIZABLE',
)
#Создаем новую сессию, первый запрос
Session = sessionmaker(bind=engine)


class _Base:
    def to_dict(model_instance, query_instance=None):
        if hasattr(model_instance, '__table__'):
            return {
                c.name: getattr(model_instance, c.name)
                for c in model_instance.__table__.columns
            }
        else:
            cols = query_instance.column_descriptions
            
            return {
                cols[i]['name']: model_instance[i]
                for i in range(len(cols))
            }


    @classmethod
    def from_dict(cls, dict, model_instance):
        for c in model_instance.__table__.columns:
            setattr(model_instance, c.name, dict[c.name])


Base = declarative_base(cls=_Base)

#Создаем таблицу с ценными бумагами в виде акций и пр.
class Security(Base):
    __tablename__ = 'security'

    '''
    SUPERTYPE,INSTRUMENT_TYPE,TRADE_CODE
    Акции,Пай биржевого ПИФа,CBOM
    '''
    id = Column(Integer, primary_key=True)
    seccode = Column(String, index=True)
    instrument_type = Column(String)
    supertype = Column(String)

#Создаем таблицу с привилегированными акциями
class PrefferedStockOrderLog(Base):
    __tablename__ = 'preffered_stock_order_log'

    '''
    NO,SECCODE,BUYSELL,TIME,ORDERNO,ACTION,PRICE,VOLUME,TRADENO,TRADEPRICE
    1,PLSM,S,100000000,1,1,0.32,36000,,
    '''
    id = Column(Integer, primary_key=True)
    no = Column(Integer, index=True)
    seccode = Column(String, index=True)
    buysell = Column(String)
    time = Column(Integer, index=True)
    orderno = Column(Integer, index=True)
    action = Column(Integer)
    price = Column(Float)
    volume = Column(Float)
    tradeno = Column(Integer)
    tradeprice = Column(Float)

#Создаем таблицу с обыкновенными акциями
class OrdinaryStockOrderLog(Base):
    __tablename__ = 'ordinary_stock_order_log'

    '''
    NO,SECCODE,BUYSELL,TIME,ORDERNO,ACTION,PRICE,VOLUME,TRADENO,TRADEPRICE
    1,PLSM,S,100000000,1,1,0.32,36000,,
    '''
    id = Column(Integer, primary_key=True)
    no = Column(Integer, index=True)
    seccode = Column(String, index=True)
    buysell = Column(String)
    time = Column(Integer, index=True)
    orderno = Column(Integer, index=True)
    action = Column(Integer)
    price = Column(Float)
    volume = Column(Float)
    tradeno = Column(Integer)
    tradeprice = Column(Float)

#Создаем таблицу с облигациями
class BondOrderLog(Base):
    __tablename__ = 'bond_order_log'

    '''
    NO,SECCODE,BUYSELL,TIME,ORDERNO,ACTION,PRICE,VOLUME,TRADENO,TRADEPRICE
    1,PLSM,S,100000000,1,1,0.32,36000,,
    '''
    id = Column(Integer, primary_key=True)
    no = Column(Integer, index=True)
    seccode = Column(String, index=True)
    buysell = Column(String)
    time = Column(Integer, index=True)
    orderno = Column(Integer, index=True)
    action = Column(Integer)
    price = Column(Float)
    volume = Column(Float)
    tradeno = Column(Integer)
    tradeprice = Column(Float)

#Создаем таблицу Tradelog
class TradeLog(Base):
    __tablename__ = 'trade_log'

    '''
    TRADENO,SECCODE,TIME,BUYORDERNO,SELLORDERNO,PRICE,VOLUME
    2516556767,SBER,100000,7592,9361,74.38,200
    '''
    id = Column(Integer, primary_key=True)
    tradeno = Column(Integer, index=True)
    seccode = Column(String, index=True)
    time = Column(Integer, index=True)
    buyorderno = Column(Integer)
    sellorderno = Column(Integer)
    price = Column(Float)
    volume = Column(Float)


Base.metadata.create_all(engine)

2) Экспорт БД в Python:
#Импортируем необходимые пакеты для Python
import os
import sys
import csv
from datetime import datetime, timedelta

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

#Импортируем таблицы из MySQL в Python
from db import engine, text, Session, Security, PrefferedStockOrderLog, OrdinaryStockOrderLog, BondOrderLog, TradeLog
