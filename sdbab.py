from queue import Queue
from time import time, sleep
from sqlalchemy import create_engine
from pymongo import MongoClient
from pandas import DataFrame
from logging import getLogger

logger = getLogger('logger_sdbab')


SDBAB_QUEUE = Queue(5)

def sdbab_counter():
    if not SDBAB_QUEUE.full():
        SDBAB_QUEUE.put(time())
    else:
        bool_msg = True
        while time() - SDBAB_QUEUE.queue[0] < 10:
            if bool_msg:
                logger.warn("Slow down, cowboy!")
                bool_msg = False
            sleep(2.5)
        SDBAB_QUEUE.get()
        SDBAB_QUEUE.put(time())


def quote(v):
    if type(v) == str:
        return "'" + v + "'"
    else:
        return str(v)


class DBClient(object):
    
    def __init__(self):
        raise NotImplementedError

    def get_connection(self):
        raise NotImplementedError

    def close_connection(self):
        raise NotImplementedError

    def insert(self):
        raise NotImplementedError

    def delete(self):
        raise NotImplementedError

    def update(self):
        raise NotImplementedError

    def get(self):
        raise NotImplementedError


class MariaDBClient(DBClient):

    def __init__(self, user, password, host, port, db, tbc):
        self.__user = user
        self.__password = password
        self.__host = host
        self.__port = int(port)
        self.__db = db
        self.__tbc = tbc
        self.__engine = create_engine(
            'mysql+mysqlconnector://' \
            + self.__user \
            + ':' \
            + self.__password\
            + '@' \
            + self.__host \
            + ':'
            + str(self.__port) \
            + "/" \
            + self.__db
        )
        self.__connection = None
    
    def get_connection(self):
        if self.__connection is None:
            self.__connection = self.__engine.connect()
        sdbab_counter()
        return self.__connection
    
    def close_connection(self):
        if self.__connection is not None:
            self.__connection.close()
            self.__connection = None
    
    def insert(self, df):
        try:
            sdbab_counter()
            df.reset_index(drop=True).to_sql(
                name=self.__tbc, 
                con=self.__engine,
                if_exists='append',
                index=False,
                chunksize=1000
            )
        except Exception as e:
            logger.error("Query execution failed! " + str(e))

    def delete(self, df_key):
        sdbab_counter()
        str_where = " OR ".join(["(" + " AND ".join([str(k) + "=" + quote(v) for k, v in zip(d.keys(), d.values())]) + ")" for d in df_key.to_dict('records')])
        connection = self.get_connection()
        try:
            connection.execute("DELETE FROM " + self.__tbc + " WHERE " + str_where + ";")
        except Exception as e:
            logger.error("Query execution failed! " + str(e))        
        finally:
            self.close_connection()

    def update(self):
        pass

    def get(self):
        pass


class MongoDBClient(DBClient):

    def __init__(self, user, password, host, port, db, tbc):
        self.__user = user
        self.__password = password
        self.__host = host
        self.__port = int(port)
        self.__db = db
        self.__tbc = tbc
        self.__client = None
        self.__connection = None
    
    def get_connection(self):
        if (self.__connection is None) or (self.__client is None):
            self.__client = MongoClient(
                'mongodb://' \
                + self.__user \
                + ':' \
                + self.__password \
                + '@' \
                + self.__host \
                + ':' \
                + str(self.__port) \
                + "/" \
                + self.__db, \
                retryWrites=False
            )
            self.__connection = self.__client[self.__db][self.__tbc]
        sdbab_counter()
        return self.__connection
    
    def close_connection(self):
        if self.__connection is not None:
            self.__client.close()
            self.__connection = None
            self.__client = None
    
    def insert(self, df):
        try:
            sdbab_counter()
            collection = self.get_connection()
            collection.insert_many(df.to_dict('records'))
        except Exception as e:
            logger.error("Query execution failed! " + str(e))
        finally:
            self.close_connection()

    def delete(self, df_key):
        sdbab_counter()
        collection = self.get_connection()
        try:
            collection.delete_many(df_key.to_dict('list'))
        except Exception as e:
            logger.error("Query execution failed! " + str(e))        
        finally:
            self.close_connection()

    def update(self):
        pass

    def get(self):
        pass