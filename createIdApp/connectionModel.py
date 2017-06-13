#_*_coding:utf-8_*_

import sqlalchemy
from sqlalchemy.pool import NullPool
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine,bindparam,and_,or_
import pymssql
import uuid

import sys

reload(sys)
sys.setdefaultencoding( "utf-8" )

def initConnect(tableName):
    #create_engine 方法进行数据库连接，返回一个 db 对象。
    #echo = True 是为了方便 控制台 logging 输出一些sql信息，默认是False
    engine = create_engine('mssql+pymssql://bs-prt:123123@192.168.1.253:1433/Collectiondb?charset=utf8',poolclass=NullPool,echo=False)
    connection = engine.connect()
    metaData = sqlalchemy.schema.MetaData(bind=engine,reflect=True)
    table_schema = sqlalchemy.Table(tableName,metaData,autoload=True)
    return engine,connection,table_schema


def getAll_Data1(creator):

    engine,connection,table_schema = initConnect("T_Treasure_EvalCustomItem")
    #创建session:
    Session = sessionmaker(bind=engine)
    session = Session()
    #获取任务
    # res = session.query(table_schema).all()

    res = session.query(table_schema).filter(table_schema.columns.Creator == creator).all()

    # res = session.execute("SELECT * FROM table_schema WHERE Creator=%s"%creator)

    # #断开连接
    session.close()
    connection.close()
    return res

#增
def add_Datas(ItemNames,Validitys,IDs):
    engine,connection,table_schema = initConnect('T_Treasure_EvalCustomItem')
    #创建session
    Session = sessionmaker(bind=engine)
    session = Session()

    tasks = table_schema(ItemName=ItemNames,Validity=Validitys,ID=IDs)
    #新增任务
    session.add(tasks)
    session.commit()

    session.close()
    connection.close()
    return tasks



def getAll_DetailDatas(ItemIDS):

    engine,connection,table_schema = initConnect("T_Treasure_EvalCustomItem_Detail")
    #创建session
    Session = sessionmaker(bind=engine)
    session = Session()
    #获取任务详情

    res = session.query(table_schema).filter(table_schema.columns.ItemID==ItemIDS).all()


    #断开连接
    session.close()
    connection.close()
    return res


def getAll_PinLuns(ItemNames,TreasureIDs):
    engine,connection,table_schema = initConnect("V_Treasure_Evaluation")
    #创建session
    Session = sessionmaker(bind=engine)
    session = Session()
    #获取任务详情
    res = session.query(table_schema).filter(and_(table_schema.columns.ItemName==ItemNames,table_schema.columns.TreasureID==TreasureIDs)).all()
    #断开连接
    session.close()
    connection.close()
    return res


#sql
class Mssql:
    def __init__(self):
        self.host = '192.168.1.253:1433'
        self.user = 'bs-prt'
        self.pwd = '123123'
        self.db = 'Collectiondb'

    def __get_connect(self):
        if not self.db:
            raise (NameError, "do not have db information")
        self.conn = pymssql.connect(
            host=self.host,
            user=self.user,
            password=self.pwd,
            database=self.db,
            charset="utf8"
        )
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, "Have some Error")
        else:
            return cur

    def exec_query(self, sql):
        """
         the query will return the list, example;
                ms = MSSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
                resList = ms.ExecQuery("SELECT id,NickName FROM WeiBoUser")
                for (id,NickName) in resList:
                    print str(id),NickName
        """
        cur = self.__get_connect()
        cur.execute(sql)
        res_list = cur.fetchall()

        # the db object must be closed
        self.conn.close()
        return res_list

    def exec_non_query(self, sql):
        """
        execute the query without return list, example：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        cur = self.__get_connect()

        cur.execute(sql)

        self.conn.commit()
        self.conn.close()

    def exec_many_query(self, sql, param):
        """
        execute the query without return list, example：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        cur = self.__get_connect()
        try:
            cur.executemany(sql, param)

            self.conn.commit()
        except Exception as e:
            self.conn.rollback()

        self.conn.close()

    def exec_one_by_one_query(self, sql, param):
        """
        execute the query without return list, example：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        cur = self.__get_connect()

        for i in param:
            sql_text = "insert into T_Treasure_EvalCustomItem_Detail values ('%s','%s','%s','%s','%s','%d','%d','%d','%s','%s','%s','%s','%s','%d','%s','%s','%s','%d','%s','%s','%d','%s','%d','%s','%s','%s','%s','%s')" %\
                       (i[0],i[3],' ',' ',' ',1,1,1,' ',uuid.uuid1(),' ',' ',' ',1,' ',' ',' ',1.0,' ',' ',1.0,' ',1,i[1],' ',' ',' ',' ')
            try:
                cur.execute(sql_text)
                self.conn.commit()
            except Exception as e:
                print e
                self.conn.rollback()

        self.conn.close()



def getAll_Data(creator):

    conn = Mssql()

    sql_text = "select * from T_Treasure_EvalCustomItem where Creator='%s'" % (creator.encode('utf8'))


    res = conn.exec_query(sql_text)
    if len(res) > 0:
        return res
    else:
        return False


def getAll_DetailData(ItemIDS):

    conn = Mssql()

    sql_text = "select * from T_Treasure_EvalCustomItem_Detail where ItemID='%s'"%(ItemIDS)

    res = conn.exec_query(sql_text)

    return res

def getAll_PinLun(ItemNames,TreasureIDs):

    conn = Mssql()

    sql_text = "select * from V_Treasure_Evaluation where ItemName='%s' and TreasureID='%s' " % (ItemNames.encode('utf-8'),TreasureIDs.encode('utf-8'))

    res = conn.exec_query(sql_text)

    return res































