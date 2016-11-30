#!/usr/bin/python3
# -*- coding: Cp1251 -*-

'''
Created on 21 окт. 2016 г.

@author: ggolyshev
'''
import sys
import pymssql
import MySQLdb
import sqlite3
from abc import ABCMeta, abstractmethod
from collections import namedtuple

tplDirection=namedtuple('tplDirection', ['header', 'data'])

const_direction=tplDirection('cat', 'data')

cstr_DefDBName='BUH'
cstr_DefHeaderTableName='CATALOG'
cstr_HeaderErrorTableName=cstr_DefHeaderTableName + '_err_insert'
cstr_DefBUHTableName='BUH_DATA'
cstr_BUHErrorTableName=cstr_DefBUHTableName + '_err_insert'

class sqlCommand():
    __metaclass__=ABCMeta
    
    @abstractmethod
    def createComm(self, tableName=cstr_DefHeaderTableName, dababasename=cstr_DefDBName, 
                   direction=const_direction.header):
        '''return create table sql command'''
    
    @abstractmethod
    def deleteComm(self, tableName=cstr_DefHeaderTableName, dababasename=cstr_DefDBName):
        '''return delete table sql command'''
        
    @abstractmethod
    def insertComm(self, tableName=cstr_DefHeaderTableName, dababasename=cstr_DefDBName, 
                   direction=const_direction.header, **kwargs):
        '''return insert table sql command'''
        
    @abstractmethod
    def updateComm(self, tableName=cstr_DefHeaderTableName, dababasename=cstr_DefDBName, 
                   direction=const_direction.header, **kwargs):
        '''return update table sql command'''
        
    @abstractmethod
    def checkExistComm(self, tableName=cstr_DefHeaderTableName, dababasename=cstr_DefDBName):
        '''return check table exists table sql command''' 
    
class msSQLcommand(sqlCommand):
    
    tstrCreateHeaderTableTemplate='''CREATE TABLE [dbo].[{0}](
    [okved] [nvarchar](50) NULL,
    [okpo] [nvarchar](50) NULL,
    [measure] [nvarchar](50) NULL,
    [type] [nvarchar](50) NULL,
    [okopf] [nvarchar](50) NULL,
    [okfs] [nvarchar](50) NULL,
    [hash] [binary](17) NOT NULL,
    [inn] [nvarchar](50) NULL,
    [name] [ntext] NULL,
 CONSTRAINT [PK_{0}] PRIMARY KEY CLUSTERED 
(
    [hash] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]'''

    tstrCreateBUHTableTemplate='''
CREATE TABLE [dbo].[{0}](
    [hash] [binary](17) NOT NULL,
    [ind_code] [int] NOT NULL,
    [ind_val] [real] NULL,
    [act_date] [smalldatetime] NOT NULL,
 CONSTRAINT [PK_{0}_1] PRIMARY KEY CLUSTERED 
(
    [hash] ASC,
    [ind_code] ASC,
    [act_date] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]
'''

    tstrDropTable='''IF  EXISTS (SELECT * FROM sys.objects 
WHERE object_id = OBJECT_ID(N'[dbo].[{0}]') AND type in (N'U'))
DROP TABLE [dbo].[{0}]
'''
    
    tstrCheckTableExsistsTemplate='''SELECT  object_id FROM sys.tables
                         WHERE (name = '{0}')
                         '''

    tstrGetTableFieldsNameTemplate=r'''SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE (TABLE_NAME = N'{0}') AND (COLUMN_NAME LIKE '[0-9]%')
        '''

    tstr_UpdateBUH='''UPDATE [{0}].[dbo].[{1}]
        SET [ind_val] = {ind_val}
        WHERE ([hash] = 0x{hash}) AND ([ind_code] = {ind_code}) AND ([act_date] = '{act_date}')'''

    tstr_InsertBUH='''INSERT INTO [{0}].[dbo].[{1}]
           ([hash]
           ,[ind_code]
           ,[ind_val]
           ,[act_date])
     VALUES
           (0x{hash}
           ,{ind_code}
           ,{ind_val}
           ,'{act_date}') '''

    tstrInsertCat='''INSERT INTO [{0}].[dbo].[{1}]
           ([okved]
           ,[okpo]
           ,[measure]
           ,[type]
           ,[okopf]
           ,[okfs]
           ,[hash]
           ,[inn]
           ,[name])
     VALUES
           ('{okved}'
           ,{okpo}
           ,'{measure}'
           ,'{type}'
           ,{okopf}
           ,{okfs}
           ,0x{hash}
           ,'{inn}'
           ,'{name}')
           '''
           
    tstrUpdateCat='''UPDATE [{0}].[dbo].[{1}]
       SET [okved] = '{okved}'
      ,[okpo] = {okpo}
      ,[measure] = '{measure}'
      ,[type] = '{type}'
      ,[okopf] = {okopf}
      ,[okfs] = {okfs}
      ,[inn] = '{inn}'
      ,[name] = '{name}'
 WHERE [hash]=0x{hash}
'''


    def __init__(self):
        pass
    
    def createComm(self, tableName=cstr_DefHeaderTableName, databasename=cstr_DefDBName, 
                   direction=const_direction.header):
        if direction==const_direction.header:
            return self.tstrCreateHeaderTableTemplate.format(tableName)
        else:
            return self.tstrCreateBUHTableTemplate.format(tableName)

    def deleteComm(self, tableName=cstr_DefHeaderTableName, databasename=cstr_DefDBName):
        return self.tstrDropTable.format(tableName)

    def checkExistComm(self, tableName=cstr_DefHeaderTableName, databasename=cstr_DefDBName):
        return self.tstrCheckTableExsistsTemplate.format(tableName)

    def insertComm(self, tableName=cstr_DefHeaderTableName, databasename=cstr_DefDBName, 
        direction=const_direction.header, dictData=dict()):
        if direction==const_direction.header:
            return self.tstrInsertCat.format(databasename, tableName, **dictData)
        else:
            return self.tstr_InsertBUH.format(databasename, tableName, **dictData)

    def updateComm(self, tableName=cstr_DefHeaderTableName, databasename=cstr_DefDBName, 
        direction=const_direction.header, dictData=dict()):
        if direction==const_direction.header:
            return self.tstrUpdateCat.format(databasename, tableName, **dictData)
        else:
            return self.tstr_UpdateBUH.format(databasename, tableName, **dictData)


class mySQLcommand(sqlCommand):
    
    tstrCreateHeaderTableTemplate='''CREATE TABLE `{0}` (
  `hash` tinyblob NOT NULL,
  `okved` varchar(45) DEFAULT NULL,
  `okpo` varchar(45) DEFAULT NULL,
  `measure` varchar(45) DEFAULT NULL,
  `type` varchar(45) DEFAULT NULL,
  `okopf` varchar(45) DEFAULT NULL,
  `okfs` varchar(45) DEFAULT NULL,
  `inn` varchar(45) DEFAULT NULL,
  `name` mediumtext,
  PRIMARY KEY (`hash`(17))
) ENGINE=MyISAM DEFAULT CHARSET=cp1251;'''


    tstrCreateBUHTableTemplate='''CREATE TABLE `{0}` (
  `hash` tinyblob NOT NULL,
  `ind_code` int(11) NOT NULL,
  `ind_val` double DEFAULT NULL,
  `act_date` datetime NOT NULL,
  PRIMARY KEY (`hash`(17),`ind_code`,`act_date`)
) ENGINE=MyISAM DEFAULT CHARSET=cp1251;'''


    tstrDropTable='''DROP TABLE IF EXISTS `{0}`.`{1}`;'''
    
    tstrCheckTableExsistsTemplate='''show tables from {0} like '{1}' '''

    tstr_UpdateBUH='''UPDATE `{0}`.`{1}`
       SET `ind_val` = {ind_val}
       WHERE `hash` = UNHEX('{hash}') AND `ind_code` = {ind_code} AND `act_date` = '{act_date}';'''

    tstr_InsertBUH='''INSERT INTO `{0}`.`{1}`
        (`hash`,
        `ind_code`,
        `ind_val`,
        `act_date`)
    VALUES
        (UNHEX('{hash}'),
         {ind_code},
         {ind_val},
         '{act_date}');'''


    tstrInsertCat='''INSERT INTO `{0}`.`{1}`
                (`hash`,
                `okved`,
                `okpo`,
                `measure`,
                `type`,
                `okopf`,
                `okfs`,
                `inn`,
                `name`)
            VALUES
                (UNHEX('{hash}'),
                '{okved}',
                {okpo},
                '{measure}',
                '{type}',
                '{okopf}',
                {okfs},
                '{inn}',
                '{name}');
           '''
           
    tstrUpdateCat='''UPDATE `{0}`.`{1}`
        SET
            `okved` = '{okved}',
            `okpo` = {okpo},
            `measure` = '{measure}',
            `type` = '{type}',
            `okopf` = {okopf},
            `okfs` = {okfs},
            `inn` = '{inn}',
            `name` = '{name}'
        WHERE `hash` = UNHEX('{hash}');'''


    def __init__(self):
        pass
    
    def createComm(self, tableName=cstr_DefHeaderTableName, databasename=cstr_DefDBName, 
                   direction=const_direction.header):
        if direction==const_direction.header:
            return self.tstrCreateHeaderTableTemplate.format(tableName)
        else:
            return self.tstrCreateBUHTableTemplate.format(tableName)

    def deleteComm(self, tableName=cstr_DefHeaderTableName, databasename=cstr_DefDBName):
        return self.tstrDropTable.format(databasename, tableName)

    def checkExistComm(self, tableName=cstr_DefHeaderTableName, databasename=cstr_DefDBName):
        return self.tstrCheckTableExsistsTemplate.format(databasename, tableName)

    def insertComm(self, tableName=cstr_DefHeaderTableName, databasename=cstr_DefDBName, 
        direction=const_direction.header, dictData=dict()):
        if direction==const_direction.header:
            return self.tstrInsertCat.format(databasename, tableName, **dictData)
        else:
            return self.tstr_InsertBUH.format(databasename, tableName, **dictData)

    def updateComm(self, tableName=cstr_DefHeaderTableName, databasename=cstr_DefDBName, 
        direction=const_direction.header, dictData=dict()):
        if direction==const_direction.header:
            return self.tstrUpdateCat.format(databasename, tableName, **dictData)
        else:
            return self.tstr_UpdateBUH.format(databasename, tableName, **dictData)


class SqliteCommand(sqlCommand):
    tstrCreateHeaderTableTemplate='''CREATE TABLE `{0}` ( 
    `hash` BLOB, 
    `okved` TEXT, 
    `okpo` TEXT, 
    `measure` TEXT, 
    `type` TEXT, 
    `okopf` TEXT, 
    `okfs` TEXT, 
    `inn` TEXT, 
    `name` TEXT, 
    PRIMARY KEY(`hash`) )'''

    tstrCreateBUHTableTemplate='''CREATE TABLE `{0}` (
    `hash`    BLOB,
    `ind_code`    INTEGER,
    `ind_val`    REAL,
    `act_date`    TEXT,
    PRIMARY KEY(`hash`,`ind_code`,`act_date`)
    );'''

    tstrDropTable='''DROP TABLE IF EXISTS `{1}`'''
    
    tstrInsertCat='''INSERT OR REPLACE INTO {1} 
                (hash, okved, okpo, measure, type, okopf,
                okfs, inn, name)
            VALUES
                ('{hash}',
                '{okved}',
                {okpo},
                '{measure}',
                '{type}',
                '{okopf}',
                {okfs},
                '{inn}',
                '{name}');
           '''

    tstr_InsertBUH='''INSERT OR REPLACE INTO {1}
        (hash,  ind_code,
        ind_val, act_date)
    VALUES
        ('{hash}',
         {ind_code},
         {ind_val},
         date('{act_date}'));'''

    tstrUpdateCat=''
    tstr_UpdateBUH=''
    tstrCheckTableExsistsTemplate='''select * from sqlite_master 
    where type='table' and name='{0}' '''
    
    def __init__(self):
        pass
    
    def createComm(self, tableName=cstr_DefHeaderTableName, databasename=cstr_DefDBName, 
                   direction=const_direction.header):
        if direction==const_direction.header:
            return self.tstrCreateHeaderTableTemplate.format(tableName)
        else:
            return self.tstrCreateBUHTableTemplate.format(tableName)

    def deleteComm(self, tableName=cstr_DefHeaderTableName, databasename=cstr_DefDBName):
        return self.tstrDropTable.format(databasename, tableName)

    def checkExistComm(self, tableName=cstr_DefHeaderTableName, databasename=cstr_DefDBName):
        return self.tstrCheckTableExsistsTemplate.format(tableName)

    def insertComm(self, tableName=cstr_DefHeaderTableName, databasename=cstr_DefDBName, 
        direction=const_direction.header, dictData=dict()):
        if direction==const_direction.header:
            return self.tstrInsertCat.format(databasename, tableName, **dictData)
        else:
            return self.tstr_InsertBUH.format(databasename, tableName, **dictData)

    def updateComm(self, tableName=cstr_DefHeaderTableName, databasename=cstr_DefDBName, 
        direction=const_direction.header, dictData=dict()):
        raise NotImplementedError
        
class SQLDB_connection:
    __conn=None
    __strDBName=''
    __sql_comm=None
    _database_type=None
    
    def __init__(self, str_server='', str_user=r'',
                str_password="", str_database='', db_type=''):
        self.__strDBName=str_database
        self._database_type=db_type
        if db_type=='MSSQL':
            self.__sql_comm=msSQLcommand()
            try:
                self.__conn=pymssql.connect(host=str_server, user=str_user, 
                                            password=str_password, database=str_database)
            except pymssql.OperationalError:
                print('Program terminated, connection failed - check user name and pass')
                exit()
            except pymssql.InterfaceError:
                print('Program terminated, connection failed - check server name')
                exit()
        elif db_type=='MySQL':
            self.__sql_comm=mySQLcommand()
            self.__conn=MySQLdb.connect(host=str_server, user=str_user, 
                                        passwd=str_password, db=str_database, charset='Cp1251')
        elif db_type=='sqlite':
            self.__sql_comm=SqliteCommand()
            self.__strDBName=str_database + '.sqlite'
            self.__conn=sqlite3.connect(self.__strDBName)
            
        else:
            print('ERROR: non-definite data provider')
            exit()
            
        #OperationalError 20002
        #InterfaceError
        
    @property
    def DBType(self):
        return self._database_type
        
    def __del__(self):
        try:
            self.__conn.close()
        except:
            pass

    def CheckTableExsist(self, strTableName):
        cursor=self.__conn.cursor()
        cursor.execute(self.__sql_comm.checkExistComm(tableName=strTableName, databasename=self.CurDataBaseName()))
        return (cursor.fetchone()!=None)

    def CreateTable(self, strTableName=cstr_DefHeaderTableName, strDirection=const_direction.header, bDeleteIfExsist=False):
        cursor=self.__conn.cursor()
        #print(self.__sql_comm.deleteComm(tableName=strTableName, databasename=self.CurDataBaseName()))
        #print(self.__sql_comm.createComm(tableName=strTableName, direction=strDirection, databasename=self.CurDataBaseName()))
        
        if bDeleteIfExsist:
            cursor.execute(self.__sql_comm.deleteComm(tableName=strTableName, databasename=self.CurDataBaseName()))
            self.CommitTransaction()
        elif self.CheckTableExsist(strTableName):
            print ('Таблица {0} уже существует'.format(strTableName))
            return True
        
        cursor.execute(self.__sql_comm.createComm(tableName=strTableName, direction=strDirection, databasename=self.CurDataBaseName()))
        self.__conn.commit()
        return self.CheckTableExsist(strTableName)
    
    def CurDataBaseName(self):
        return self.__strDBName
    
    def __MakeInsertCommand(self, strTableName, dictData):
        return 'INSERT INTO [{0}].[dbo].[{1}]'.format(self.CurDataBaseName(), strTableName) + ' ' + \
        '('+ ', '.join(map(lambda x: '['  + x + ']', dictData.keys())) + ')' + ' VALUES (' + \
        ', '.join(map(lambda x: "'{"  + x + "}'", dictData.keys())).format(**dictData) + ')'
    
    def __MakeUpdateCommand(self, strTableName, dictData, tKeyFieldName=('hash',)):
        return 'UPDATE [{0}].[dbo].[{1}] SET'.format(self.CurDataBaseName(), strTableName) + ' ' + \
        ', '.join("[{0}]='{1}'".format(x, y) for x, y in dictData.items() if x not in tKeyFieldName) + \
        " WHERE (" + ' AND '.join("[{0}]='{1}'".format(x, y) for x, y in dictData.items() if x in tKeyFieldName) + ")"
    
    def InsertIntoTable(self, strTableName, w_direction, dctData):
        cursor=self.__conn.cursor()
        try:
            #print (self.__MakeInsertBUHComm(strTableName, dctData))
            strC=self.__sql_comm.insertComm(tableName=strTableName, databasename=self.CurDataBaseName(), 
                                           direction=w_direction, dictData=dctData)
            #print(strC)
            cursor.execute(strC)
            return True
        except pymssql.IntegrityError as err:
            if err.args[0] == 2627:
                # primary key error
                return False
            
        except MySQLdb.connections.IntegrityError as err:
            if err.args[0]==1062:
                return False
        except:
            print(sys.exc_info()[0])
    
    def UpdateTable(self, strTableName, w_direction, dctData):
        cursor=self.__conn.cursor()
        strC=self.__sql_comm.updateComm(tableName=strTableName, databasename=self.CurDataBaseName(), 
                                        direction=w_direction, dictData=dctData)
        #print(strC)
        cursor.execute(strC)
        return True
    
    def CommitTransaction(self):
        self.__conn.commit()
        #print(strComm)
        
    
#mTest=SQLDB_connection(str_database='BUHO', db_type='sqlite')
#mTest.CreateTable(strTableName=cstr_DefHeaderTableName, 
#                  strDirection=const_direction.header, bDeleteIfExsist=True)
#print(mTest.DBType)
#dctHTest={'okopf': '47', 'hash': '0x85ad6286c644a1728cacc096dc272d23', 'okved': '40.11.1', 'okpo': '105638', 'okfs': '49', 'measure': '384', 'type': '2', 'name': 'Кузбасское Открытое акционерное общество энергетики и электрификации', 'inn': '4200000333'}
#mTest.InsertIntoTable(cstr_DefHeaderTableName, const_direction.header, dctHTest)

#dctBTest={'hash': '0x85ad6286c644a1728cacc096dc272d23', 'ind_code': '105638', 'ind_val': '49', 'act_date': '10-10-2016'}
#mTest.CreateTable(strTableName=cstr_DefBUHTableName, 
#                  strDirection=const_direction.data, bDeleteIfExsist=True)
#mTest.InsertIntoTable(cstr_DefBUHTableName, const_direction.data, dctBTest)
#mTest.CommitTransaction()

#mc=msSQLcommand()
#print(mc.createComm(tableName='Test', dababasename='DBT', direction=const_direction.header))
#print(mc.createComm(tableName='Test', dababasename='DBT', direction=const_direction.data))

#print(mc.deleteComm(tableName='Test', dababasename='DBT'))
#print(mc.checkExistComm(tableName='Test', dababasename='DBT'))

#print(mc.insertComm(tableName='Test', dababasename='DBT', direction=const_direction.header, dictData=dctHTest))
#print(mc.insertComm(tableName='Test', dababasename='DBT', direction=const_direction.data, dictData=dctBTest))

#strNN=', '.join("[{0}]='{1}'".format(x, y) for x, y in dctTest.items() if x != 'hash')
#print(cstr_HeaderErrorTableName)
#cn=SQLDB_connection()
#print(cn.MakeInsertBUHComm('TEST', dctBTest))

#cn.CreateHeaderTable('TestHeaderTable')

#print (cn.CheckTableExsist('4tablecreate'))
#print (cn.CreateHeaderTable('4tablecreate', bDeleteIfExsist=True))
#cn.InsertIntoTable(cstr_DefHeaderTableName, dctTest)


