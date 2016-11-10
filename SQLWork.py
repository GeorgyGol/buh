#!/usr/bin/python3
# -*- coding: Cp1251 -*-

'''
Created on 21 окт. 2016 г.

@author: ggolyshev
'''
import pymssql
import MySQLdb

cstr_DefHeaderTableName='CATALOG'
cstr_HeaderErrorTableName=cstr_DefHeaderTableName + '_err_insert'
cstr_DefBUHTableName='BUH_DATA'
cstr_BUHErrorTableName=cstr_DefBUHTableName + '_err_insert'

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

tstrGetTableFieldsNameTemplate=r'''SELECT COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE (TABLE_NAME = N'{0}') AND (COLUMN_NAME LIKE '[0-9]%')
    ''' 

tstrCheckTableExsistsTemplate='''SELECT  object_id FROM sys.tables
    WHERE (name = '{0}')
'''

tstrDropTable='''IF  EXISTS (SELECT * FROM sys.objects 
WHERE object_id = OBJECT_ID(N'[dbo].[{0}]') AND type in (N'U'))
DROP TABLE [dbo].[{0}]
'''

tstr_UpdateBUF_Dict='''UPDATE [{0}].[dbo].[{1}]
   SET [ind_val] = {ind_val}
 WHERE ([hash] = {hash}) AND ([ind_code] = {ind_code}) AND ([act_date] = {act_date})'''

tstr_InsertBUH_Dict='''INSERT INTO [{0}].[dbo].[{1}]
           ([hash]
           ,[ind_code]
           ,[ind_val]
           ,[act_date])
     VALUES
           ({hash}
           ,{ind_code}
           ,{ind_val}
           ,'{act_date}') '''


tstrInsertIntoCat='''INSERT INTO [{0}].[dbo].[{1}]
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
           ,{hash}
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
 WHERE [hash]={hash}
'''

class SQLDB_connection:
    __conn=None
    __strDBName=''
    
    def __init__(self, str_server='', str_user=r'',
                str_password="", str_database='', db_type=''):
        if db_type=='MSSQL':
            try:
                self.__conn=pymssql.connect(host=str_server, user=str_user, password=str_password, database=str_database)
                self.__strDBName=str_database
            except pymssql.OperationalError:
                print('Program terminated, connection failed - check user name and pass')
                exit()
            except pymssql.InterfaceError:
                print('Program terminated, connection failed - check server name')
                exit()
        else:
            self.__conn=MySQLdb.connect(host=str_server, user=str_user, passwd=str_password, db=str_database)
            
        #OperationalError 20002
        #InterfaceError
        
    def __del__(self):
        try:
            self.__conn.close()
        except:
            pass

    def CheckTableExsist(self, strTableName):
        cursor=self.__conn.cursor()
        cursor.execute(tstrCheckTableExsistsTemplate.format(strTableName))
        return (cursor.fetchone()!=None)

    def CreateHeaderTable(self, strTableName=cstr_DefHeaderTableName, bDeleteIfExsist=False):
        cursor=self.__conn.cursor()
        if bDeleteIfExsist:
            cursor.execute(tstrDropTable.format(strTableName))
        elif self.CheckTableExsist(strTableName):
            return True
        cursor.execute(tstrCreateHeaderTableTemplate.format(strTableName))
        self.__conn.commit()
        return self.CheckTableExsist(strTableName)
    
    def CreateBUHTable(self, strTableName=cstr_DefBUHTableName, bDeleteIfExsist=False):
        cursor=self.__conn.cursor()
        if bDeleteIfExsist:
            cursor.execute(tstrDropTable.format(strTableName))
        elif self.CheckTableExsist(strTableName):
            return True
        cursor.execute(tstrCreateBUHTableTemplate.format(strTableName))
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
        
    
    def __MakeUpdateBUHComm(self, strTableName, dictData):
        return tstr_UpdateBUF_Dict.format(self.CurDataBaseName(), strTableName, **dictData)
    
    def __MakeInsertBUHComm(self, strTableName, dictData):
        return tstr_InsertBUH_Dict.format(self.CurDataBaseName(), strTableName, **dictData)
        
    def __MakeInsertCatComm(self, strTableName, dictData):
        return tstrInsertIntoCat.format(self.CurDataBaseName(), strTableName, **dictData)

    def __MakeUpdateCatComm(self, strTableName, dictData):
        return tstrUpdateCat.format(self.CurDataBaseName(), strTableName, **dictData)

    
    def InsertIntoTable(self, strTableName, dctData):
        cursor=self.__conn.cursor()
        try:
            cursor.execute(self.__MakeInsertCommand(strTableName, dctData))
            return True
        except pymssql.IntegrityError as err:
            if err.args[0] == 2627:
                # primary key error
                return False
            
    def UpdateTable(self, strTableName, dictData, tKeyFieldName=('hash',)):
        cursor=self.__conn.cursor()
        cursor.execute(self.__MakeUpdateCommand(strTableName, dictData, tKeyFieldName))
        return True
            
    def UpdateBUHTable(self, strTableName, dictData):
        cursor=self.__conn.cursor()
        cursor.execute(self.__MakeUpdateBUHComm(strTableName, dictData))
        return True
    
    def InsertIntoBUHTable(self, strTableName, dctData):
        cursor=self.__conn.cursor()
        try:
            #print (self.__MakeInsertBUHComm(strTableName, dctData))
            cursor.execute(self.__MakeInsertBUHComm(strTableName, dctData))
            return True
        except pymssql.IntegrityError as err:
            if err.args[0] == 2627:
                # primary key error
                return False
    
    def UpdateCatTable(self, strTableName, dictData):
        cursor=self.__conn.cursor()
        cursor.execute(self.__MakeUpdateCatComm(strTableName, dictData))
        return True
    
    def InsertIntoCatTable(self, strTableName, dctData):
        cursor=self.__conn.cursor()
        try:
            #print (self.__MakeInsertCatComm(strTableName, dctData))
            cursor.execute(self.__MakeInsertCatComm(strTableName, dctData))
            return True
        except pymssql.IntegrityError as err:
            if err.args[0] == 2627:
                # primary key error
                return False
    
    
    def CommitTransaction(self):
        self.__conn.commit()
        #print(strComm)
        
    
#dctHTest={'okopf': '47', 'hash': '85ad6286c644a1728cacc096dc272d23', 'okved': '40.11.1', 'okpo': '105638', 'okfs': '49', 'measure': '384', 'type': '2', 'name': 'Кузбасское Открытое акционерное общество энергетики и электрификации', 'inn': '4200000333'}
#dctBTest={'hash': '85ad6286c644a1728cacc096dc272d23', 'ind_code': '105638', 'ind_val': '49', 'act_date': '10-10-2016'}
#strNN=', '.join("[{0}]='{1}'".format(x, y) for x, y in dctTest.items() if x != 'hash')
#print(cstr_HeaderErrorTableName)
#cn=SQLDB_connection()
#print(cn.MakeInsertBUHComm('TEST', dctBTest))

#cn.CreateHeaderTable('TestHeaderTable')

#print (cn.CheckTableExsist('4tablecreate'))
#print (cn.CreateHeaderTable('4tablecreate', bDeleteIfExsist=True))
#cn.InsertIntoTable(cstr_DefHeaderTableName, dctTest)


