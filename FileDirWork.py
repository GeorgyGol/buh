#!/usr/bin/python3
# -*- coding: Cp1251 -*-

'''
Created on 21 РѕРєС‚. 2016 Рі.

@author: ggolyshev
'''

import os
import os.path
import csv
import hashlib
import datetime
import SQLWork
import time

cstrEncoding='Cp1251'

class clsRW_CSV2WorkinkFormat:
    fl_sourceFile=None
    fl_DestFile=None
    rd_DictCSV=None
    
    cstr_HeaderFileName='catalog'
    
    __Headers=[]
    str_delimiter=','
    str_quote_char='|'
    str_newline=''
    
    __msc={'янв':'01', 'фев':'02', 'мар':'03', 
            'апр':'04', 'маи':'05', 'июн':'06', 'май':'05',
            'июл':'07', 'авг':'08', 'сен':'09', 
            'окт':'10', 'ноя':'11', 'дек':'12'}
    
    _hash_fld_name='hash'
    _date_fld_name='act_date'
    _indi_code_fld_name='ind_code'
    _indi_val_fld_name='ind_val'
    _fld_name_for_hash=('okpo', 'inn', _date_fld_name)
    _data_fld_keys=(_hash_fld_name,_indi_code_fld_name, _date_fld_name)
    
    iMaxWidth=0
    
    def __init__(self, strFullCSVFilePath):
        try:
            self.fl_sourceFile=open(strFullCSVFilePath, newline=self.str_newline, encoding=cstrEncoding)
        
        except FileNotFoundError:
            print('clsRW_CSV2WorkinkFormat -- {} not found'.format(strFullCSVFilePath))
        self.__Headers=self.__CSVReadHeaders(self.fl_sourceFile)
        
     
    def __del__(self):
        try:
            self.fl_sourceFile.close()
            self.fl_DestFile.close()
        except:
            pass
        
    def __CSVReadHeaders(self, flFileName):
        s_reader = csv.reader(flFileName, delimiter=self.str_delimiter, 
                          quotechar=self.str_quote_char, quoting=csv.QUOTE_NONE)
        for row in s_reader:
            self.__Headers=row
            return self.__Headers
     
    def GetCurDir(self):
        #s=self.fl_sourceFile.name.split('\\')
        #return '\\'.join(s[:-1])
        return os.path.split(self.fl_sourceFile.name)[0]

    def GetFileName(self):
        #s=self.fl_sourceFile.name.split('\\')
        #return s[-1]
        return os.path.split(self.fl_sourceFile.name)[-1]

    def GetWorkDir(self):
        return os.path.join(self.GetCurDir(), self.GetFileName().split('.')[-2])
        #return self.GetCurDir() + '\\' + self.GetFileName().split('.')[-2]
     
    def __MakeHash(self, dct_Source, encoding='ASCII'):
        strW='{0}{1}'.format(dct_Source[self._fld_name_for_hash[0]], 
                                    dct_Source[self._fld_name_for_hash[1]])
        m = hashlib.md5()
        m.update(bytearray(strW.encode(encoding)))
        #print(str(hash(strW)).__len__())
        #print (m.digest_size)
        return '0x'+m.hexdigest()
        #return hash(strW)
    
    def Headers(self):
        return self.__Headers
    
    def __CreateDir(self):
        try:
            os.mkdir(self.GetWorkDir())
        except FileExistsError:
            pass
        return self.GetWorkDir()
        
    def __OpenDestFile(self, isHead=True, strFileNum=''):
        strFileName=''
        if isHead:
            strFileName=self.cstr_HeaderFileName + strFileNum + '.csv'
        else:
            pass
        self.fl_DestFile=open(os.path.join(self.__CreateDir(), strFileName), 'w', 
                              newline='', encoding=cstrEncoding)
        #self.fl_DestFile=open(self.__CreateDir() + '\\' + strFileName, 'w', newline='')
        return self.fl_DestFile
    
    def __CreateDestDictWriter(self, fileDectID=None, fldnames=set()):
        dct_writer= csv.DictWriter(self.fl_DestFile, fieldnames=fldnames, 
                                        delimiter=self.str_delimiter,
                                        quotechar=self.str_quote_char, 
                                        quoting=csv.QUOTE_MINIMAL, extrasaction='ignore')
        dct_writer.writeheader()
        return dct_writer
    
    def __CreateSourceDictReader(self):
        self.rd_DictCSV=csv.DictReader(self.fl_sourceFile, fieldnames=self.__Headers)
        return self.rd_DictCSV
     
    def HeadersIdentity(self):
        return {x for x in self.__Headers if x.isalpha() and x!=self._date_fld_name}
    def DataIdentity(self):
        return {x for x in self.__Headers if x.isdigit() or x in self._fld_name_for_hash}
    
    def __CorrectFields(self, dctSource):
        def correctOkved(strOKVEDCode):
            splt=strOKVEDCode.split('.')
            for i in range(len(splt)):
                try:
                    #print(splt, self.__msc[splt[i]])
                    splt[i]=self.__msc[splt[i]]
                except KeyError:
                    pass
            return '.'.join(splt)
        
        def correctName(strSource):
            return strSource.replace(',', ' ')
        #print ('OKVED = {0} --> corrected = {1}'.format(dctSource['okved'], correctOkved(dctSource['okved'])))
        dctSource['okved']=correctOkved(dctSource['okved'])
        dctSource['name']=correctName(dctSource['name'])
        #if len(dctSource['name'])>self.iMaxWidth:
        #    self.iMaxWidth=len(dctSource['name'])
        
    def CreateCSVHeaderFiles(self, iStringInFile=0, intBuffSize=1000):
        lFileCnt=0
        lRowCnt=0
        flReader=self.__CreateSourceDictReader()
        flD=self.__OpenDestFile(isHead=True, strFileNum=str(lFileCnt))
        flWriter=self.__CreateDestDictWriter(fileDectID=flD, 
                                             fldnames=self.HeadersIdentity() | {self._hash_fld_name})
        strDectFileName=flD.name
        lstBuff=list()
        def WriteRows(writer=None, srcList=list(), strPrintStatus=''):
            writer.writerows(srcList)
            srcList.clear()
            print (strPrintStatus)
            #print ('!'*20 + 'Max lenght of NAME = ' + str(self.iMaxWidth) + '!'*20)

        for row in flReader:
            self.__CorrectFields(row)
            
            hdr_csv={k:v for k,v in row.items() if k in self.HeadersIdentity()}
            hdr_csv.setdefault(self._hash_fld_name, self.__MakeHash(hdr_csv))
            lstBuff.append(hdr_csv)
            lRowCnt+=1
            if lRowCnt % intBuffSize == 0:
                WriteRows(writer=flWriter, srcList=lstBuff, 
                          strPrintStatus='Writed {0} row in {1}'.format(lRowCnt, strDectFileName))

            if iStringInFile != 0 and lRowCnt >= iStringInFile:
                lRowCnt=0
                lFileCnt+=1
                WriteRows(writer=flWriter, srcList=lstBuff, 
                          strPrintStatus='Writed {0} row in {1}'.format(lRowCnt, strDectFileName))
                flD=self.__OpenDestFile(isHead=True, strFileNum=str(lFileCnt))
                flWriter=self.__CreateDestDictWriter(fileDectID=flD, fldnames=self.HeadersIdentity() | {self._hash_fld_name})
                strDectFileName=flD.name
        else:
            if lRowCnt % intBuffSize != 0:
                WriteRows(writer=flWriter, srcList=lstBuff, 
                          strPrintStatus='Writed {0} row in {1}'.format(lRowCnt, strDectFileName))
        print ('='*30 + 'All Done' + '='*30)
        return 0
    
    def CreateCSVHeaderFile(self, intBuffSize=1000):
        lRowCnt=0
        flReader=self.__CreateSourceDictReader()
        flD=self.__OpenDestFile(isHead=True)
        flWriter=self.__CreateDestDictWriter(fileDectID=flD, fldnames=self.HeadersIdentity() | {self._hash_fld_name})
        lstBuff=list()
        
        for row in flReader:
            self.__CorrectFields(row)
            hdr_csv={k:v for k,v in row.items() if k in self.HeadersIdentity()}
            hdr_csv.setdefault(self._hash_fld_name, self.__MakeHash(hdr_csv))
            lstBuff.append(hdr_csv)
            lRowCnt+=1
            if lRowCnt % intBuffSize == 0:
                flWriter.writerows(lstBuff)
                print ('Writed ', lRowCnt)
                lstBuff.clear()
        else:
            if lRowCnt % intBuffSize != 0:
                flWriter.writerows(lstBuff)
                print ('Writed ', lRowCnt)

        print ('='*30 + 'All Done' + '='*30)
        return 0
    '''
    def __PrintHeaderStatusInsertion(self, a, b, c, d):
        print ('Affected {0} --> inserted {1}, updated {2}, ' \
                       'insert into error_table {3} '.format(a, b, c, d))
    
    def WriteHeaderToDB(self, strDBTableName=SQLWork.cstr_DefHeaderTableName, 
                        intBuffSize=1000, bUpdateIfExsist=True, bNoErrorTable=False,
                        strHeaderErrorTableName=SQLWork.cstr_HeaderErrorTableName):
        
        print('Insert header to DB, work table = {0}, ' \
              'update_if_exsist = {1}, create_error_table = {2}, ' \
              ' error table = {3}'.format(strDBTableName, bUpdateIfExsist, not bNoErrorTable, 
                                       strHeaderErrorTableName))
       
        db_cn = SQLWork.SQLDB_connection()
        
        if not bUpdateIfExsist and not bNoErrorTable:
            db_cn.CreateHeaderTable(strTableName=strHeaderErrorTableName, bDeleteIfExsist=True)
        
        lRowCnt=0
        flReader=self.__CreateSourceDictReader()
        
        iInsertRow=0
        iUpdateRow=0
        iErrInsert=0
        
        for row in flReader:
            
            self.__CorrectFields(row)
            hdr_csv={k:v for k,v in row.items() if k in self.HeadersIdentity()}
            hdr_csv.setdefault(self.__hash_fld_name, self.__MakeHash(hdr_csv))
            
            if db_cn.InsertIntoCatTable(strDBTableName, hdr_csv):
                iInsertRow+=1
            elif bUpdateIfExsist:
                db_cn.UpdateCatTable(strDBTableName, hdr_csv, tKeyFieldName=(self.__hash_fld_name,))
                iUpdateRow+=1
            elif not bNoErrorTable:
                db_cn.InsertIntoCatTable(strHeaderErrorTableName, hdr_csv)
                iErrInsert+=1
                
            lRowCnt+=1

            if lRowCnt % intBuffSize == 0:
                db_cn.CommitTransaction()
                self.__PrintHeaderStatusInsertion(lRowCnt, iInsertRow, iUpdateRow, iErrInsert)
            
        else:
            if lRowCnt % intBuffSize != 0:
                db_cn.CommitTransaction()
                self.__PrintHeaderStatusInsertion(lRowCnt, iInsertRow, iUpdateRow, iErrInsert)
                
        print ('<>'*30 + 'All Done' + '<>'*30)
        return 0
    
    def WriteDataToDB(self, strDBTableName=SQLWork.cstr_DefBUHTableName, dtActDate=datetime.datetime.now(),
                        intBuffSize=100, bUpdateIfExsist=True, bNoErrorTable=False,
                        strErrorTableName=SQLWork.cstr_BUHErrorTableName):
        print('Insert data to DB, work table = {0}, ' \
              'update_if_exsist = {1}, create_error_table = {2}, ' \
              ' error table = {3}'.format(strDBTableName, bUpdateIfExsist, not bNoErrorTable, 
                                       strErrorTableName))
        db_cn = SQLWork.SQLDB_connection()
        
        if not bUpdateIfExsist and not bNoErrorTable:
            db_cn.CreateBUHTable(strTableName=strErrorTableName, bDeleteIfExsist=True)
        
        lRowCnt=0
        flReader=self.__CreateSourceDictReader()
        
        iInsertRow=0
        iUpdateRow=0
        iErrInsert=0
        cur_dt=None
        
        if dtActDate is not None:
            cur_dt=datetime.datetime.strptime(dtActDate, "%d.%m.%Y")
        
        for row in flReader:
            
            data_csv={k:v for k,v in row.items() if k in self.DataIdentity()}
            data_csv.setdefault(self.__hash_fld_name, self.__MakeHash(data_csv))
            
            if data_csv.get(self.__date_fld_name) is None:
                data_csv.setdefault(self.__date_fld_name, str(cur_dt))
            else:
                data_csv[self.__date_fld_name]=str(datetime.datetime.strptime(data_csv.get(self.__date_fld_name), "%Y%m%d"))
            if data_csv[self.__date_fld_name]=='None':
                print('Error! Empty current date!')
                exit()
            for k, v in data_csv.items():
                
                if v=='0':continue
                
                if k.isdigit():
                    dct={self.__date_fld_name:data_csv[self.__date_fld_name], 
                         self.__hash_fld_name:data_csv[self.__hash_fld_name], 
                         self.__indi_code_fld_name:k, self.__indi_val_fld_name:v}
                    #print (dct)
                    #continue
                    if db_cn.InsertIntoBUHTable(strDBTableName, dct):
                        iInsertRow+=1
                    elif bUpdateIfExsist:
                        db_cn.UpdateBUHTable(strDBTableName, dct)
                        iUpdateRow+=1
                    elif not bNoErrorTable:
                        db_cn.InsertIntoBUHTable(strErrorTableName, dct)
                        iErrInsert+=1

            lRowCnt+=1
            if lRowCnt % intBuffSize == 0:
                db_cn.CommitTransaction()
                self.__PrintHeaderStatusInsertion(lRowCnt, iInsertRow, iUpdateRow, iErrInsert)
        else:
            if lRowCnt % intBuffSize != 0:
                db_cn.CommitTransaction()
                self.__PrintHeaderStatusInsertion(lRowCnt, iInsertRow, iUpdateRow, iErrInsert)
        print ('//'*30 + 'All Done' + '\\'*30)
        
        return 0    
    
    def CreateDestTable(self, direction='cat', tableName='CATALOG'):
        db_cn = SQLWork.SQLDB_connection()
        if direction=='cat':
            db_cn.CreateHeaderTable(strTableName=tableName)
        else:
            db_cn.CreateBUHTable(strTableName=tableName)
        print ('Cоздана таблица {0}, тип = {1}'.format(tableName, direction))
'''

class cls2db(clsRW_CSV2WorkinkFormat):
    __db_cn=None
    
    def __init__(self, strFullCSVFilePath, server='', user='',
                 password="", database='', dbtype=''):
        clsRW_CSV2WorkinkFormat.__init__(self, strFullCSVFilePath)
        print(server, user, password, database, dbtype)
        self.__db_cn=SQLWork.SQLDB_connection(str_server=server, str_user=user,
                str_password=password, str_database=database, db_type=dbtype)
        
        
    def __PrintHeaderStatusInsertion(self, a, b, c, d):
        print ('Affected {0} --> inserted {1}, updated {2}, ' \
                       'insert into error_table {3} '.format(a, b, c, d))
    
    def WriteHeaderToDB(self, strDBTableName=SQLWork.cstr_DefHeaderTableName, 
                        intBuffSize=1000, bUpdateIfExsist=True, bNoErrorTable=False,
                        strHeaderErrorTableName=SQLWork.cstr_HeaderErrorTableName):
        
        print('Insert header to DB, work table = {0}, ' \
              'update_if_exsist = {1}, create_error_table = {2}, ' \
              ' error table = {3}'.format(strDBTableName, bUpdateIfExsist, not bNoErrorTable, 
                                       strHeaderErrorTableName))
       
        if not bUpdateIfExsist and not bNoErrorTable:
            self.__db_cn.CreateHeaderTable(strTableName=strHeaderErrorTableName, bDeleteIfExsist=True)
        
        lRowCnt=0
        flReader=super()._clsRW_CSV2WorkinkFormat__CreateSourceDictReader()
        
        iInsertRow=0
        iUpdateRow=0
        iErrInsert=0
        
        for row in flReader:
            
            super()._clsRW_CSV2WorkinkFormat__CorrectFields(row)
            hdr_csv={k:v for k,v in row.items() if k in clsRW_CSV2WorkinkFormat.HeadersIdentity(self)}
            hdr_csv.setdefault(clsRW_CSV2WorkinkFormat._hash_fld_name, super()._clsRW_CSV2WorkinkFormat__MakeHash(hdr_csv))
            
            if self.__db_cn.InsertIntoCatTable(strDBTableName, hdr_csv):
                iInsertRow+=1
            elif bUpdateIfExsist:
                self.__db_cn.UpdateCatTable(strDBTableName, hdr_csv)
                iUpdateRow+=1
            elif not bNoErrorTable:
                self.__db_cn.InsertIntoCatTable(strHeaderErrorTableName, hdr_csv)
                iErrInsert+=1
                
            lRowCnt+=1

            if lRowCnt % intBuffSize == 0:
                self.__db_cn.CommitTransaction()
                self.__PrintHeaderStatusInsertion(lRowCnt, iInsertRow, iUpdateRow, iErrInsert)
            
        else:
            if lRowCnt % intBuffSize != 0:
                self.__db_cn.CommitTransaction()
                self.__PrintHeaderStatusInsertion(lRowCnt, iInsertRow, iUpdateRow, iErrInsert)
                
        print ('<>'*30 + 'All Done' + '<>'*30)
        return 0
    
    def WriteDataToDB(self, strDBTableName=SQLWork.cstr_DefBUHTableName, dtActDate=datetime.datetime.now(),
                        intBuffSize=100, bUpdateIfExsist=True, bNoErrorTable=False,
                        strErrorTableName=SQLWork.cstr_BUHErrorTableName):
        print('Insert data to DB, work table = {0}, ' \
              'update_if_exsist = {1}, create_error_table = {2}, ' \
              ' error table = {3}'.format(strDBTableName, bUpdateIfExsist, not bNoErrorTable, 
                                       strErrorTableName))
        if not bUpdateIfExsist and not bNoErrorTable:
            self.__db_cn.CreateBUHTable(strTableName=strErrorTableName, bDeleteIfExsist=True)
        
        lRowCnt=0
        flReader=super()._clsRW_CSV2WorkinkFormat__CreateSourceDictReader()
        
        iInsertRow=0
        iUpdateRow=0
        iErrInsert=0
        cur_dt=None
        
        if dtActDate is not None:
            cur_dt=datetime.datetime.strptime(dtActDate, "%d.%m.%Y")
        
        for row in flReader:
            
            data_csv={k:v for k,v in row.items() if k in clsRW_CSV2WorkinkFormat.DataIdentity(self)}
            data_csv.setdefault(clsRW_CSV2WorkinkFormat._hash_fld_name, super()._clsRW_CSV2WorkinkFormat__MakeHash(data_csv))
            
            if data_csv.get(clsRW_CSV2WorkinkFormat._date_fld_name) is None:
                data_csv.setdefault(clsRW_CSV2WorkinkFormat._date_fld_name, str(cur_dt))
            else:
                data_csv[clsRW_CSV2WorkinkFormat._date_fld_name]=str(datetime.datetime.strptime(data_csv.get(clsRW_CSV2WorkinkFormat._date_fld_name), "%Y%m%d"))
            if data_csv[clsRW_CSV2WorkinkFormat._date_fld_name]=='None':
                print('Error! Empty current date!')
                exit()
            for k, v in data_csv.items():
                
                if v=='0':continue
                
                if k.isdigit():
                    dct={clsRW_CSV2WorkinkFormat._date_fld_name:data_csv[clsRW_CSV2WorkinkFormat._date_fld_name], 
                         clsRW_CSV2WorkinkFormat._hash_fld_name:data_csv[clsRW_CSV2WorkinkFormat._hash_fld_name], 
                         clsRW_CSV2WorkinkFormat._indi_code_fld_name:k, clsRW_CSV2WorkinkFormat._indi_val_fld_name:v}
                    #print (dct)
                    #continue
                    if self.__db_cn.InsertIntoBUHTable(strDBTableName, dct):
                        iInsertRow+=1
                    elif bUpdateIfExsist:
                        self.__db_cn.UpdateBUHTable(strDBTableName, dct)
                        iUpdateRow+=1
                    elif not bNoErrorTable:
                        self.__db_cn.InsertIntoBUHTable(strErrorTableName, dct)
                        iErrInsert+=1

            lRowCnt+=1
            if lRowCnt % intBuffSize == 0:
                self.__db_cn.CommitTransaction()
                self.__PrintHeaderStatusInsertion(lRowCnt, iInsertRow, iUpdateRow, iErrInsert)
        else:
            if lRowCnt % intBuffSize != 0:
                self.__db_cn.CommitTransaction()
                self.__PrintHeaderStatusInsertion(lRowCnt, iInsertRow, iUpdateRow, iErrInsert)
        print ('//'*30 + 'All Done' + '\\'*30)
        
        return 0    
    
    def CreateDestTable(self, direction='cat', tableName='CATALOG'):
        
        if direction=='cat':
            self.__db_cn.CreateHeaderTable(strTableName=tableName)
        else:
            self.__db_cn.CreateBUHTable(strTableName=tableName)
        print ('Cоздана таблица {0}, тип = {1}'.format(tableName, direction))


if __name__ == '__main__':
    #MakeReadableCSV('D:\Drag\INDA\data-20140829t000000-structure-20121231t000000.csv')
    #cls=clsRW_CSV2WorkinkFormat('/home/egor/csv_source/data-20140829t000000-structure-20121231t000000.csv')
    
    #cls.WriteHeaderToDB(strDBTableName='TestHeaderTable', bUpdateIfExsist=False)
    #cls.CreateDestTable(direction='data', tableName='BUH')
    #time.sleep(30)
    #cls.WriteDataToDB(bUpdateIfExsist=False, bNoErrorTable=True, dtActDate="29.08.2014", strDBTableName='BUH')
    #print(cls.HeadersIdentity())
    #cls.CreateCSVHeaderFiles(iStringInFile=50000)
    #cls.CreateCSVHeaderFile()
    pass

