#!/usr/bin/python3
# -*- coding: Cp1251 -*-

'''
Created on 14 окт. 2016 г.

@author: ggolyshev
'''

import FileDirWork
import argparse

dest_file=None
dest_db=None

def createParser():
    parser= argparse.ArgumentParser(prog='workbuh', description='''Программа выполняет разбор
    файла CSV Росстата, выбирает из него названия и параметры фирм и разворачивает показатели в линейную таблицу. 
    Данные записываются либо в файл (файлы) CSV, либо в базу данных. В базе создаются соответствующие таблицы.
    Данные либо добавляются в соотв. таблицы, либо обновляются в них, либо (при невозможности добавления) 
    добавляются в таблицы ошибок''', epilog='''(c) ЦМАКП, ноябрь 2016. Автор: golyshev  ''')
    dests=parser.add_subparsers(dest='destination', title='место назначения', 
                                description='Второй обязательный параметр. Возможные значения:')

    parser.add_argument('-sf', '-s','--sfile', '--source_file', required=True, help='Обязательный параметр - имя исходного файла с данными Росстата')
   
    parser.add_argument('-pc', '--print_cat', '--print_catalog', action='store_const', const=True, default=False, 
                           help='''Печать на экране имен полей каталога из файла Росстата. Каталог = названия фирм и их параметры (инн, окпо и т.д.)''')

    parser.add_argument('-pd', '--print_d', '--print_data', action='store_const', const=True, default=False, 
                           help='''Печать на экране имен полей данных из файла Росстата. Данные - поля с показателями, в именах полей ожидаются коды показателей''')
    
    parser.add_argument('-ph', '--print_head', action='store_true', default=False, 
                           help='''Печать  на экране первой строки (заголовков) из файла Росстата''')
    
    parser.add_argument('-b', '--buf', '--buffer', default=1000, type=int,
                           help='''Размер буфера записи в строках. Величина влияет на скорость записи''')
    
    
    dest_file=dests.add_parser('file')
    dest_db=dests.add_parser('db')
    
    ''' file subcommand '''
    
    dest_file.add_argument('-mc', '--make_cat', '--make_catalog', action='store_const', const=True, default=False, 
                           help='''Выборка из файла Росстата КАТАЛОГА и запись его в файл (файлы) ''')

    dest_file.add_argument('-s', '--size', '--size_file', default=0, type=int, 
                           help='''если != 0 - запись будет в серию файлов, каждый размером в <s>-строк. Если = 0 - запись в один (большой) файл''')
    
   
    dest_db.add_argument('-t', '--table', default=None, required=True, 
                           help='''Имя целевой таблицы''')
    dest_db.add_argument('-d', '--direction', choices=['data', 'cat'], default='cat', 
                           help='''Направление работы (каталог <-> данные)''')
    dest_db.add_argument('-c', '--command', choices=['create', 'ins', 'ins_update'], default='ins_update', 
                           help='''Команды работы с базой данных: create - создать целевую таблбицу, ins - вставить данные из файла, ins_update=вставить данные, имеющиеся обновить''')
    dest_db.add_argument('-cd', '--cur_date', default=None, 
                           help='''Дата актуализации данных: None - дата берется из файла-источника csv''')
    return parser


strWorkFile = 'D:\Drag\INDA'

if __name__ == '__main__':
    parser=createParser()
    ns=parser.parse_args()
    
    print ('Work with ', ns.sfile)
    
    cls=FileDirWork.clsRW_CSV2WorkinkFormat(ns.sfile)
    print(ns)
    if ns.print_head:
        print ('CSV file headers = ', cls.Headers())
        
    if ns.print_cat:
        print ('Catalog headers = ', cls.HeadersIdentity())
        
    if ns.print_d:
        print ('Data headers =  ', cls.DataIdentity())
    if ns.destination==None:
        print()
        print('Use key --help for print help')
        exit()
    elif ns.destination=='file':
        
        if ns.size == 0:
            cls.CreateCSVHeaderFile(ns.buf)
        else:
            cls.CreateCSVHeaderFiles(ns.size, ns.buf)
    else:
        strTableName=ns.table
        if strTableName==None:
            print('Необходимо ввести имя целевой таблицы')
            exit()
            
        if ns.direction=='cat':
            if ns.command=='ins':
                cls.WriteHeaderToDB(strDBTableName=strTableName, 
                        intBuffSize=ns.buf, bUpdateIfExsist=False)
            elif ns.command=='ins_update':
                cls.WriteHeaderToDB(strDBTableName=strTableName, 
                        intBuffSize=ns.buf, bUpdateIfExsist=True)
            elif ns.command=='create':
                cls.CreateDestTable(direction=ns.direction, tableName=strTableName)
        elif ns.direction=='data':
            if ns.command=='ins':
                cls.WriteDataToDB(strDBTableName=strTableName, dtActDate=ns.cur_date, intBuffSize=ns.buf, 
                                  bUpdateIfExsist=False, bNoErrorTable=True)
            elif ns.command=='ins_update':
                cls.WriteDataToDB(strDBTableName=strTableName, dtActDate=ns.cur_date, intBuffSize=ns.buf, 
                                  bUpdateIfExsist=True, bNoErrorTable=True)
            elif ns.command=='create':
                cls.CreateDestTable(direction=ns.direction, tableName=strTableName)
        else:
            print ('Error in command string params, use --help for help')
            
        print(ns)
    
    
    