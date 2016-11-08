#!/usr/bin/python3
# -*- coding: Cp1251 -*-

'''
Created on 14 ���. 2016 �.

@author: ggolyshev
'''

import FileDirWork
import argparse

dest_file=None
dest_db=None

def createParser():
    parser= argparse.ArgumentParser(prog='workbuh', description='''��������� ��������� ������
    ����� CSV ��������, �������� �� ���� �������� � ��������� ���� � ������������� ���������� � �������� �������. 
    ������ ������������ ���� � ���� (�����) CSV, ���� � ���� ������. � ���� ��������� ��������������� �������.
    ������ ���� ����������� � �����. �������, ���� ����������� � ���, ���� (��� ������������� ����������) 
    ����������� � ������� ������''', epilog='''(c) �����, ������ 2016. �����: golyshev  ''')
    dests=parser.add_subparsers(dest='destination', title='����� ����������', 
                                description='������ ������������ ��������. ��������� ��������:')

    parser.add_argument('-sf', '-s','--sfile', '--source_file', required=True, help='������������ �������� - ��� ��������� ����� � ������� ��������')
   
    parser.add_argument('-pc', '--print_cat', '--print_catalog', action='store_const', const=True, default=False, 
                           help='''������ �� ������ ���� ����� �������� �� ����� ��������. ������� = �������� ���� � �� ��������� (���, ���� � �.�.)''')

    parser.add_argument('-pd', '--print_d', '--print_data', action='store_const', const=True, default=False, 
                           help='''������ �� ������ ���� ����� ������ �� ����� ��������. ������ - ���� � ������������, � ������ ����� ��������� ���� �����������''')
    
    parser.add_argument('-ph', '--print_head', action='store_true', default=False, 
                           help='''������  �� ������ ������ ������ (����������) �� ����� ��������''')
    
    parser.add_argument('-b', '--buf', '--buffer', default=1000, type=int,
                           help='''������ ������ ������ � �������. �������� ������ �� �������� ������''')
    
    
    dest_file=dests.add_parser('file')
    dest_db=dests.add_parser('db')
    
    ''' file subcommand '''
    
    dest_file.add_argument('-mc', '--make_cat', '--make_catalog', action='store_const', const=True, default=False, 
                           help='''������� �� ����� �������� �������� � ������ ��� � ���� (�����) ''')

    dest_file.add_argument('-s', '--size', '--size_file', default=0, type=int, 
                           help='''���� != 0 - ������ ����� � ����� ������, ������ �������� � <s>-�����. ���� = 0 - ������ � ���� (�������) ����''')
    
   
    dest_db.add_argument('-t', '--table', default=None, required=True, 
                           help='''��� ������� �������''')
    dest_db.add_argument('-d', '--direction', choices=['data', 'cat'], default='cat', 
                           help='''����������� ������ (������� <-> ������)''')
    dest_db.add_argument('-c', '--command', choices=['create', 'ins', 'ins_update'], default='ins_update', 
                           help='''������� ������ � ����� ������: create - ������� ������� ��������, ins - �������� ������ �� �����, ins_update=�������� ������, ��������� ��������''')
    dest_db.add_argument('-cd', '--cur_date', default=None, 
                           help='''���� ������������ ������: None - ���� ������� �� �����-��������� csv''')
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
            print('���������� ������ ��� ������� �������')
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
    
    
    