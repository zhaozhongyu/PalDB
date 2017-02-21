#-------------------------------------------------------------------------------
# Project:     Paldb
# Name:        Paldb
# Purpose:     
# Author:      zhaozhongyu
# Created:     2/9/2017 4:23 PM
# Copyright:   (c) "zhaozhongyu" "2/9/2017 4:23 PM" 
# Licence:     <your licence>
# -*- coding:utf-8 -*-
#-------------------------------------------------------------------------------
from Paldb.ipml import ReaderIpml, WriterIpml

class PalDB:
    def __init__(self):
        print("PalDB init.")



def createWriter(file):
    print("PalDB createWriter.")
    return WriterIpml.WriterIpml(file)


def createReader(file):
    print("PalDB createReader.")
    return ReaderIpml.ReaderIpml(file)
