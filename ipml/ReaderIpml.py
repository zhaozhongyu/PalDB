#-------------------------------------------------------------------------------
#!/usr/bin/env python3
# Project:     mysite
# Name:        ReaderIpml
# Purpose:     
# Author:      zhaozhongyu
# Created:     2/9/2017 8:40 PM
# Copyright:   (c) "zhaozhongyu" "2/9/2017 8:40 PM" 
# Licence:     <your licence>
# -*- coding:utf-8 -*-
#-------------------------------------------------------------------------------
from io import BufferedReader, BufferedRandom
from Paldb.ipml import StorageReader
import os, struct, pickle


class ReaderIpml:
    def __init__(self, file):
        if not isinstance(file, str) and not (isinstance(file, BufferedReader) or isinstance(file, BufferedRandom)):
            raise TypeError("invalid value type, expect BufferedWriter or string.")
        if (isinstance(file, BufferedReader) or isinstance(file, BufferedRandom)) and file.closed:
            raise ValueError("invalid value, file is closed.")
        elif isinstance(file, str) and not os.path.isfile(file):
            raise ValueError("invalid value, file is not exists.")
        if isinstance(file, BufferedReader) or isinstance(file, BufferedRandom):
            self.file = file
        else:
            self.file = open(file, "r+b")
        self.storage = StorageReader.StorageReader(self.file)
        self.__open = True


    def size(self):
        self.file.seek(0, 2)
        return self.file.tell()

    def get(self, key):
        self.__checkOpen()
        if isinstance(key, bytes):
            return self.storage.get(key)
        else:
            return self.storage.get(pickle.dumps(key))


    def close(self):
        self.__open = False
        self.file.close()

    def __checkOpen(self):
        if not self.__open:
            raise InterruptedError("file is error closed.")
