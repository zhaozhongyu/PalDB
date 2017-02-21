#-------------------------------------------------------------------------------
#!/usr/bin/env python3
# Project:     mysite
# Name:        WriterIpml
# Purpose:     
# Author:      zhaozhongyu
# Created:     2/9/2017 8:41 PM
# Copyright:   (c) "zhaozhongyu" "2/9/2017 8:41 PM" 
# Licence:     <your licence>
# -*- coding:utf-8 -*-
#-------------------------------------------------------------------------------
from  io import BufferedWriter
import os, pickle
from Paldb.ipml import StorageWriter

class WriterIpml:
    def __init__(self, file):
        if not isinstance(file, str) and not isinstance(file, BufferedWriter):
            raise TypeError("invalid value type, expect BufferedWriter or string.")
        if isinstance(file, BufferedWriter) and file.closed:
            raise ValueError("invalid value, BufferedWriter is closed.")
        elif isinstance(file, str) and os.path.isdir(file):
            raise ValueError("invalid value, file name is a exists dir.")
        if isinstance(file, BufferedWriter):
            self.file = file
        else:
            self.file = open(file, "w+b")
        self.storage = StorageWriter.StorageWriter(file)
        self.opened = True


    def __checkOpen(self):
        if not self.opened:
            raise InterruptedError("The store is closed.")


    def put(self, key, value):
        '''
            @:param key bytes
            @:param value bytes
            '''
        self.__checkOpen()
        if isinstance(key, bytes):
            self.storage.put(key, value)
        else:
            key = pickle.dumps(key)
            value = pickle.dumps(value)
            self.storage.put(key, value)




    def putAll(self, keys, values):
        if (not ((isinstance(keys, set) and isinstance(values, set)) or (isinstance(keys, list) and isinstance(values, list)))):
            raise TypeError("Type Error, Keys and values must be set or list.")
        if (len(keys) != len(values)):
            raise ValueError("The length of keys and values must be equal.")
        if len(keys) < 1:
            raise ValueError("The length of keys must large than 0.")
        for i in (0, len(keys)):
            self.put(keys[i], values[i])


    def close(self):
        self.__checkOpen()
        if self.file.closed:
            return
        self.opened = False
        self.file.close()
        self.storage.close()
