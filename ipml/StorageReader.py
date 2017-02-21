#-------------------------------------------------------------------------------
#!/usr/bin/env python3
# Project:     mysite
# Name:        StorageReader
# Purpose:     
# Author:      zhaozhongyu
# Created:     2/17/2017 11:40 AM
# Copyright:   (c) "zhaozhongyu" "2/17/2017 11:40 AM" 
# Licence:     <your licence>
# -*- coding:utf-8 -*-
#-------------------------------------------------------------------------------
from io import BufferedRandom, BufferedReader
from Paldb.ipml import LongPacker
import struct, pickle
class StorageReader():
    def __init__(self, file):
        if not isinstance(file, BufferedRandom) and not isinstance(file, BufferedReader):
            raise ValueError("file type error. Need a BufferedRandom or BufferedReader. Current file is {}".format(type(file)))
        print("opening file {}.".format(file.name))
        self.__file = file
        # begin read meta data
        self.__keyCount = self.__readInt()
        self.__keyLengthCount = self.__readInt()
        self.__maxKeyLength = self.__readInt()
        self.indexOffsets = [0 for i in range(self.__maxKeyLength+1)]
        self.dataOffsets = [0 for i in range(self.__maxKeyLength+1)]
        self.keyCounts = [0 for i in range(self.__maxKeyLength+1)]
        self.slots = [0 for i in range(self.__maxKeyLength+1)]
        self.slotSize = [0 for i in range(self.__maxKeyLength+1)]
        maxSlotSize = 0
        for i in range(self.__keyLengthCount):
            keyLength = self.__readInt()
            self.keyCounts[keyLength] = self.__readInt()
            self.slots[keyLength] = self.__readInt()
            self.slotSize[keyLength] = self.__readInt()
            self.indexOffsets[keyLength] = self.__readInt()
            self.dataOffsets[keyLength] = self.__readInt()
            maxSlotSize = max(maxSlotSize, self.slotSize[keyLength])

        self.__indexOffset = self.__readInt()
        self.__dataOffset = self.__readInt()


    def get(self, key):
        if not isinstance(key, bytes):
            raise TypeError("Key type is wrong, need bytes, get {}".format(type(key)))
        keyLength = len(key)
        if keyLength > len(self.slots) or self.keyCounts[keyLength] == 0:
            return None
        h = hash(key)
        for probe in range(self.slots[keyLength]):
            slot = (h + probe) % self.slots[keyLength]
            self.__file.seek(self.__indexOffset + self.indexOffsets[keyLength]+ slot * self.slotSize[keyLength])
            keyBuffer = self.__file.read(keyLength)
            if keyBuffer == key:
                offset = LongPacker.unpackInt(self.__file)
                self.__file.seek(self.__dataOffset+self.dataOffsets[keyLength]+offset) #将指针移动到data的起点
                dataLength = LongPacker.unpackInt(self.__file)  #读取data长度
                value = self.__file.read(dataLength)  #读取data
                value = pickle.loads(value)
                return value
            else:
                continue


    def __readInt(self):
        b = self.__file.read(4)
        try:
            i = struct.unpack(">I", b)[0]
        except Exception:
            self.__file.close()
        return i
