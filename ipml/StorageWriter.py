#-------------------------------------------------------------------------------
#!/usr/bin/env python3
# Project:     mysite
# Name:        StorageWriter
# Purpose:     
# Author:      zhaozhongyu
# Created:     2/9/2017 9:06 PM
# Copyright:   (c) "zhaozhongyu" "2/9/2017 9:06 PM" 
# Licence:     <your licence>
# -*- coding:utf-8 -*-
#-------------------------------------------------------------------------------
import tempfile, struct, os, io
from Paldb.ipml import LongPacker
class StorageWriter:
    def __init__(self, file):
        if isinstance(file, io.BufferedRandom):
            self.file = file
        else:
            self.file = open(file, "r+b") #创建的文件
        self.tempdir = tempfile.TemporaryDirectory("paldbtempwriter")
        self.indexStreams = []
        self.dataStreams = []
        self.indexFiles = []
        self.dataFiles = []
        self.lastValues = []
        self.lastValueLength = []
        self.dataLengths = []
        #Max offset length
        self.maxOffsetLengths = []
        # number of key
        self.keyCount = 0
        self.valueCount = 0
        self.collisions = 0

        self.keyCounts = []
        self.longPacker = LongPacker
        self.LoadFactor = 0.75
        self.indexesLength = 0



    def __getIndexStream(self, keyLength):
        if len(self.indexStreams) <= keyLength:
            self.indexStreams.extend(range(len(self.indexStreams), keyLength+1))
            self.indexFiles.extend(range(len(self.indexStreams), keyLength+1))
            self.keyCounts.extend([0 for i in range(len(self.dataLengths), keyLength+1)])
            self.maxOffsetLengths.extend([0 for i in range(len(self.dataLengths), keyLength+1)])
            self.lastValues.extend(range(len(self.lastValues), keyLength+1))
            self.dataLengths.extend([0 for i in range(len(self.dataLengths), keyLength+1)])
            self.lastValueLength.extend([0 for i in range(len(self.lastValueLength), keyLength+1)])
        dos = self.indexStreams[keyLength]
        if not isinstance(dos, tempfile._TemporaryFileWrapper):
            dos = tempfile.TemporaryFile(dir=self.tempdir.name, mode="r+b")
            self.indexStreams[keyLength] = dos
            self.dataLengths[keyLength] = self.dataLengths[keyLength] + 1
        return dos

    def __getDataStream(self, keyLength):
        if len(self.dataStreams) <= keyLength:
            self.dataStreams.extend(range(len(self.dataStreams), keyLength+1))

        dos = self.dataStreams[keyLength]
        if not isinstance(dos, tempfile._TemporaryFileWrapper):
            dos = tempfile.TemporaryFile(dir=self.tempdir.name, mode="r+b")
            self.dataStreams[keyLength] = dos
            # Write one byte so the zero offset is reserved
            dos.write('0'.encode())
        return dos


    def put(self, key, value):
        keyLength = len(key)
        indexStream = self.__getIndexStream(keyLength)
        #Write key
        indexStream.write(key)

        #Check if the value is identical to the last inserted
        lastValue = self.lastValues[keyLength]
        dataLength = self.dataLengths[keyLength]
        if lastValue == value:  #write the pointer number to indexStream for seek
            dataLength -= self.lastValueLength[keyLength]

        #Write offset and record max offset length
        offsetLength = self.longPacker.packInt(indexStream, dataLength)
        self.maxOffsetLengths[keyLength] = max(offsetLength, self.maxOffsetLengths[keyLength])

        if not lastValue == value:
            dataStream = self.__getDataStream(keyLength)
            # Write size and value
            valueSize = self.longPacker.packInt(dataStream, len(value))
            dataStream.write(value)
            #update data length
            self.dataLengths[keyLength] += valueSize + len(value)
            #Update last value
            self.lastValues[keyLength] = value
            print(len(self.lastValueLength))
            self.lastValueLength[keyLength] = valueSize + len(value)
            self.valueCount += 1
        self.keyCounts[keyLength] += 1
        self.keyCount += 1




    def close(self):
        fileTomerge = []
        metadataFile = tempfile.TemporaryFile(dir=self.tempdir.name, mode="r+b")
        self.__writeMetadata(metadataFile)
        files = [metadataFile,]
        for keyLength in range(len(self.indexStreams)):
            if isinstance(self.indexStreams[keyLength], tempfile._TemporaryFileWrapper):

                files.append(self.__buildIndex(keyLength))


        for datafile in self.dataStreams:
            files.append(datafile)

        self.__mergeFile(files)
        self.file.close() #关闭文件流
        self.tempdir.cleanup() #清除临时文件


    def __mergeFile(self, files):
        for file in files:
            if isinstance(file, tempfile._TemporaryFileWrapper):
                print("merging {0} ".format(file.name))
                file.seek(0)  #将指针移动到开头
                b = file.read(2048)
                while len(b) >= 2048:
                    self.file.write(b)
                    b = file.read(2048)
                self.file.write(b)
                file.close() # 关闭并删除临时文件
            else:
                print("Skip merging file {0} because it doesn't exist.".format(file))
        print("merging file ends.")




    def __writeMetadata(self, dataOutputStream):
        print("write MetaData begin.")
        keyLengthCount = self.__getNumKeyCount()
        maxKeyLength = len(self.keyCounts) - 1
        # Write size (number of keys), each of int for 4 byte
        self.__writeInt(dataOutputStream, self.keyCount) #写入共有多少个key
        self.__writeInt(dataOutputStream, keyLengthCount) #写入key的长度种数
        self.__writeInt(dataOutputStream, maxKeyLength)  #写入最长key的长度
        datasLength = 0
        #for each key length
        for i in range(0, len(self.keyCounts)):
            if self.keyCounts[i] > 0:
                self.__writeInt(dataOutputStream, i)  #写入当前key长度
                self.__writeInt(dataOutputStream, self.keyCounts[i]) #写入当前key长度有几个key
                slots = round(self.keyCounts[i] / self.LoadFactor)  # 留出空位Hash使用
                self.__writeInt(dataOutputStream, slots) #写入当前key长度的有多少个位置
                offsetLength = self.maxOffsetLengths[i] #indexfile中偏移量的最大值
                self.__writeInt(dataOutputStream, offsetLength + i) #写入每个位置的byte长度
                self.__writeInt(dataOutputStream, self.indexesLength) #写入当前key在index文件的指针位置
                self.indexesLength += (i + offsetLength) * slots #当前key在index文件的指针位置
                self.__writeInt(dataOutputStream, datasLength) #写入当前key长度对应的data在文件中的指针初始位置
                datasLength += self.dataLengths[i] #更新当前key长度对应的data在文件的指针位置到下一个key的开头
        indexOffset = dataOutputStream.tell() + 8 #8 means two int, 标识meta数据即将结束, 预留的8字节写入index和data的位置
        self.__writeInt(dataOutputStream, indexOffset)  #最后写入index的偏移
        self.__writeInt(dataOutputStream, indexOffset + self.indexesLength) #写入indexes的结尾指针位置, 即data的位置
        print("write metadata end.")


    def __buildIndex(self, keyLength):
        count = self.keyCounts[keyLength]
        print("key Count:", count)
        slots = round(count / self.LoadFactor) #当前key的hash表总大小
        offsetLength = self.maxOffsetLengths[keyLength] #value的指针位置经过packInt转换后需要的位数最大值
        slotSize = keyLength + offsetLength  #每个key需要的位置大小

        #init index
        indexFile = tempfile.TemporaryFile(dir=self.tempdir.name, mode="r+b") #读写模式打开文件
        tempIndexStream = self.indexStreams[keyLength] #获取原先的key index文件
        tempIndexStream.seek(0)
        #read all keys
        for i in range(count):
            keyBuffer = tempIndexStream.read(keyLength) #读取key
            offset = self.longPacker.unpackInt(tempIndexStream) #读取offset

            h = hash(keyBuffer)
            collision = False
            for probe in range(count):
                slot = (h + probe) % slots #计算当前key的hash位置，如果当前key的位置已被其他key占用, 则查找下一个位置
                indexFile.seek(slot * slotSize)
                slotBuffer = indexFile.read(keyLength) #读取当前位置的key
                found = self.longPacker.unpackInt(indexFile)
                if found == 0: #当前的slot为空
                    indexFile.seek(slot * slotSize)
                    indexFile.write(keyBuffer)
                    self.longPacker.packInt(indexFile, offset)
                    break
                else :
                    collision = True
                    if keyBuffer == slotBuffer:
                        raise RuntimeError("A duplicate key has been found for for key bytes {}".format(keyBuffer.decode()))
                if collision:
                    self.collisions += 1;
        if isinstance(tempIndexStream, tempfile._TemporaryFileWrapper):
            tempIndexStream.close()

        print("Built index file {} success".format(indexFile.name))
        indexFile.seek(0)
        print("index File:", indexFile.read())
        return indexFile



    def __writeInt(self, dataOutputStream, key):
        dataOutputStream.write(struct.pack(">I", key))

    def __getNumKeyCount(self):
        res = 0
        for i in range(0, len(self.keyCounts)):
            if self.keyCounts[i] != 0:
                res += 1
        return res
