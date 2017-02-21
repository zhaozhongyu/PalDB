#-------------------------------------------------------------------------------
#!/usr/bin/env python3
# Project:     mysite
# Name:        LongPacker
# Purpose:     
# @Author:      zhaozhongyu
# Created:     2/10/2017 5:05 PM
# Copyright:   (c) "zhaozhongyu" "2/10/2017 5:05 PM" 
# Licence:     <your licence>
# -*- coding:utf-8 -*-
#-------------------------------------------------------------------------------
from io import BufferedReader,BufferedWriter, BufferedRandom
import struct
import tempfile

#os是指OutputStream
def packInt(os, value):
    '''
    :param os:
    :param value:
    :return: int
    '''
    if (not isinstance(os, BufferedWriter) and not isinstance(os, tempfile._TemporaryFileWrapper)) or not isinstance(value, int):
        raise TypeError("invalid value type, expect (BufferedWriter, int).")
    if os.closed:
        raise ValueError("invalid value, BufferedWriter is closed.")
    i = 1
    while (value & ~0x7F) != 0:
        os.write(struct.pack('>B', (value & 0x7F) | 0x80))
        value = value >> 7
        i = i + 1
    os.write(struct.pack('>B', value))
    return i

#返回一个int数字
def unpackInt(inputStream):
    '''
    :param inputStream:
    :return: int
    '''
    if not isinstance(inputStream, BufferedReader) and not isinstance(inputStream, tempfile._TemporaryFileWrapper)\
            and not isinstance(inputStream, BufferedRandom):
        raise TypeError("invalid value type, expect (BufferedReader). current value type is {}".format(type(inputStream)))
    if inputStream.closed:
        raise ValueError("invalid value, BufferedReader is closed.")
    result = 0
    for offset in range(0, 32, 7):
        byte = inputStream.read(1)
        if byte == b'':
            return result
        b = struct.unpack('>B', byte)[0]
        result = result | (b & 0x7F) << offset
        if (b & 0x80) == 0:
            return result
    raise InterruptedError("Malformed integer.")
