# PalDB
==========
## PalDB is an embeddable write-once key-value store written in Python.
### PalDB's scripts is only 20k and has not dependency.

Code samples
------------
How to write a store
```python
from Paldb.api import PalDB

c=PalDB.createWriter("D:\\paldb.db")
c.put("aaa", "aaaaaaa")
c.put("bbb", "bbbbbbb")
c.put(123, [1,2,3,4,5,])
c.close()
```

How to read a store
```python
r = PalDB.createReader("D:\\paldb.db")
print(r.get("aaa"))
print(r.get(123))
r.close()
```

