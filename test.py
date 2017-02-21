from Paldb.api import PalDB

c=PalDB.createWriter("D:\\paldb.db")
c.put("aaa", "aaaaaaa")
c.put("bbb", "bbbbbbb")
c.put(123, [1,2,3,4,5,])
c.close()

r = PalDB.createReader("D:\\paldb.db")
print(r.get("aaa"))
print(r.get(123))
r.close()
