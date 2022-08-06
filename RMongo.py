def readMongo(cmplnt_num, db):
  mydoc = db.myTable.find({"CMPLNT_NUM": cmplnt_num})
  for x in mydoc:
    print(x)