def deleteMongo(cmplnt, db):

  print(db.myTable.count_documents({}))
  mydoc = db.myTable.delete_one( { "CMPLNT_NUM": cmplnt } )
  print(db.myTable.count_documents({}))