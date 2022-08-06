def readMySQL(cmplnt_num, cursor, cnx):
  sql_query = ("SELECT * FROM crime_data WHERE CMPLNT_NUM = %s")
  cursor.execute(sql_query, (cmplnt_num,))
  #cursor.commit()
  result = cursor.fetchone()
  if result is not None:
   for x in result:
     print(x)
  else:
    print("Record Not Found!")