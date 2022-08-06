def deleteMySQL(cursor, cnx, cmplntNum):
  sql_delete_query = """DELETE from crime_data WHERE CMPLNT_NUM = %s"""
  cursor.execute(sql_delete_query, (cmplntNum,))
  cnx.commit()
  print("Record Deleted")