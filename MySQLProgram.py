from __future__ import print_function
from CMySQL import *
from RMySQL import *
from UMySQL import *
from DMySQL import *

import time
import mysql.connector
import pandas as pd
import numpy as np
from mysql.connector import errorcode

DB_NAME = 'crimeDatabase'

cnx = mysql.connector.connect(host="localhost",user="group9", password="group9")
cursor = cnx.cursor()
def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        print("Database {} created successfully.".format(DB_NAME))
      
        print("OK")
    else:
        print(err)
        exit(1)

cnx.database = DB_NAME

        
cursor.execute("select database();")
record = cursor.fetchone()
print("You're connected to database: ", record)
cursor.execute('DROP TABLE IF EXISTS crime_data;')

startTimer = time.time()
data = pd.read_csv('10.csv')
#data = pd.read_csv('1000_NYPD_Complaint_Data_Current_YTD.csv')
df = pd.DataFrame(data).replace(np.nan, "NULL")
cursor.execute('''
              CREATE TABLE crime_data (
              CMPLNT_NUM TINYTEXT,
              CMPLNT_FR_DT TINYTEXT,
              CMPLNT_FR_TM TINYTEXT,
              CMPLNT_TO_DT TINYTEXT,
              CMPLNT_TO_TM TINYTEXT,
              RPT_DT TINYTEXT,
              KY_CD TINYTEXT,
              OFNS_DESC TINYTEXT,
              PD_CD TINYTEXT,
              PD_DESC TINYTEXT,
              CRM_ATPT_CPTD_CD TINYTEXT,
              LAW_CAT_CD TINYTEXT,
              JURIS_DESC TINYTEXT,
              BORO_NM TINYTEXT,
              ADDR_PCT_CD TINYTEXT,
              LOC_OF_OCCUR_DESC TINYTEXT,
              PREM_TYP_DESC TINYTEXT,
              PARKS_NM TINYTEXT,
              HADEVELOPT TINYTEXT,
              X_COORD_CD TINYTEXT,
              Y_COORD_CD TINYTEXT,
              Latitude TINYTEXT,
              Longitude TINYTEXT,
              Lat_Lon TINYTEXT)
              ''' )
i = 1
print(df)
for row in df.itertuples():
  sql = "INSERT INTO crime_data (CMPLNT_NUM,CMPLNT_FR_DT,CMPLNT_FR_TM,CMPLNT_TO_DT,CMPLNT_TO_TM,RPT_DT,KY_CD,OFNS_DESC,PD_CD,PD_DESC,CRM_ATPT_CPTD_CD,LAW_CAT_CD,JURIS_DESC,BORO_NM,ADDR_PCT_CD,LOC_OF_OCCUR_DESC,PREM_TYP_DESC,PARKS_NM,HADEVELOPT,X_COORD_CD,Y_COORD_CD,Latitude,Longitude,Lat_Lon) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
  
  
  val = (row.CMPLNT_NUM, row.CMPLNT_FR_DT,
  row.CMPLNT_FR_TM, row.CMPLNT_TO_DT,
  row.CMPLNT_TO_TM, row.RPT_DT,
  row.KY_CD, row.OFNS_DESC,
  row.PD_CD, row.PD_DESC,
  row.CRM_ATPT_CPTD_CD, row.LAW_CAT_CD,
  row.JURIS_DESC, row.BORO_NM,
  row.ADDR_PCT_CD, row.LOC_OF_OCCUR_DESC,
  row.PREM_TYP_DESC, row.PARKS_NM,
  row.HADEVELOPT, row.X_COORD_CD,
  row.Y_COORD_CD, row.Latitude,
  row.Longitude, row.Lat_Lon)
  cursor.execute(sql,val)
  cnx.commit()
  print(row)
  print(i)
  i+=1
  
                  
endTimer = time.time()
print("Time for initial setup: " + str(endTimer - startTimer))
#cursor.execute('DROP TABLE IF EXISTS crime_data;')
#cursor.execute("CREATE TABLE crime_data (CMPLNT_NUM INT,CMPLNT_FR_DT TINYTEXT, CMPLNT_FR_TM TINYTEXT,CMPLNT_TO_DT TINYTEXT,CMPLNT_TO_TM TINYTEXT,RPT_DT TINYTEXT,KY_CD TINYTEXT,OFNS_DESC TINYTEXT,PD_CD TINYTEXT,PD_DESC TINYTEXT,CRM_ATPT_CPTD_CD TINYTEXT,LAW_CAT_CD TINYTEXT,JURIS_DESC TINYTEXT,BORO_NM TINYTEXT,ADDR_PCT_CD TINYTEXT,LOC_OF_OCCUR_DESC TINYTEXT,PREM_TYP_DESC TINYTEXT,PARKS_NM TINYTEXT,HADEVELOPT TINYTEXT,X_COORD_CD TINYTEXT,Y_COORD_CD TINYTEXT,Latitude TINYTEXT,Longitude TINYTEXT,Lat_Lon TINYTEXT);")

#cursor.execute("LOAD DATA INFILE '10000_NYPD_Complaint_Data_Current_YTD.csv' INTO TABLE crime_data FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;")

while(True):
  try:
    option = input(f'\n\
                       Enter your choice:\n\n\
                       C: Create \n\
                       R: Read \n\
                       U: Update \n\
                       D: Delete\n\
                       E: Exit\n\n'
                       )
  except:
    print('Wrong input. Please enter a number ...')#Check what choice was entered and act accordingly
    
  if option == 'C':
    startTimer = time.time()
    createMySQL("000","09/30/2016","23:25:00","09/30/2016","23:25:00","09/30/2016","236","DANGEROUS WEAPONS","782","WEAPONS"," POSSESSION, ETC","COMPLETED","MISDEMEANOR","N.Y. TRANSIT POLICE","BRONX,42","","TRANSIT - NYC SUBWAY","","","1015308","244373","40.837376359","-73.887760929","\"(40.837376359, -73.887760929)\"", cursor, cnx)
    endTimer = time.time()
    print("Time for Create function: " + str(endTimer - startTimer))
  elif option == 'R':
    startTimer = time.time()
    readMySQL("000", cursor, cnx)
    endTimer = time.time()
    print("Time for Read function: " + str(endTimer - startTimer))
  elif option == 'U':
    startTimer = time.time()
    updateMySQL("000","000","000","09/30/2016","23:25:00","09/30/2016","236","DANGEROUS WEAPONS","782","WEAPONS"," POSSESSION, ETC","COMPLETED","MISDEMEANOR","N.Y. TRANSIT POLICE","BRONX,42","","TRANSIT - NYC SUBWAY","","","1015308","244373","40.837376359","-73.887760929","\"Somewhere\"", cursor, cnx)
    endTimer = time.time()
    print("Time for Update function: " + str(endTimer - startTimer))
  elif option == 'D':
    startTimer = time.time()
    deleteMySQL(cursor, cnx, "000")
    endTimer = time.time()
    print("Time for Delete function: " + str(endTimer - startTimer))
  elif option == 'E':
    exit()
  else:
    print('Invalid option. Please enter a number between 1 and 4.')

#startTimer = time.time()
#createMySQL("000","09/30/2016","23:25:00","09/30/2016","23:25:00","09/30/2016","236","DANGEROUS WEAPONS","782","WEAPONS"," POSSESSION, ETC","COMPLETED","MISDEMEANOR","N.Y. TRANSIT POLICE","BRONX,42","","TRANSIT - NYC SUBWAY","","","1015308","244373","40.837376359","-73.887760929","\"(40.837376359, -73.887760929)\"", cursor, cnx)
#endTimer = time.time()
#print("Time for Create function: " + str(endTimer - startTimer))
#
#startTimer = time.time()
#updateMySQL("000","000","000","09/30/2016","23:25:00","09/30/2016","236","DANGEROUS WEAPONS","782","WEAPONS"," POSSESSION, ETC","COMPLETED","MISDEMEANOR","N.Y. TRANSIT POLICE","BRONX,42","","TRANSIT - NYC SUBWAY","","","1015308","244373","40.837376359","-73.887760929","\"Somewhere\"", cursor, cnx)
#endTimer = time.time()
#print("Time for Update function: " + str(endTimer - startTimer))
#
#startTimer = time.time()
#deleteMySQL(cursor, cnx, "000")
#endTimer = time.time()
#print("Time for Delete function: " + str(endTimer - startTimer))


cursor.close()
cnx.close()
print("Closed.")