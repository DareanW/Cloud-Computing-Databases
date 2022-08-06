from pymongo import MongoClient
from CMongo import *
from RMongo import *
from UMongo import *
from DMongo import *

import csv
import time
mongoClient = MongoClient('localhost', 27017) 
#db = mongoClient.crime_data
db = mongoClient['NYPD_crime_data']
myTable = db["crimeData"]
db.myTable.drop()
#db.segment.drop()
startTimer = time.time()
header = [ "CMPLNT_NUM", "CMPLNT_FR_DT", "CMPLNT_FR_TM", "CMPLNT_TO_DT", "CMPLNT_TO_TM", "RPT_DT", "KY_CD", "OFNS_DESC", "PD_CD", "PD_DESC", "CRM_ATPT_CPTD_CD", "LAW_CAT_CD", "JURIS_DESC", "BORO_NM", "ADDR_PCT_CD", "LOC_OF_OCCUR_DESC", "PREM_TYP_DESC", "PARKS_NM", "HADEVELOPT", "X_COORD_CD", "Y_COORD_CD", "Latitude", "Longitude", "Lat_Lon" ]
csvfile = open('1000_NYPD_Complaint_Data_Current_YTD.csv', 'r')
reader = csv.DictReader(csvfile)
i = 1
for each in reader:
  row = {}
  print(type(reader))
  for field in header:
    print(field)
    row[field]=each[field]
    if not row[field]:
      row[field] = 'NULL'  
  
  print(row)
  print(i)
  i = i + 1
  #db.segment.insert_one(row)
  db.myTable.insert_one(row)

endTimer = time.time()
print("Time for initial setup: " + str(endTimer - startTimer))

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
    createMongo("000","09/30/2016","23:25:00","09/30/2016","23:25:00","09/30/2016","236","DANGEROUS WEAPONS","782","WEAPONS"," POSSESSION, ETC","COMPLETED","MISDEMEANOR","N.Y. TRANSIT POLICE","BRONX,42","","TRANSIT - NYC SUBWAY","","","1015308","244373","40.837376359","-73.887760929","\"(40.837376359, -73.887760929)\"", db)
    endTimer = time.time()
    print("Time for Create function: " + str(endTimer - startTimer))
  elif option == 'R':
    startTimer = time.time()
    readMongo("000", db)
    endTimer = time.time()
    print("Time for Read function: " + str(endTimer - startTimer))
  elif option == 'U':
    startTimer = time.time()
    updateMongo("000","000","000","09/30/2016","23:25:00","09/30/2016","236","DANGEROUS WEAPONS","782","WEAPONS"," POSSESSION, ETC","COMPLETED","MISDEMEANOR","N.Y. TRANSIT POLICE","BRONX,42","","TRANSIT - NYC SUBWAY","","","1015308","244373","40.837376359","-73.887760929","\"Somewhere\"", db)
    endTimer = time.time()
    print("Time for Update function: " + str(endTimer - startTimer))
  elif option == 'D':
    startTimer = time.time()
    deleteMongo("000", db)
    endTimer = time.time()
    print("Time for Delete function: " + str(endTimer - startTimer))
  elif option == 'E':
    exit()
  else:
    print('Invalid option. Please enter a given letter.')



#index = 0
#for i in db.myTable.find({'OFNS_DESC': 'DANGEROUS WEAPONS'}):
#   print(i)
#   index += 1
#print (index)
  
#with open('10000_NYPD_Complaint_Data_Current_YTD.csv', 'r') as file:
#    list = []
#    data = csv.reader(file,delimiter = '\n')  # extracting one row 
#    for i in data:
#        list.append(i[0].split(';')) #splitting the data with delimiter ;
#with open('new_10000_NYPD_Complaint_Data_Current_YTD.csv', 'w',newline='') as data:
#    writer = csv.writer(data)
#    writer.writerows(list)
#import pandas
#df.head()
#data=df.to_dict(orient="records")
#print(data)
#db.segment.drop()
#db.segment.insert_many(data)
#collection_name.insert_many([item_1,item_2])