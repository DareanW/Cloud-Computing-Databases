from pymongo import MongoClient
import csv
mongoClient = MongoClient('localhost', 27017) 
#db = mongoClient.crime_data
db = mongoClient['NYPD_crime_data']
myTable = db["crimeData"]
#db.segment.drop()

header = [ "CMPLNT_NUM", "CMPLNT_FR_DT", "CMPLNT_FR_TM", "CMPLNT_TO_DT", "CMPLNT_TO_TM", "RPT_DT", "KY_CD", "OFNS_DESC", "PD_CD", "PD_DESC", "CRM_ATPT_CPTD_CD", "LAW_CAT_CD", "JURIS_DESC", "BORO_NM", "ADDR_PCT_CD", "LOC_OF_OCCUR_DESC", "PREM_TYP_DESC", "PARKS_NM", "HADEVELOPT", "X_COORD_CD", "Y_COORD_CD", "Latitude", "Longitude", "Lat_Lon" ]
csvfile = open('1000_NYPD_Complaint_Data_Current_YTD.csv', 'r')
reader = csv.DictReader(csvfile)
i = 1
for each in reader:
  row = {}
  for field in header:
    
    row[field]=each[field]
    if not row[field]:
      row[field] = 'NULL'  
  
  print(row)
  print(i)
  i = i + 1
  #db.segment.insert_one(row)
  db.myTable.insert_one(row)
  

for i in db.myTable.find({'OFNS_DESC': 'DANGEROUS WEAPONS'}):
    print(i)

  
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
