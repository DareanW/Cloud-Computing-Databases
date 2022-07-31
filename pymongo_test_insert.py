from pymongo import MongoClient
import csv
mongoClient = MongoClient("mongodb+srv://group9:group9@group9-1.kqczjud.mongodb.net/?retryWrites=true&w=majority") 
db = mongoClient.crime_data

csvfile = open('10000_NYPD_Complaint_Data_Current_YTD.csv', 'r')
reader = csv.DictReader( csvfile )
i = 0
listOfDict = []
for each in reader:
    row={}
    if(i == 0):
        header = each
    for field in header:
        row[field]=each[field]

    print (row)
    listOfDict.append(row)
    i = i + 1
    print ("\n" + str(i) + "\n")
db.segment.drop()
db.segment.insert_many(listOfDict)
#collection_name.insert_many([item_1,item_2])
