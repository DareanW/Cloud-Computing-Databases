from pymongo import MongoClient
import csv
mongoClient = MongoClient("mongodb+srv://group9:group9@group9-1.kqczjud.mongodb.net/?retryWrites=true&w=majority") 
db = mongoClient.crime_data

with open('10000_NYPD_Complaint_Data_Current_YTD.csv', 'r') as file:
    list = []
    data = csv.reader(file,delimiter = '\n')  # extracting one row 
    for i in data:
        list.append(i[0].split(';')) #splitting the data with delimiter ;
with open('new_10000_NYPD_Complaint_Data_Current_YTD.csv', 'w',newline='') as data:
    writer = csv.writer(data)
    writer.writerows(list)
import pandas
df.head()
data=df.to_dict(orient="records")
print(data)
db.segment.drop()
db.segment.insert_many(data)
#collection_name.insert_many([item_1,item_2])