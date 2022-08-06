def createMongo(CMPLNT_NUM,CMPLNT_FR_DT,CMPLNT_FR_TM,CMPLNT_TO_DT,CMPLNT_TO_TM,RPT_DT,KY_CD,OFNS_DESC,PD_CD,PD_DESC,CRM_ATPT_CPTD_CD,LAW_CAT_CD,JURIS_DESC,BORO_NM,ADDR_PCT_CD,LOC_OF_OCCUR_DESC,PREM_TYP_DESC,PARKS_NM,HADEVELOPT,X_COORD_CD,Y_COORD_CD,Latitude,Longitude,Lat_Lon,db):
  header = [ "CMPLNT_NUM", "CMPLNT_FR_DT", "CMPLNT_FR_TM", "CMPLNT_TO_DT", "CMPLNT_TO_TM", "RPT_DT", "KY_CD", "OFNS_DESC", "PD_CD", "PD_DESC", "CRM_ATPT_CPTD_CD", "LAW_CAT_CD", "JURIS_DESC", "BORO_NM", "ADDR_PCT_CD", "LOC_OF_OCCUR_DESC", "PREM_TYP_DESC", "PARKS_NM", "HADEVELOPT", "X_COORD_CD", "Y_COORD_CD", "Latitude", "Longitude", "Lat_Lon" ]
  crimeData = [str(CMPLNT_NUM),str(CMPLNT_FR_DT),str(CMPLNT_FR_TM),str(CMPLNT_TO_DT),str(CMPLNT_TO_TM),str(RPT_DT),str(KY_CD),str(OFNS_DESC),str(PD_CD),str(PD_DESC),str(CRM_ATPT_CPTD_CD),str(LAW_CAT_CD),str(JURIS_DESC),str(BORO_NM),str(ADDR_PCT_CD),str(LOC_OF_OCCUR_DESC),str(PREM_TYP_DESC),str(PARKS_NM),str(HADEVELOPT),str(X_COORD_CD),str(Y_COORD_CD),str(Latitude),str(Longitude),str(Lat_Lon)]

  row = {"CMPLNT_NUM": CMPLNT_NUM, "CMPLNT_FR_DT":CMPLNT_FR_DT, "CMPLNT_FR_TM":CMPLNT_FR_TM, "CMPLNT_TO_DT":CMPLNT_TO_DT,"CMPLNT_TO_TM":CMPLNT_TO_TM, "RPT_DT":RPT_DT, "KY_CD":KY_CD, "OFNS_DESC":OFNS_DESC, "PD_CD":PD_CD, "PD_DESC":PD_DESC, "CRM_ATPT_CPTD_CD":CRM_ATPT_CPTD_CD, "LAW_CAT_CD":LAW_CAT_CD, "JURIS_DESC":JURIS_DESC, "BORO_NM":BORO_NM, "ADDR_PCT_CD":BORO_NM, "LOC_OF_OCCUR_DESC":BORO_NM, "PREM_TYP_DESC":PREM_TYP_DESC, "PARKS_NM":PARKS_NM, "HADEVELOPT":HADEVELOPT, "X_COORD_CD":HADEVELOPT, "Y_COORD_CD":Y_COORD_CD, "Latitude":Latitude, "Longitude":Longitude, "Lat_Lon":Lat_Lon}
  for each in row:
    if not row[each]:
      row[each] = 'NULL'  
#   for each in reader:
#  row = {}
#  print(type(reader))
#  for field in header:
#    print(field)
#    row[field]=each[field]
#    if not row[field]:
#      row[field] = 'NULL'  
  
  print(row)
  #print(i)
  #i = i + 1
  db.myTable.insert_one(row)
