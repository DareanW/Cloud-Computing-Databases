import pandas as pd
import numpy as np

def createMySQL(CMPLNT_NUM,CMPLNT_FR_DT,CMPLNT_FR_TM,CMPLNT_TO_DT,CMPLNT_TO_TM,RPT_DT,KY_CD,OFNS_DESC,PD_CD,PD_DESC,CRM_ATPT_CPTD_CD,LAW_CAT_CD,JURIS_DESC,BORO_NM,ADDR_PCT_CD,LOC_OF_OCCUR_DESC,PREM_TYP_DESC,PARKS_NM,HADEVELOPT,X_COORD_CD,Y_COORD_CD,Latitude,Longitude,Lat_Lon, cursor, cnx):
  
  #myDataframe = pd.DataFrame({CMPLNT_NUM,CMPLNT_FR_DT,CMPLNT_FR_TM,CMPLNT_TO_DT,CMPLNT_TO_TM,RPT_DT,KY_CD,OFNS_DESC,PD_CD,PD_DESC,CRM_ATPT_CPTD_CD,LAW_CAT_CD,JURIS_DESC,BORO_NM,ADDR_PCT_CD,LOC_OF_OCCUR_DESC,PREM_TYP_DESC,PARKS_NM,HADEVELOPT,X_COORD_CD,Y_COORD_CD,Latitude,Longitude,Lat_Lon}).replace(np.nan, "NULL").replace("", "NULL")
   
  #for row in myDataframe.itertuples():
    sql = "INSERT INTO crime_data (CMPLNT_NUM,CMPLNT_FR_DT,CMPLNT_FR_TM,CMPLNT_TO_DT,CMPLNT_TO_TM,RPT_DT,KY_CD,OFNS_DESC,PD_CD,PD_DESC,CRM_ATPT_CPTD_CD,LAW_CAT_CD,JURIS_DESC,BORO_NM,ADDR_PCT_CD,LOC_OF_OCCUR_DESC,PREM_TYP_DESC,PARKS_NM,HADEVELOPT,X_COORD_CD,Y_COORD_CD,Latitude,Longitude,Lat_Lon) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
      
      
    val = (CMPLNT_NUM,CMPLNT_FR_DT,
    CMPLNT_FR_TM, CMPLNT_TO_DT,
    CMPLNT_TO_TM, RPT_DT,
    KY_CD, OFNS_DESC,
    PD_CD, PD_DESC,
    CRM_ATPT_CPTD_CD, LAW_CAT_CD,
    JURIS_DESC, BORO_NM,
    ADDR_PCT_CD, LOC_OF_OCCUR_DESC,
    PREM_TYP_DESC, PARKS_NM,
    HADEVELOPT, X_COORD_CD,
    Y_COORD_CD, Latitude,
    Longitude, Lat_Lon)
    cursor.execute(sql,val)
    cnx.commit()