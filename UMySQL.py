import pandas as pd
import numpy as np

def updateMySQL(CMPLNT_NUM,CMPLNT_FR_DT,CMPLNT_FR_TM,CMPLNT_TO_DT,CMPLNT_TO_TM,RPT_DT,KY_CD,OFNS_DESC,PD_CD,PD_DESC,CRM_ATPT_CPTD_CD,LAW_CAT_CD,JURIS_DESC,BORO_NM,ADDR_PCT_CD,LOC_OF_OCCUR_DESC,PREM_TYP_DESC,PARKS_NM,HADEVELOPT,X_COORD_CD,Y_COORD_CD,Latitude,Longitude,Lat_Lon, cursor, cnx):
  
  #myDataframe = pd.DataFrame({CMPLNT_NUM,CMPLNT_FR_DT,CMPLNT_FR_TM,CMPLNT_TO_DT,CMPLNT_TO_TM,RPT_DT,KY_CD,OFNS_DESC,PD_CD,PD_DESC,CRM_ATPT_CPTD_CD,LAW_CAT_CD,JURIS_DESC,BORO_NM,ADDR_PCT_CD,LOC_OF_OCCUR_DESC,PREM_TYP_DESC,PARKS_NM,HADEVELOPT,X_COORD_CD,Y_COORD_CD,Latitude,Longitude,Lat_Lon}).replace(np.nan, "NULL").replace("", "NULL")
   
  #for row in myDataframe.itertuples():
    sql = "UPDATE crime_data SET CMPLNT_NUM = %s, CMPLNT_FR_DT = %s, CMPLNT_FR_TM = %s, CMPLNT_TO_DT = %s,CMPLNT_TO_TM = %s,RPT_DT = %s,KY_CD = %s,OFNS_DESC = %s,PD_CD = %s,PD_DESC = %s,CRM_ATPT_CPTD_CD = %s,LAW_CAT_CD = %s,JURIS_DESC = %s,BORO_NM = %s,ADDR_PCT_CD = %s,LOC_OF_OCCUR_DESC = %s,PREM_TYP_DESC = %s,PARKS_NM = %s,HADEVELOPT = %s,X_COORD_CD = %s,Y_COORD_CD = %s,Latitude = %s,Longitude = %s,Lat_Lon = %s  WHERE CMPLNT_NUM = %s"
      
      
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
    Longitude, Lat_Lon, CMPLNT_NUM)
    cursor.execute(sql,val)
    cnx.commit()