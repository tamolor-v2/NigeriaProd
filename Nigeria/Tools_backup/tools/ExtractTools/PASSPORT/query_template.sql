select ID, 
FACE_COUNT, 
VERIFIED_BASIC_DATA_FK, 
BASIC_DATA_FK from BIOCAPTURE.PASSPORT  WHERE SUBSTR(id,-1,1)='${MSSDN}'