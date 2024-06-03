SELECT /*+ parallel(10) */ DISTINCT
   Incident_Number,
   Reported_Date,
   ACTUAL_REPORTED_DATE__C,
   TO_CHAR(TO_DATE('19700101', 'yyyymmdd') + ((Reported_Date) / 24 / 60 / 60),'yyyymmddhh24miss') Reported_Date_D,
   Last_Modified_Date,
   REQUIRED_RESOLUTION_DATETIME,
   Assigned_Group,
   Priority,
   Status,
   DECODE(Status,1,'Assigned',2,'In Progress',3,'Pending','OTHER') STATUS_DECODE,
   Assigned_Support_Organization,
   DESCRIPTION,
   SLM_STATUS,
   SERVICE_TYPE   
   
FROM ARADMIN.HPD_HELP_DESK
WHERE Incident_Number IS NOT NULL
