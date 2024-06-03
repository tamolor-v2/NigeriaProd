KAMANJA_HOME=/home/stguser/FlareStageLoadInstall/FlareStageLoad
MD=/mnt/beegfs_api/Nabil/FacebookProject
TENANTID=flarestageload

#add input messages
#$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties add message $MD/ECR_FINAL/ECR_FINAL_input_Msg.json TENANTID $TENANTID

#add output message
#$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties add message $MD/ECR_FINAL/ECR_FINAL_output_Msg.json TENANTID $TENANTID


#add config Model
#$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties upload compile config $MD/ECR_FINAL/ECR_FINAL_Model_config.json

#add model
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties add model scala $MD/ECR_FINAL/ECR_FINAL_Model.scala  DEPENDSON ECR_FINALModel TENANTID $TENANTID


#add msg adapter
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties add adaptermessagebinding FROMFILE $MD/ECR_FINAL/ECR_FINAL_Adapter_inpMsg_CSV_Binding.json
