KAMANJA_HOME=/opt/flare/PROD/flare_prod/

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties  remove model com.mtn.models.ecr_final_model.000000000001000000 TENANTID flare_prod

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties remove adaptermessagebinding KEY  "ECR_FINAL_kafka_in,com.mtn.messages.ecr_final_input,com.ligadata.kamanja.serializer.csvserdeser"

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties remove adaptermessagebinding KEY  "output_hdfs_kafka,com.mtn.messages.ecr_final,com.ligadata.kamanja.serializer.csvserdeser"

#$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties remove adaptermessagebinding KEY  "ECR_FINAL_kafka_out,com.mtn.messages.ecr_final,com.ligadata.kamanja.serializer.csvserdeser"

$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties remove message com.mtn.messages.ecr_final.000000000001000000 TENANTID flare_prod
$KAMANJA_HOME/bin/kamanja $KAMANJA_HOME/config/MetadataAPIConfig.properties remove message com.mtn.messages.ecr_final_input.000000000001000000 TENANTID flare_prod
