ETLExecutor{
        LoadCluster {
                ClusterName=NigeriaLoadCluster
                Schema="flare_8"
				Kerberos="kinit|-k|-t|/etc/security/keytabs/daasuser.keytab|daasuser@MTN.COM"
				PrestoConnString="jdbc:presto://master01004.mtn.com:8099/hive5/flare_8"
				PrestoUID="test"
				PrestoPWD="null"
                Steps{
                Step_1{
                        seq=1
                        queryFilePath="/mnt/beegfs/tools/ETLExecutor/queries/firstSet.hql|/mnt/beegfs/tools/ETLExecutor/queries/secondSet.hql|"
                        }
                Step_2{
                        seq=2
                        queryFilePath="/mnt/beegfs/tools/ETLExecutor/queries/thirdtSet.hql|/mnt/beegfs/tools/ETLExecutor/queries/fourthSet.hql"
                        }
             }
        }
}

