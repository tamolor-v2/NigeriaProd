package AlertProducer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import java.net.URI


object HdfsUtils {
  def getHadoopFs(defaultName: String) : FileSystem = {

    System.setProperty("java.security.krb5.realm", "MTN.COM")
    System.setProperty("java.security.krb5.kdc", "edge01001.mtn.com")

    val conf = new Configuration()

    conf.set("hadoop.security.authentication", "kerberos")
    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"))

    val uri = URI.create(defaultName)
    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)


    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.loginUserFromKeytab("flare@MTN.COM", "/etc/security/keytabs/flare.keytab")

    val fs = FileSystem.newInstance(uri, conf)
    fs
  }
}
