package AlertProducer

import java.io.{BufferedWriter, File, FileWriter, FilenameFilter, IOException, OutputStreamWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.fs.Path

import scala.io.Source

object FileUtils {
  def WriteToFile(message: String, localFilePath: String, hdfsFilePath: String, hdfsDefaultName: String) : Unit = {
    // get current date and time to build path for local and hdfs
    val cal = Calendar.getInstance()
    val now = cal.getTime()
    val timeFormat = new SimpleDateFormat("yyyyMMdd_HHmmss")
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val currentTimeAsString = timeFormat.format(now)
    val currentDateAsString = dateFormat.format(now)

    // local directory creation for csv files
    val directory = new File(localFilePath+"/"+currentDateAsString)
    directory.mkdir()


    if (message.length() > 1) {
      // get JobName from message
      val jobName = message.split("\\u0001").apply(1)

      // Local File creation depending on the job name and current date / time
      val file = new File(directory.toString+"/"+jobName+"_"+currentTimeAsString+".csv")

      // Hadoop file creation depending on the job name and current date / time
      val fs = HdfsUtils.getHadoopFs(hdfsDefaultName)
      val filePath = new Path(hdfsFilePath+"/tbl_dt="+currentDateAsString+"/"+jobName+"_"+currentTimeAsString+".csv")

      // Write the message to the local file
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write(message+"\n")
      bw.close()

      // Write the message to hadoop file
      val writer = new BufferedWriter(new OutputStreamWriter(fs.create(filePath)))
      writer.write(message)
      writer.close
    }
    else
      println("Invalid Message!!, No files were written")
  }

}
