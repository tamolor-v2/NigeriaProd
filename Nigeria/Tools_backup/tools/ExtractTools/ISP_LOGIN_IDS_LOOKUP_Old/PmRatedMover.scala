#!/bin/sh
exec scala -cp  -savecompiled "$0" "$@"
!#

import java.io.File
import java.nio.file.{Files, StandardCopyOption}


object PmRatedMover {

  var filesLocation = ""
  var destination = ""


  def main(args: Array[String]): Unit = {

    parsArgs(args)


    println("filesLocation"+ filesLocation)
    println("destination"+ destination)

    val listDir = FileHelper.getSubFileDirs(filesLocation, (f: File) => f.isFile)
    val des = new File(destination)
    if (des.exists() && des.isDirectory) {
      listDir.foreach(f => {
        val originFileName = f.getName
        println("file:%s".format(originFileName))
		val splittedName = originFileName.split("_")
        val date = splittedName(0)
        val hour = splittedName(1)
		val min =  splittedName(2)
		val anum = splittedName(4)
        val newFile = s"${date}_WBS_PM_RATED_CDRS_${hour}_${min}_0${anum}.gz"
        val newFilePath = destination + FileHelper.FILE_SEPARATOR + newFile
        println("moving file %s to %s ".format(f.getAbsolutePath, newFilePath))
        FileHelper.moveFile(f.getAbsolutePath, newFilePath)
      })
    } else {
      System.err.println("destination %s is not a valid directory ".format(destination))
    }

  }

  def parsArgs = (args: Array[String]) => {
    if (args == null || args.length == 0) {
      System.err.println("Invalid arguments %s".format(args.mkString(" ")))
      System.exit(0)
    }
    args.sliding(2, 2).toList.collect {
      case Array("--filesLocation", argFilesLocation: String) => filesLocation = argFilesLocation
      case Array("--destination", argDestination: String) => destination = argDestination
      case Array("--help") | _ => println("--filesLocation --destination  --help"); System.exit(0)
    }
  }
}

object FileHelper {
  val FILE_SEPARATOR: String = System.getProperty("file.separator")

  private def getFile(path: String): File = {
    if (path == null || path.length == 0) {
      throw new IllegalArgumentException(s"Path is not valid ($path)")
    }
    new File(path)
  }

  def moveFile(p1: String, p2: String): Unit = {
    val f1 = getFile(p1)
    val f2 = getFile(p2)
    Files.move(f1.toPath, f2.toPath, StandardCopyOption.REPLACE_EXISTING)
  }

  def getSubFileDirs(path: String, f: File => Boolean): List[File] = {
    val file = getFile(path)
    if (!file.exists || !file.isDirectory)
      throw new IllegalArgumentException(s"Path is not a valid directory ($path)")
    else
      file.listFiles.filter(f(_)).toList
  }

  def mkDir(path: String): Unit = {
    val file = getFile(path)
    file.mkdirs
  }
}

PmRatedMover.main(args)
