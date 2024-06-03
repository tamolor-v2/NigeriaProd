package AlertProducer

object ArgsParser {
  var configFile = ""
  var message = "some_message"
  def parsArgs = (args: Array[String]) => {
    def printHelpAndExit(): Unit = {
      println("--configFile --message");
      System.exit(0)
    }

    def validateParam(param: String): String = {
      if (param == null || param.length == 0) {
        println(s"param ( $param ) is not valid")
        printHelpAndExit
      }
      param
    }

    args.sliding(2, 2).toList.collect {
      case Array("--configFile", argConfigFile: String) => configFile = validateParam(argConfigFile)
      case Array("--message", argMessage: String) => message = validateParam(argMessage)
      case Array("--help") | _ => println(args.mkString(" ")); printHelpAndExit
    }
  }
}
