import scala.collection.immutable.TreeMap
import scala.collection.mutable._
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable

object CommandHelper {

  def readArgs(args:Array[String], logger: Logger) : TreeMap[String,String] = {

    val argsIterator = args.iterator
    val inputParams = new mutable.HashMap[String,String]

    while(argsIterator.hasNext) {
      argsIterator.next().toLowerCase match {

        case "-inputPath" => inputParams("inputPath") = argsIterator.next()
        case x => logger.info("Unknown switch " + x)
          throw new RuntimeException("Invalid command line argument - " + x)
      }
    }

    val finalMap = new TreeMap[String,String]()(Ordering.by(_.toLowerCase))

    finalMap ++ inputParams

  }
}