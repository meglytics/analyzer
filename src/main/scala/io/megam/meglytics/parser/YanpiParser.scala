package io.megam.meglytics.parser


import com.typesafe.config.{ Config, ConfigFactory }
import io.megam.meglytics.core.{ YanpiDAG }
import scalaz._
import Scalaz._
import scalaz.Validation._
import io.megam.meglytics.core._
import scala.collection.JavaConverters._



class YanpiParser()  {

  def load(config: Config): ValidationNel[Throwable, YanpiDAG] = {

      val list = config.getObjectList("input.json.connectors").asScala.map(_.unwrapped()).toList
      val query = config.getString("input.json.query")
//get list of connectors
//parse list into
    println("=====================")
    println(list)
    println(query)
    println("=====================")
    return mkC(query, list)



  }

  private def mkC(query: String, list: List[java.util.Map[String,Object]]): ValidationNel[Throwable, YanpiDAG] = {
    //parse the query and list to create and return YanpiDAG class
    val x = new YanpiDAG()
    x.successNel[Throwable]

   }


}
