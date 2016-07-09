/*
** Copyright [2013-2016] [Megam Systems]
**
** Licensed under the Apache License, Version 2.0 (the "License");
** you may not use this file except in compliance with the License.
** You may obtain a copy of the License at
**
** http://www.apache.org/licenses/LICENSE-2.0
**
** Unless required by applicable law or agreed to in writing, software
** distributed under the License is distributed on an "AS IS" BASIS,
** WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
** See the License for the specific language governing permissions and
** limitations under the License.
*/

/**
 * @author morpheyesh
 *
 */


package io.megam.meglytics

import io.megam.meglytics.core.{ YanpiDAG, SparkContextConfig }
import io.megam.meglytics.parser._
import scalaz._
import Scalaz._
import scalaz.Validation._
import org.apache.spark._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext, SparkContext._
import org.apache.spark.rdd.RDD
import com.typesafe.config.{ Config, ConfigFactory }
import scala.util.Try
import org.megam.common.riak.GunnySack
import scala.collection.JavaConverters._
import org.apache.spark.sql.SQLContext



object Main extends spark.jobserver.SparkJob with SparkContextConfig {

  def main(args: Array[String]) {
   val yag = ConfigFactory.load()
   runJob(sc, yag)
  }

  def validate(sc: SparkContext, config: Config): spark.jobserver.SparkJobValidation = spark.jobserver.SparkJobValid

  override def runJob(sc: SparkContext, config: Config): Any = { //since we are overriding runJob, the second arg is a Config
     val yag: ValidationNel[Throwable,YanpiDAG] = new YanpiParser().load(config)

/*    for {
      df  <- yag.toDataFrames()
      dm <- df.MergeAll()
    } yield {
      return dm.Query(yag.query)
    }
*/

return "test"
  }
}
