package io.megam.meglytics.core

import org.apache.spark.{SparkConf, SparkContext}
import com.typesafe.config.{ Config, ConfigFactory }
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame


trait SparkContextConfig {

  private val config =  ConfigFactory.load()

  private lazy val sparkConf = {
    val master = "mesos://103.56.92.44:5050" //get from config - set ip of spark cluster
    new SparkConf()
          .setMaster(master)
          .setAppName("meglytics")
          .set("spark.driver.allowMultipleContexts", "true")
          .set("spark.executor.extraClassPath", "/root/spark-1.5.1/lib/mysql-connector-java-5.1.34.jar")
  }
    val sc = new SparkContext(sparkConf)
}



trait SqlContext { //this needs to extend sparkcontextconfig and sqlcontext should be created implicitly
//  implicit val sqlContext = new SQLContext(sc)
  def executeAnalysis(sc: SparkContext, tableName: String, url: String, prop: java.util.Properties): DataFrame = {

    val p = new SQLContext(sc).read.jdbc(url,tableName,prop)
      println(p.show())
    return p


  }

}
