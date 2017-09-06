package com.dataanalysis.E911

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object EmergencyNumberAnanysis {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("E911").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    //load E911 CSV file
    val e911 = sc.textFile("/home/cloudera/DataSets/Emergency911Data/E911.csv")

    val e911Header = e911.first()

    val dataWithOutHeader = e911.filter(row => row != e911Header)

    val emergencyDataWithSchema = dataWithOutHeader.map(x => x.split(",")).filter(x => x.length >= 9).map(x => emergency(x(0), x(1), x(2), x(3), x(4).substring(0, x(4).indexOf(":")), x(5), x(6), x(7), x(8))).toDF

    //Create temp table for E911
    emergencyDataWithSchema.registerTempTable("emergency_911")

    //load zip codes from CSV file
    val zipcodes = sc.textFile("/home/cloudera/DataSets/Emergency911Data/zipcode.csv")

    val zipHead = zipcodes.first()

    val zipCodesWithOutHeader = zipcodes.filter(row => row != zipHead)

    val zipcodesWithSchema = zipCodesWithOutHeader.map(x => x.split(",")).map(x => zipcode(x(0).replace("\"", ""), x(1).replace("\"", ""), x(2).replace("\"", ""), x(3), x(4), x(5), x(6))).toDF

    //Create Temp table for zipcodes
    zipcodesWithSchema.registerTempTable("zipcode_table")
    
    //Problem Statment One - Kind of Problem prevalent and state
    val problem = sqlContext.sql("select e.title, z.city,z.state from emergency_911 e join zipcode_table z on e.zip = z.zip")
    
    val ps = problem.map(x => (x(0)+" -->"+x(2).toString))
 
    val ps1 = ps.map(x=> (x,1)).reduceByKey(_+_).map(item => item.swap).sortByKey(false).foreach(println)
    
    //Problem Statment One - Kind of Problem prevalent and city    
    val pc = problem.map(x => (x(0)+" -->"+x(1).toString))
 
    val pc1 = pc.map(x=> (x,1)).reduceByKey(_+_).map(item => item.swap).sortByKey(false).foreach(println)
  }

  case class emergency(lat: String, lng: String, desc: String, zip: String, title: String, timeStamp: String, twp: String, addr: String, e: String)
  
  case class zipcode(zip: String, city: String, state: String, latitude: String, longitude: String, timezone: String, dst: String)

}