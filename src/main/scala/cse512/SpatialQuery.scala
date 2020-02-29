package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((stContains(pointString, queryRectangle))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((stContains(pointString, queryRectangle))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((stWithin(pointString1, pointString2, distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((stWithin(pointString1, pointString2, distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def stWithin(pointString1:String, pointString2:String, distance:Double) : Boolean = {
    //   val in scala is like const
    val p1 = pointString1.split(",")
    // splits returns a list
    // trim coz its a string and double coz its float
    val p_x1 = p1(0).trim().toDouble
    val p_y1 = p1(1).trim().toDouble
    
    val p2 = pointString2.split(",")
    val p_x2 = p2(0).trim().toDouble
    val p_y2 = p2(1).trim().toDouble
    
    val distance_between = scala.math.pow(scala.math.pow((p_x1 - p_x2), 2) + scala.math.pow((p_y1 - p_y2), 2), 0.5)
    // <= for boundry points
    if(distance_between <= distance) 
    {return true}
    else
    {return false}
    }

def stContains(pointString:String, queryRectangle:String) : Boolean = {

    // parse rectangle points
    val rect = queryRectangle.split(",")
    val rect_x1 = rect(0).trim().toDouble
    val rect_y1 = rect(1).trim().toDouble
    val rect_x2 = rect(2).trim().toDouble
    val rect_y2 = rect(3).trim().toDouble
    // parse the point
    val p = pointString.split(",")
    val p_x = p(0).trim().toDouble
    val p_y = p(1).trim().toDouble
    

    // find minimum x and minimum y
    var min_x: Double = 0
    var max_x: Double = 0
    if(rect_x1 < rect_x2) {
      min_x = rect_x1
      max_x = rect_x2
    } else {
      min_x = rect_x2
      max_x = rect_x1
    }
    
    var min_y: Double = 0
    var max_y: Double = 0
    if(rect_y1 < rect_y2) {
      min_y = rect_y1
      max_y = rect_y2
    } else {
      min_y = rect_y2
      max_y = rect_y1
    }
    
    if(p_x >= min_x && p_x <= max_x && p_y >= min_y && p_y <= max_y) {
      return true
    } else {
      return false
    }
  }
}
