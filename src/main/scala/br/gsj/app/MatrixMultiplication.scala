package br.gsj.app

import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import br.gsj.utils.MatrixUtils

object MatrixMultiplication {
  
  var w_path = "/matrices/w"
  var f_path = "/matrices/f"
  def main(args: Array[String]): Unit = {
    
   
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("HareScalaSpark-" + args(0).substring(args(0).lastIndexOf("/") + 1))
      .getOrCreate()
 
      
       w_path = args(0) + w_path
       f_path = args(0) + f_path
    
      val sc = spark.sparkContext
     
      
      val w_rdd = sc.textFile(w_path)
      val f_rdd = sc.textFile(f_path)
      
       var w = loadCoordinateMatrix(w_rdd)
       var f = loadCoordinateMatrix(f_rdd)
       
       val res = MatrixUtils.coordinateMatrixMultiply(w, f)
       
       res.entries.foreach(println)
           
  }
  
   def loadCoordinateMatrix(rdd : RDD[String]): CoordinateMatrix = {
    new CoordinateMatrix(rdd.map{ x =>
      val a = x.split(",")
      new MatrixEntry(a(0).toLong,a(1).toLong,a(2).toDouble)})
}
  
}