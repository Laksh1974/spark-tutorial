package org.test.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  def main(args: Array[String]) {
      //val inputFile = args(0)
      //val outputFile = args(1)
      val conf = new SparkConf().setAppName("WordCount").setMaster("local")
      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)
      // Load our input data.
      val test =  sc.textFile("food.txt")
      // Split up into words.
      val words = test.flatMap(line => line.split(" "))
      // Transform into word and count.
      val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
      // Save the word count back out to a text file, causing evaluation.
      counts.saveAsTextFile("food.count.txt")
    }
}