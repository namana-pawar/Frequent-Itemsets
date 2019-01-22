import java.io._

import util.control.Breaks._

import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD


object NamanaMadanMohanRao_Pawar_SON {
  // apriori algorithm
  def apriori(chunk: Iterator[Set[String]], threshold:Int): Iterator[Set[String]] = {
    val baskets = chunk.toList

    // frequent single items
    val single = baskets.flatten.groupBy(identity).mapValues(_.size).filter(x => x._2 >= threshold).keySet

    var currcandidateset = single

    //Set to store all the frequent items
    var freqitems = Set.empty[Set[String]]
    single.foreach(word => freqitems = freqitems + Set(word))

    var setsize = 2

    while (currcandidateset.nonEmpty) {
      //Set to store candidate items of each iteration
      var newset = Set.empty[String]
      var subsets = currcandidateset.subsets(setsize)
      for (candidate <- subsets) {
        breakable {
          var support = 0
          for (itemset <- baskets) {
            if (candidate.subsetOf(itemset)) {
              support += 1
            }
            if (support >= threshold) {
              newset = newset ++ candidate
              freqitems = freqitems + candidate
              break
            }
          }
        }
      }
      currcandidateset = newset
      setsize += 1
    }


    freqitems.iterator

  }

  def prepare_output(result : RDD[String]) : String = {
    var prevlen = 1
    var first = true
    var output = result.map(x => {

      //val y = x.toList
      var line = ""
      if (x.count(_ == ',') + 1 > prevlen) {
        line = line.concat("\n\n(")
      }
      else if (!first) {
        line = line.concat(", (")
      }

      line = line.concat(x)
      line = line.concat(")")

      prevlen = x.count(_ == ',') + 1
      first = false
      line
    }).collect().mkString("")
    output = "(" + output
    output
  }

  def main(args: Array[String]) : Unit = {
    val t1 = System.nanoTime

    val sparkSession = org.apache.spark.sql.SparkSession.builder.master("local[*]").appName("SparkScala").getOrCreate
    val sc = sparkSession.sparkContext

    var raw = sc.textFile(args(0))
    //"/Users/namanapawar/IdeaProjects/SparkScala/src/Data/yelp_reviews_large.txt"
    // split by ,
    var data = raw.map(row => row.split(","))

    // get partition number and to modify support
    var no_of_partitions = raw.getNumPartitions

    //val sup = args(2).toInt
    val support = args(1).toInt

    //Generate baskets
    var buckets: RDD[Set[String]] = data.map(row => (row(0), row(1))).groupByKey().map(_._2.toSet)

    //Modify threshold according to no of partitions
    var threshold = support / no_of_partitions


    println("PHASE ONE " + (System.nanoTime - t1) / 1e9d)

    //Determine frequent sets using Apriori
    var phase_one = buckets.mapPartitions(chunk => {
      apriori(chunk, threshold)
    }).map(x => (x, 1)).reduceByKey((v1, v2) => 1).map(_._1).collect()

    val broadcasted = sc.broadcast(phase_one)

    println("PHASE TWO " + (System.nanoTime - t1) / 1e9d)

    var phase_two = buckets.mapPartitions(chunk => {
      var chunklist = chunk.toList
      var out = new ListBuffer[String]()
      for (i <- chunklist) {
        for (j <- broadcasted.value) {
          if (j.subsetOf(i)) {
            out = out += j.toSeq.sorted.mkString(",")
          }
        }
      }
      out.sorted.iterator
    })


    println("FILTERING " + (System.nanoTime - t1) / 1e9d)
    var result = phase_two.map(row => (row, 1)).reduceByKey(_ + _).filter(_._2 >= support)
      .map(_._1)


    println("SORTING " + (System.nanoTime - t1) / 1e9d)
    result = result.map(row => ((row.count(_ == ',') + 1, row), 1)).sortByKey(numPartitions = 1).map(row => row._1._2)


    //write to file
    println("PREPARING OUTPUT " + (System.nanoTime - t1) / 1e9d)
    val output = prepare_output(result)
    val output_path = args(2)
    new PrintWriter(output_path) {
      write(output)
      close()
    }

    val duration = (System.nanoTime - t1) / 1e9d
    println(s"Time = " + duration)
  }
}
