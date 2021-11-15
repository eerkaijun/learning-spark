package imdb

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

case class TitleBasics(tconst: String, titleType: Option[String], primaryTitle: Option[String],
                      originalTitle: Option[String], isAdult: Int, startYear: Option[Int], endYear: Option[Int],
                      runtimeMinutes: Option[Int], genres: Option[List[String]]) {
  def getGenres(): List[String] = genres.getOrElse(List[String]())
}
case class TitleRatings(tconst: String, averageRating: Float, numVotes: Int)
case class TitleCrew(tconst: String, directors: Option[List[String]], writers: Option[List[String]])
case class NameBasics(nconst: String, primaryName: Option[String], birthYear: Option[Int], deathYear: Option[Int],
                      primaryProfession: Option[List[String]], knownForTitles: Option[List[String]])

object ImdbAnalysis {

  val conf: SparkConf = new SparkConf().setAppName("Coursework").setMaster("local[2]");
  val sc: SparkContext = new SparkContext(conf);

  // Hint: use a combination of `ImdbData.titleBasicsPath` and `ImdbData.parseTitleBasics`
  val titleBasicsRDD: RDD[TitleBasics] = sc.textFile(ImdbData.titleBasicsPath).map(ImdbData.parseTitleBasics _)
  // val titleBasicsRDD: RDD[TitleBasics] = sc.parallelize(ImdbData.readFile(ImdbData.titleBasicsPath, ImdbData.parseTitleBasics _));

  // Hint: use a combination of `ImdbData.titleRatingsPath` and `ImdbData.parseTitleRatings`
  val titleRatingsRDD: RDD[TitleRatings] = sc.textFile(ImdbData.titleRatingsPath).map(ImdbData.parseTitleRatings _);
  //val titleRatingsRDD: RDD[TitleRatings] = sc.parallelize(ImdbData.readFile(ImdbData.titleRatingsPath, ImdbData.parseTitleRatings _));

  // Hint: use a combination of `ImdbData.titleCrewPath` and `ImdbData.parseTitleCrew`
  val titleCrewRDD: RDD[TitleCrew] = sc.textFile(ImdbData.titleCrewPath).map(ImdbData.parseTitleCrew _);
  // val titleCrewRDD: RDD[TitleCrew] = sc.parallelize(ImdbData.readFile(ImdbData.titleCrewPath, ImdbData.parseTitleCrew _));

  // Hint: use a combination of `ImdbData.nameBasicsPath` and `ImdbData.parseNameBasics`
  val nameBasicsRDD: RDD[NameBasics] = sc.textFile(ImdbData.nameBasicsPath).map(ImdbData.parseNameBasics _);
  // val nameBasicsRDD: RDD[NameBasics] = sc.parallelize(ImdbData.readFile(ImdbData.nameBasicsPath, ImdbData.parseNameBasics _));

  def task1(rdd: RDD[TitleBasics]): RDD[(Float, Int, Int, String)] = {
    rdd.filter({ x => x.genres != None && x.runtimeMinutes != None})
    .flatMap{ case x => x.genres.get.zip(List.fill(x.genres.get.length)(x.runtimeMinutes.get))}
    .groupBy(x => x._1).map { case (k,v) => (k,v.map(_._2))}
    .map({ case (k,v) => (v.sum.toFloat/v.size, v.min, v.max, k)})
  }

  def task2(l1: RDD[TitleBasics], l2: RDD[TitleRatings]): RDD[String] = {
    val l1_filtered = l1.filter({ case x => 
      x.titleType != None && x.titleType.get == "movie" && 
      x.primaryTitle != None && 
      x.startYear != None && x.startYear.get >= 1990 && 
      x.startYear.get <= 2018})
      .map(x => (x.tconst, x.primaryTitle.get))
    val l2_filtered = l2.filter({ case y =>
      y.averageRating >= 7.5 && y.numVotes >= 500000})
      .map(y => (y.tconst, y.numVotes))
    l1_filtered.join(l2_filtered).map{ case (k,v) => v._1}
  }

  def task3(l1: RDD[TitleBasics], l2: RDD[TitleRatings]): RDD[(Int, String, String)] = {
    val l2_map = l2.map(x => (x.tconst, x.averageRating))
    val l1_filtered = l1.filter({ case x => 
      x.titleType != None && x.titleType.get == "movie" && 
      x.primaryTitle != None && x.genres != None &&
      x.startYear != None && x.startYear.get >= 1900 && 
      x.startYear.get <= 1999})
    .map(x => (x.tconst, (x.startYear.get, x.primaryTitle.get, x.genres.get)))
    l1_filtered.join(l2_map)
    .map{ case (k,v) => (v._1._1, v._1._2, v._1._3, v._2)}
    .groupBy(x => x._1 / 10 % 10)
    .map{ case(k,v) => (k,v.flatMap(x => (x._3,(List.fill(x._3.length)(x._2)),(List.fill(x._3.length)(x._4))).zipped.toList))}
    .map{ case(k,v) => (k,v.groupBy(_._1).map{ case (k,v) => (k, v.maxBy(_._3))})}
    .flatMap{ case(k,v) => v.map{ case(genre, tuple) => (k,genre,tuple._2)}}
    .sortBy(_._1).sortBy(_._2)
  }

  // Hint: There could be an input RDD that you do not really need in your implementation.
  def task4(l1: RDD[TitleBasics], l2: RDD[TitleCrew], l3: RDD[NameBasics]): RDD[(String, Int)] = {
    val l3_filtered = l3.filter{ case x => 
      x.primaryName != None && x.knownForTitles != None &&
      x.knownForTitles.get.length >= 2
    }
    val l1_filtered = l1.filter{ case x => 
      x.startYear != None && x.startYear.get >= 2010 && x.startYear.get <= 2021
    }.map(x => (x.tconst, x.primaryTitle.get))
    l3_filtered.flatMap(x => (x.knownForTitles.get,(List.fill(x.knownForTitles.get.length)(x.primaryName.get)),(List.fill(x.knownForTitles.get.length)(x.nconst))).zipped.toList)
    .map(x => (x._1, (x._2, x._3)))
    .join(l1_filtered).map{ case (k,v) => (v._1._1, v._1._2) }
    .groupBy(x => x._2)
    .filter{case(k,v) => v.size >= 2}
    .map{case(k,v)=>(v.head._1,v.size)}
  }

  def main(args: Array[String]) {
    // val temp = List((0.toFloat, 0, 0, "placeholder"))
    // titleBasicsRDD.filter({ x => x.genres != None && x.runtimeMinutes != None})
    // .flatMap{ case x => x.genres.get.zip(List.fill(x.genres.get.length)(x.runtimeMinutes.get))}
    // .groupBy(x => x._1).map { case (k,v) => (k,v.map(_._2))}
    // .map({ case (k,v) => (v.sum.toFloat/v.size, v.min, v.max, k)})
    // .foreach(println)

    val l3_filtered = nameBasicsRDD.filter{ case x => 
      x.primaryName != None && x.knownForTitles != None &&
      x.knownForTitles.get.length >= 2
    }
    val l1_filtered = titleBasicsRDD.filter{ case x => 
      x.startYear != None && x.startYear.get >= 2010 && x.startYear.get <= 2021
    }.map(x => (x.tconst, x.primaryTitle.get))
    l3_filtered.flatMap(x => (x.knownForTitles.get,(List.fill(x.knownForTitles.get.length)(x.primaryName.get)),(List.fill(x.knownForTitles.get.length)(x.nconst))).zipped.toList)
    .map(x => (x._1, (x._2, x._3)))
    .join(l1_filtered).map{ case (k,v) => (v._1._1, v._1._2) }
    .groupBy(x => x._2)
    .filter{case(k,v) => v.size >= 2}
    .map{case(k,v)=>(v.head._1,v.size)}
    .foreach(println)
    //
    //.filter{case(k,v) => v.length >= 2}
    //.map{case(k,v)=>(v.head._2,v.length)}

    sc.stop()



    // val durations = timed("Task 1", task1(titleBasicsRDD).collect().toList)
    // val titles = timed("Task 2", task2(titleBasicsRDD, titleRatingsRDD).collect().toList)
    // val topRated = timed("Task 3", task3(titleBasicsRDD, titleRatingsRDD).collect().toList)
    // val crews = timed("Task 4", task4(titleBasicsRDD, titleCrewRDD, nameBasicsRDD).collect().toList)
    // println(durations)
    // println(titles)
    // println(topRated)
    // println(crews)
    // println(timing)
    // sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
