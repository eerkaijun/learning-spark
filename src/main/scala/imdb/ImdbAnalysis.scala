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
    val temp = List((0.toFloat, 0, 0, "placeholder"))
    sc.parallelize(temp)
  }

  def task2(l1: RDD[TitleBasics], l2: RDD[TitleRatings]): RDD[String] = {
    val temp = List(("placeholder"))
    sc.parallelize(temp)
  }

  def task3(l1: RDD[TitleBasics], l2: RDD[TitleRatings]): RDD[(Int, String, String)] = {
    val temp = List((0, "placeholder", "placeholder"))
    sc.parallelize(temp)
  }

  // Hint: There could be an input RDD that you do not really need in your implementation.
  def task4(l1: RDD[TitleBasics], l2: RDD[TitleCrew], l3: RDD[NameBasics]): RDD[(String, Int)] = {
    val temp = List(("placeholder", 0))
    sc.parallelize(temp)
  }

  def main(args: Array[String]) {
    val temp = List((0.toFloat, 0, 0, "placeholder"))
    titleBasicsRDD.filter({ x => x.genres != None && x.runtimeMinutes != None}).foreach(println)
    //.flatMap{ case x => x.genres.get.zip(List.fill(x.genres.get.length)(x.runtimeMinutes.get))}
    //.groupBy(x => x._1).map { case (k,v) => (k,v.map(_._2))}
    //.map({ case (k,v) => (v.sum.toFloat/v.size, v.min, v.max, k)}).toList
    //sc.parallelize(temp).foreach(println)
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
