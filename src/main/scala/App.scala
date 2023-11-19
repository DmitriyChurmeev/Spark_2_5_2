import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object App extends Context{

  override val appName: String = "Spark_2_5_2"
  val limitForDF: Int = 20
  val idColumnName: String = "id"
  val wordsColumnName: String = "words"
  val countColumnName: String = "count"

  def main(args: Array[String]) = {

    val subtitlesFrame1 = getDfFromFile(spark, "src/main/resources/subtitles_s1.json")
    val subtitlesFrame2 = getDfFromFile(spark, "src/main/resources/subtitles_s2.json")
    val countSubtitles1 = getGroupCountSubtitlesFrame(new SubtitlesFrameData(subtitlesFrame1, limitForDF, "[{"))
    val countSubtitles2 = getGroupCountSubtitlesFrame(new SubtitlesFrameData(subtitlesFrame2, limitForDF, "{"))
    val innerJoinDF = countSubtitles1.col(idColumnName) === countSubtitles2.col(idColumnName)

    countSubtitles1
      .join(countSubtitles2, innerJoinDF,  "inner")
      .drop(countSubtitles2.col(idColumnName))
      .select(
        countSubtitles1.col(wordsColumnName) as "w_s1",
        countSubtitles1.col(countColumnName) as "cnt_s1",
        countSubtitles1.col(idColumnName),
        countSubtitles2.col(wordsColumnName) as "w_s2",
        countSubtitles2.col(countColumnName) as "cnt_s2")
      .show
  }

  def getDfFromFile(spark: SparkSession, filePath: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .option("inferSchema", "true")
      .csv(filePath)
  }

  /**
   * Get dataframe group count words
   * with column "words" and "count"
   * also "id" column
   * @param subtitlesFrameData data for grouping
   * @return grouping dataframe
   */
  def getGroupCountSubtitlesFrame(subtitlesFrameData: SubtitlesFrameData): DataFrame = {
    subtitlesFrameData.dataFrame
      .transform(df => splitDfByColumn(df, subtitlesFrameData.columnName))
      .transform(df => filterEmptyDf(df))
      .transform(df => groupByColumnWithCount(df))
      .transform(df => orderByColumn(df))
      .transform(df => addIdColumn(df))
      .limit(subtitlesFrameData.limit)
  }

  def splitDfByColumn(df: DataFrame, columnName: String): DataFrame = {
    df.withColumn("words", explode(split(lower(col(columnName)), "\\W+")))
  }
  def filterEmptyDf(df: DataFrame): DataFrame = {
    df.filter(col("words") =!= "")
  }
  def groupByColumnWithCount(df: DataFrame): DataFrame = {
    df.groupBy(col("words")).agg(count(col("words")) as "count")
  }
  def orderByColumn(df: DataFrame): DataFrame = {
    df.orderBy(desc("count"))
  }

  def addIdColumn(df: DataFrame): DataFrame = {
    df.withColumn("id", monotonically_increasing_id())
  }
}

trait Context {
  val appName: String

  lazy val spark = createSession(appName)

  private def createSession(appName: String) = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }
}
