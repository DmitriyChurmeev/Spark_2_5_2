import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.functions.{asc, col, count, desc, explode, lower, monotonically_increasing_id, split}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}


object App {

  def main(args: Array[String]) = {

    val spark = SparkSession.builder()
      .appName("Spark_2_5_2")
      .master("local")
      .getOrCreate()

    val subtitlesFrame1 = spark.read
      .option("header","true")
      .option("delimiter","|")
      .option("inferSchema", "true")
      .csv("src/main/resources/subtitles_s1.json")

    val subtitlesFrame2 = spark.read
      .option("header","true")
      .option("delimiter","|")
      .option("inferSchema", "true")
      .csv("src/main/resources/subtitles_s2.json")

    val limitForDF = 20
    val countSubtitles1 = subtitlesFrame1.select(explode(split(lower(col("[{")), "\\W+")) as "w_s1").groupBy("w_s1")
      .agg(count(col("w_s1")) as "cnt_s1")
      .filter(col("w_s1") =!= "")
      .orderBy(desc("cnt_s1"))
      .withColumn("id", monotonically_increasing_id())
      .limit(limitForDF)

    val countSubtitles2 =  subtitlesFrame2.select(explode(split(lower(col("{")), "\\W+")) as "w_s2").groupBy("w_s2")
      .agg(count(col("w_s2")) as "cnt_s2")
      .filter(col("w_s2") =!= "")
      .orderBy(desc("cnt_s2"))
      .withColumn("id", monotonically_increasing_id())
      .limit(limitForDF)

    val innerJoinDF = countSubtitles1.col("id") === countSubtitles2.col("id")

    countSubtitles1
      .join(countSubtitles2, innerJoinDF,  "inner")
      .drop(countSubtitles2.col("id"))
      .select("w_s1","cnt_s1","id", "w_s2", "cnt_s2")
      .show
  }

}
