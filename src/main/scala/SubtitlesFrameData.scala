import org.apache.spark.sql.DataFrame

class SubtitlesFrameData (val dataFrame: DataFrame,
                          val limit: Int,
                          val columnName: String)
