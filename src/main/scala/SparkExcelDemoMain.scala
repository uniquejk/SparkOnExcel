
import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import com.crealytics.spark.excel._
import org.apache.spark.sql.api.java.{UDF1, UDF2}
import org.apache.spark.sql.types.{DecimalType, DoubleType, StringType, StructField, StructType}


object SparkExcelDemoMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkExcelDemoMain").setMaster("local[8]")
    val splitStr = "公司"

    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sparkcontext = sparkSession.sparkContext
    sparkcontext.setLogLevel("warn")

    val tableSchema = StructType(Array(
      StructField("Count", DecimalType(38,2), nullable = false),
      StructField("Info", StringType, nullable = false),
      StructField("Duration", StringType, nullable = false),
      StructField("SAPCode", StringType, nullable = false),
      StructField("HeaderText", StringType, nullable = false)
    ))

    sparkSession.udf.register("getHeaderCode",new UDF1[String,String]{
      override def call(t1: String): String = {
        val strings: Array[String] = t1.split(splitStr)
        if(strings.length > 1){
          strings(1)
        } else {
          t1
        }
      }
    },StringType)

    val target:String = "/Users/jk/Test/huan/data6/tmp"
    val targetPath:File = new File(target)
    val files: Array[File] = targetPath.listFiles().filter(_.getName.endsWith("xlsx"))
    for ( file <- files ) {
      val input = file.getAbsolutePath
      val tmp: String = file.getName
      val output = target+ "/final/"+ tmp.split("Extract")(0) + "final.xlsx"
      extractAndAnaylisis(input,output)
    }

    def extractAndAnaylisis(inputPath:String,outputPath:String) = {
      val input = sparkSession.read
        .format("com.crealytics.spark.excel")
        .option("sheetName", "Sheet1")
        .option("useHeader", "true")
        .schema(tableSchema)
        .load(inputPath)

      input.createOrReplaceTempView("test")

      sparkSession.sql("select Count,Info,Duration,SAPCode,HeaderText from test where substr(Info,0,2)='6月'").createOrReplaceTempView("filterResult")

      val result = sparkSession.sql(
        """
          |select SAPCode,
          |getHeaderCode(HeaderText) as HeaderCode,
          |HeaderText,
          |sum(count) as totalcount from filterResult
          |group by SAPCode,HeaderCode,HeaderText
          |order by SAPCode
          |""".stripMargin).toDF()

      result.write
        .format("com.crealytics.spark.excel")
        .option("sheetName", "Sheet1")
        .option("useHeader", "true")
        .mode("overwrite")
        .save(outputPath)
    }

  }
}
