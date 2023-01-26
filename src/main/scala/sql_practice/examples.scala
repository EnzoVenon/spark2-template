package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object examples {
  def exec1(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)


  }



  def exec2(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val demoDF = spark.read.json("data/input/demographie_par_commune.json")
    val depDF = spark.read.csv("data/input/departements.txt")
      .withColumnRenamed("_c0", "Nom_departement")
      .withColumnRenamed("_c1", "Id_departement")

    demoDF.select("population")
      .agg(sum("population").as("France_pop"))
      .show()

    demoDF.groupBy("Departement")
      .agg(sum("Population").as("pop"))
      .orderBy($"pop".desc)
      .limit(10)
      .show()

    depDF.join(demoDF, demoDF("Departement") === depDF("Id_departement"))
      .groupBy("Nom_departement").agg(sum("Population").as("pop"))
      .orderBy($"pop".desc)
      .limit(10)
      .show()
  }



  def exec3(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val s7 = spark.read.option("delimiter", "\t").csv("data/input/sample_07")
      .withColumnRenamed("_c0", "ID")
      .withColumnRenamed("_c1", "Occupation")
      .withColumnRenamed("_c2", "Total_emp")
      .withColumnRenamed("_c3", "Salary")

    val s8 = spark.read.option("delimiter", "\t").csv("data/input/sample_08")
      .withColumnRenamed("_c0", "ID")
      .withColumnRenamed("_c1", "Occupation")
      .withColumnRenamed("_c2", "Total_emp")
      .withColumnRenamed("_c3", "Salary")

    s7.select("Occupation", "Salary")
      .where("Salary>100000")
      .orderBy($"Salary".desc)
      .limit(10)
      .show()

    s8.join(s7, s7("ID") === s8("ID"))
      .select(s8.col("Occupation"), (s8.col("Salary") - s7.col("Salary")).as("Salary_growth"))
      .where("Salary_growth>0")
      .orderBy($"Salary_growth".desc)
      .limit(10)
      .show()

    s8.join(s7, s7("ID") === s8("ID"))
      .select(s8.col("Occupation"), (s7.col("Total_emp") - s8.col("Total_emp")).as("Jobs_loss"))
      .where(s8.col("Salary")>100000)
      .where("Jobs_loss>0")
      .orderBy($"Jobs_loss".desc)
      .limit(10)
      .show()
  }
}
