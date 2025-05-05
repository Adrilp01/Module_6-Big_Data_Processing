package examen

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import examen._
import org.apache.spark
import utils.TestInit

import scala.collection.Seq


class examenTest extends TestInit {

  val sc = spark.sparkContext


 
  "exercise1" should "1" in {
    val Name = Seq(
      Row("Maria", 20, 9.1),
      Row("Juan", 22, 7.5),
      Row("Lucia", 19, 8.7),
      Row("Pedro", 21, 6.3),
      Row("Sofia", 23, 9.5)
    )
    val schema = StructType(Seq(
      StructField("Name", StringType, nullable = false),
      StructField("Age", IntegerType, nullable = false),
      StructField("Marks", DoubleType, nullable = false)
    ))
    val students = spark.createDataFrame(spark.sparkContext.parallelize(nombres), schema)
    val out = exercise1(students).collect().map(_.getString(0))
    out shouldBe List("Sofia", "Maria", "Lucia", "Juan", "Pedro")
  }

  "exercise2" should "2" in{
    val students = Seq(
      Row("Maria", 20, 9.1),
      Row("Juan", 22, 7.5),
      Row("Lucia", 19, 8.7),
      Row("Pedro", 21, 6.3),
      Row("Sofia", 23, 9.5)
    )
    val schema = StructType(Seq(
      StructField("Name", StringType, nullable = false),
      StructField("Age", IntegerType, nullable = false),
      StructField("Marks", DoubleType, nullable = false)
    ))
    val numbers = spark.createDataFrame(spark.sparkContext.parallelize(students), schema)
    val out = exercise2(numbers).collect().map(_.getString(3))

    out shouldBe List("20 is even", "22 is even", "19 is odd", "21 is odd", "23 is odd")
  }

  "exercise3" should "3" in {
    val estud1 = Seq(
      Row(1, "Maria"),
      Row(2, "Juan"),
      Row(3, "Lucia"),
      Row(4, "Pedro"),
      Row(5, "Sofia")
    )
    val schema1 = StructType(Seq(
      StructField("ID", IntegerType, nullable = false),
      StructField("Name", StringType, nullable = false)
    ))
    val students = spark.createDataFrame(spark.sparkContext.parallelize(estud1), schema1)

    val estud2 = Seq(
      Row(1, "Maths", 10),
      Row(1, "Literature", 4),
      Row(1, "Science", 0),
      Row(2, "Literature", 4),
      Row(2, "History", 7),
      Row(3, "Science", 6),
      Row(3, "History", 4),
      Row(4, "History",  4),
      Row(5, "Maths", 6),
      Row(5, "Economy", 0)
    )
    val schema2 = StructType(Seq(
      StructField("ID_student", IntegerType, nullable = false),
      StructField("Subject", StringType, nullable = false),
      StructField("Mark", IntegerType, nullable = false)
    ))
    val marks = spark.createDataFrame(spark.sparkContext.parallelize(estud2), schema2)

    val out = ejercicio3(students , marks).collect().map(x => (x.get(0),x.get(1),x.get(2)))
    out shouldBe List((1,"Maria",4.666666666666667), (2,"Juan",5.5), (3,"Lucia",5.0), (4,"Pedro",4.0), (5,"Sofia",3.0))
  }

  "exercise4" should "4" in{
    val words = List("dog","cat","dog","whale","lion", "cat", "dog", "whale")

    val out = ejercicio4(palabras).collect().map(x => (x._1,x._2)).sorted
    out shouldBe Array(("dog",3), ("cat",2), ("whale",2), ("lion",1)).sorted
  }

  "exercise5" should "5" in {
    val sales = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/test/resources/examn/ventas.csv")

    exercise5(sales)
    val out = exercise5(sales).collect().map(x => (x.getInt(0),x.get(1)))

    out.toList shouldBe List((108,486.0), (101,460.0), (103,280.0), (107,396.0), (102,405.0), (109,540.0), (105,570.0), (110,494.0), (106,425.0), (104,800.0))
  }

}



