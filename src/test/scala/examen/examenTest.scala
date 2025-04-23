package examen

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import examen._
import org.apache.spark
import utils.TestInit

import scala.collection.Seq


class examenTest extends TestInit {

  val sc = spark.sparkContext


 
  "ejercicio1" should "1" in {
    val nombres = Seq(
      Row("Maria", 20, 9.1),
      Row("Juan", 22, 7.5),
      Row("Lucia", 19, 8.7),
      Row("Pedro", 21, 6.3),
      Row("Sofia", 23, 9.5)
    )
    val schema = StructType(Seq(
      StructField("Nombre", StringType, nullable = false),
      StructField("Edad", IntegerType, nullable = false),
      StructField("Calificacion", DoubleType, nullable = false)
    ))
    val estudiantes = spark.createDataFrame(spark.sparkContext.parallelize(nombres), schema)
    val out = ejercicio1(estudiantes).collect().map(_.getString(0))
    out shouldBe List("Maria", "Juan", "Lucia", "Pedro", "Sofia")
  }

  "ejercicio2" should "2" in{
    val estudiantes = Seq(
      Row("Maria", 20, 9.1),
      Row("Juan", 22, 7.5),
      Row("Lucia", 19, 8.7),
      Row("Pedro", 21, 6.3),
      Row("Sofia", 23, 9.5)
    )
    val schema = StructType(Seq(
      StructField("Nombre", StringType, nullable = false),
      StructField("Edad", IntegerType, nullable = false),
      StructField("Calificacion", DoubleType, nullable = false)
    ))
    val numeros = spark.createDataFrame(spark.sparkContext.parallelize(estudiantes), schema)
    val out = ejercicio2(numeros).collect().map(_.getString(3))

    out shouldBe List("20 es par", "22 es par", "19 es impar", "21 es impar", "23 es impar")
  }

  "ejercicio3" should "3" in {
    val estud1 = Seq(
      Row(1, "Maria"),
      Row(2, "Juan"),
      Row(3, "Lucia"),
      Row(4, "Pedro"),
      Row(5, "Sofia")
    )
    val schema1 = StructType(Seq(
      StructField("ID", IntegerType, nullable = false),
      StructField("Nombre", StringType, nullable = false)
    ))
    val estudiantes = spark.createDataFrame(spark.sparkContext.parallelize(estud1), schema1)

    val estud2 = Seq(
      Row(1, "Matematicas", 10),
      Row(1, "Literatura", 4),
      Row(1, "Ciencia", 0),
      Row(2, "Literatura", 4),
      Row(2, "Historia", 7),
      Row(3, "Ciencia", 6),
      Row(3, "Historia", 4),
      Row(4, "Historia",  4),
      Row(5, "Matematicas", 6),
      Row(5, "Economia", 0)
    )
    val schema2 = StructType(Seq(
      StructField("ID_estudiante", IntegerType, nullable = false),
      StructField("Asignatura", StringType, nullable = false),
      StructField("Calificacion", IntegerType, nullable = false)
    ))
    val calificaciones = spark.createDataFrame(spark.sparkContext.parallelize(estud2), schema2)

    val out = ejercicio3(estudiantes , calificaciones).collect().map(x => (x.get(0),x.get(1),x.get(2)))
    out shouldBe List((1,"Maria",4.666666666666667), (2,"Juan",5.5), (3,"Lucia",5.0), (4,"Pedro",4.0), (5,"Sofia",3.0))
  }

  "ejercicio4" should "4" in{
    val palabras = List("perro","gato","perro","orca","leon", "gato", "perro", "orca")

    val out = ejercicio4(palabras).collect().map(x => (x._1,x._2)).sorted
    out shouldBe Array(("perro",3), ("gato",2), ("orca",2), ("leon",1)).sorted
  }

  "ejercicio5" should "5" in {
    val ventas = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/test/resources/examen/ventas.csv")

    ejercicio5(ventas)
    val out = ejercicio5(ventas).collect().map(x => (x.getInt(0),x.get(1)))

    out.toList shouldBe List((108,486.0), (101,460.0), (103,280.0), (107,396.0), (102,405.0), (109,540.0), (105,570.0), (110,494.0), (106,425.0), (104,800.0))
  }

}



