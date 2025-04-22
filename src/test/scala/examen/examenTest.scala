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
    ejercicio1(estudiantes)
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
    ejercicio2(numeros)
  }

  "ejercicio3" should "3" in {
    val estud1 = Seq(
      Row(1, "María"),
      Row(2, "Juan"),
      Row(3, "Lucía"),
      Row(4, "Pedro"),
      Row(5, "Sofía")
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
    ejercicio3(estudiantes, calificaciones)
  }

  "ejercicio4" should "4" in{
    val palabras = List("perro","gato","perro","orca","leon", "gato", "perro", "orca")

    ejercicio4(palabras).collect().foreach(println)
  }

  "ejercicio5" should "5" in {
    val ventas = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\adril\\Desktop\\Módulos KC\\Data Processing\\Data processing\\src\\test\\resources\\examen\\ventas.csv")

    ejercicio5(ventas)
  }

}



