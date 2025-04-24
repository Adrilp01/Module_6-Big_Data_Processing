package examen

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object examen {

  //EJERCICIO 1
  def ejercicio1(estudiantes: DataFrame)(implicit spark:SparkSession): DataFrame = {

    import spark.implicits._

    println("Imprimo esquema")
    estudiantes.printSchema()
    println("Imprimo calificaciones mayores a 8")
    estudiantes.filter("Calificacion > 8").show()
    println("Imprimo nombres ordenados por calificacion") //si quisiera mostrar solo los nombres tendía que quitar las calificaciones del select
    val estudiantesorden = estudiantes.select("Nombre", "Calificacion").orderBy(col("Calificacion").desc)
    println("Imprimo nuevo dataframe")
    estudiantesorden.show()
    estudiantesorden
  }


  //EJERCICIO 2
  def ejercicio2(numeros: DataFrame)(implicit spark:SparkSession): DataFrame =  {
    import spark.implicits._
    val ParImparUDF = udf((n: Int) => if (n % 2 == 0) s"$n es par" else s"$n es impar")
    val newdf = numeros.withColumn("Par_Impar", ParImparUDF(numeros("Edad")))
    newdf.show()
    newdf
  }

  //EJERCICIO 3
  def ejercicio3(estudiantes: DataFrame , calificaciones: DataFrame): DataFrame = {
    val result = estudiantes.join(calificaciones, estudiantes("ID") === calificaciones("ID_estudiante"))
    val medias = result.groupBy("ID", "Nombre").agg(avg("Calificacion").alias("Media"))
    result.show()
    medias.show()
    medias
  }

  //EJERCICIO 4
  def ejercicio4(palabras: List[String])(implicit spark:SparkSession): RDD[(String, Int)] = {
    val rdd = spark.sparkContext.parallelize(palabras) //convierto la lista en rdd
    val contar = rdd.map(a => (a, 1)).reduceByKey(_ + _)
    contar.collect().foreach(println)
    contar
  }

  //EJERCICIO 5
  def ejercicio5(ventas: DataFrame)(implicit spark:SparkSession): DataFrame = {
    import spark.implicits._
    val cantXprecio = ventas.withColumn("dineroXid", col("cantidad") * col("precio_unitario"))

    val totalXProducto = cantXprecio.groupBy("id_producto").agg(sum("dineroXid").alias("dineroXproducto"))

    //val en_orden = totalXProducto.orderBy(desc("id_producto")) //lo ordeno para que quede mas claro el resultado

    totalXProducto.show()
    totalXProducto
  }


}