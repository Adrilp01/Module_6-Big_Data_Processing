package examn

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object examn {

  //EJERCICIO 1
  def exercise1(students: DataFrame)(implicit spark:SparkSession): DataFrame = {

    import spark.implicits._

    println("Print schema")
    students.printSchema()
    println("Print marks greater than 8")
    students.filter("Marks > 8").show()
    println("print name sorted by mark.") //si quisiera mostrar solo los nombres tendía que quitar las calificaciones del select
    val ordered_students = students.select("Name", "Marks").orderBy(col("Marks").desc)
    println("Print new dataframe")
    ordered_students.show()
    ordered_students
  }


  //EJERCICIO 2
  def exercise2(numbers: DataFrame)(implicit spark:SparkSession): DataFrame =  {
    import spark.implicits._
    val Even_oddUDF = udf((n: Int) => if (n % 2 == 0) s"$n is even" else s"$n is odd")
    val newdf = numbers.withColumn("Even_Odd", Even_oddUDF(numbers("Age")))
    newdf.show()
    newdf
  }

  //EJERCICIO 3
  def exercise3(students: DataFrame , marks: DataFrame): DataFrame = {
    val result = students.join(marks, students("ID") === marks("ID_student"))
    val averages = result.groupBy("ID", "Name").agg(avg("Mark").alias("Average"))
    result.show()
    medias.show()
    medias
  }

  //EJERCICIO 4
  def exercise4(words: List[String])(implicit spark:SparkSession): RDD[(String, Int)] = {
    val rdd = spark.sparkContext.parallelize(words) //convert list into RDD
    val count = rdd.map(a => (a, 1)).reduceByKey(_ + _)
    count.collect().foreach(println)
    count
  }

  //EJERCICIO 5
  def exercise5(sales: DataFrame)(implicit spark:SparkSession): DataFrame = {
    import spark.implicits._
    val quantXprice = sales.withColumn("MoneyXid", col("cantidad") * col("precio_unitario"))

    val totalXProduct = quantXprice.groupBy("id_producto").agg(sum("MoneyXid").alias("MoneyXproduct"))

    //val en_orden = totalXProducto.orderBy(desc("id_producto")) //lo ordeno para que quede mas claro el resultado

    totalXProduct.show()
    totalXProduct
  }


}
