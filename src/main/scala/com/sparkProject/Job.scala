package com.sparkProject

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object Job {

  def loadSparkSesssion(): SparkSession ={
    // SparkSession configuration
    val spark = SparkSession
      .builder
      .master("local")
      .appName("spark session TP_parisTech")
      .getOrCreate()

    //return spark.sparkContext
    return spark
  }

  def loadDataFrame(spark:SparkSession):DataFrame={
    val df = spark.read
    .option("header","True")
    .option("inferSchema","true") //infers the input schema automatically from data. It requires one extra pass over the data.
    .option("comment","#") //sets the single character used for skipping lines beginning with this character.
    .csv("/home/daphne/TP_Spark/tp_spark/cumulative.csv")
    return df
  }

  def partie1(sc:SparkContext): Unit={
    /********************************************************************************
    *
    *        TP 1
    *
    *        - Set environment, InteliJ, submit jobs to Spark
    *        - Load local unstructured data
    *        - Word count , Map Reduce
    ********************************************************************************/

    // ----------------- word count ------------------------
/*
    val df_wordCount = sc.textFile("/home/daphne/spark-2.0.0-bin-hadoop2.6/README.md")
       .flatMap{case (line: String) => line.split(" ")}
       .map{case (word: String) => (word, 1)}
       .reduceByKey{case (i: Int, j: Int) => i + j}
       .toDF("word", "count")

     df_wordCount.orderBy($"count".desc).show()
*/
  }

  def partie3(df:DataFrame): Unit ={
    println("%s %d - %s %d".format("************************************* Column count: ",df.columns.length," - Line count : ",df.count()))
    //Print the first row
    df.show(numRows = 1,truncate = true)



    //afficher sous forme de table un sous-ensemble des colonnes (exemple les colonnes 10 à 20).
    val columns = df.columns.slice(10, 20) // df.columns returns an Array. In scala arrays have a method “slice” returning a slice of the array
    df.select(columns.map(col): _*).show(50) // Applique la fonction col à chaque élément de l'array columns et le transforme en dataset (coleection d'éléments uniques)
                                            // grâce à l'instruciton _*

    // Afficher le schéma du dataFrame et afficher des stats sur les colonnes
    //df.printSchema()
    //df.describe().show()

    //Afficher le schéma du dataFrame (nom des colonnes et le type des données contenues dans chacune d’elles).
    print("******************************************  DAta types")

        val indices: List[Int] = List()
        (1 to df.columns.length/10).foreach { i =>
          val columns = df.columns.slice(10*i, 10*(i+1))
          df.select(columns.map(col): _*).printSchema()
        }

    println( "%s %d".format("**********************  Nombre de lignes dans koi_disposition ",df.select("koi_disposition").count()))
  }

  def partie4(df:DataFrame):DataFrame={

    /*Conserver uniquement les lignes qui nous intéressent pour le modèle (koi_disposition = CONFIRMED ou FALSE POSITIVE )*/
     val df_filtered = df.where("koi_disposition = 'CONFIRMED' or koi_disposition = 'FALSE POSITIVE'")
     df.take(1).foreach(println)
    //df.printSchema()

    println("%s %d - %s %d".format("************************************* Column count: ",df_filtered.columns.length," - Line count : ",df_filtered.count()))
      /* Afficher le nombre d’éléments distincts dans la colonne “koi_eccen_err1”. Certaines colonnes sont complètement vides: elles ne servent à rien pour l’entraînement du modèle.  */
    df_filtered.groupBy("koi_eccen_err1").count().show()
    //df_filtered.select("koi_eccen_err1").distinct().show()

    /* Supprime toutes les colonnes non désifrées */
    val df_restricted = df_filtered.drop("index","koi_eccen_err1","kepid","koi_fpflag_nt","koi_fpflag_ss","koi_fpflag_co","koi_fpflag_ec","koi_sparprov","koi_trans_mod",
      "koi_datalink_dvr","koi_datalink_dvs", "koi_tce_delivname","koi_parm_prov", "koi_limbdark_mod", "koi_fittype","koi_disp_prov",
      "koi_comment", "kepoi_name","kepler_name", "koi_vet_date", "koi_pdisposition")

    /* Supprime les colonnes qui ne contiennent que 1 valeur*/
    val cols = df_restricted.columns

    var droppedCols = List[String]()
    for (col <- cols) {
      if (df_restricted.groupBy(col).count().count() <= 1) {
        droppedCols ++= List(col)
      }
    }



    val df1 = df_restricted.drop(droppedCols:_*)
    println("Dropped columns:")
    for (col <- droppedCols) {
      println("\t" + col)
    }


    /*Afficher des statistiques sur les colonnes du dataFrame, éventuellement pour quelques colonnes seulement pour la lisibilité du résultat.*/



    // TODO


    /* Certaines cellules du dataFrame ne contiennent pas de valeur. Remplacer toutes les valeurs manquantes par zéro. */
    val df_final = df1.na.fill(0.0)

    return df_final
  }

 def partie5(df4:DataFrame):DataFrame={

   val df_labels = df4.select("rowid", "koi_disposition")
   val df_features = df4.drop("koi_disposition")

   val df_joined = df_features
     .join(df_labels, usingColumn = "rowid")
  return df_joined
  }

  def partie6(df_5:DataFrame)(implicit spark: SparkSession,sc:SparkContext):DataFrame={
    import spark.implicits._
    def udf_sum = udf((col1: Double, col2: Double) => col1 + col2)


    val df_newFeatures = df_5
      .withColumn("koi_ror_min", udf_sum($"koi_ror", $"koi_ror_err2"))
      .withColumn("koi_ror_max", $"koi_ror" + $"koi_ror_err1")
    return df_newFeatures
  }

  def main(args: Array[String]): Unit = {
    implicit val spark = loadSparkSesssion()
    implicit val sc =  spark.sparkContext
    import spark.implicits._
    val df = loadDataFrame(spark)

    val df4 =  partie4(df)
    val df_joined =  partie5(df4)
    val df_newFeatures = partie6(df_joined)
    df_newFeatures
      .coalesce(1) // optional : regroup all data in ONE partition, so that results are printed in ONE file
      // >>>> You should not that in general, only when the data are small enough to fit in the memory of a single machine.
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("/home/daphne/TP_Spark/tp_spark/cleanedDataFrame.csv")
    print("******************************************************************")
  }



}
