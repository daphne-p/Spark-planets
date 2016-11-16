package com.sparkProject

import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.tuning.TrainValidationSplitModel

import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator}

/**
  * Created by daphne on 25/10/16.
  */
object JobML {

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

  def loadDataFrame(spark:SparkSession, path : String):DataFrame={
    val df = spark.read.parquet(path)
    val df2 = df.drop("rowid")

    return df2
  }

  def dataCleaning(df: DataFrame): DataFrame ={
    //******************************************************************************************
    //
    //   P A C K A G E R     L E S    F E A T U R E S    D A N S     U N     V E C T E U R
    //
    //******************************************************************************************
  val features = df.columns.filter(x => (x!="koi_disposition" ))
  val assembler = new VectorAssembler()
    .setInputCols(features)
    .setOutputCol("features")

  val df_vectorized_tmp =  assembler.transform(df)
  val df_vectorized = df_vectorized_tmp.select("features","koi_disposition")

    //******************************************************************************************
    //
    //   C R E A T I O N     D E    D U M M Y     V A R I A B L E S
    //
    //******************************************************************************************
  val indexer = new StringIndexer()
    .setInputCol("koi_disposition")
    .setOutputCol("label")
    .fit(df_vectorized)
  val indexedDF = indexer.transform(df_vectorized)
  val finalDF = indexedDF.drop("koi_disposition")

  return finalDF
}

  def myLogisticRegression(df:DataFrame):TrainValidationSplitModel={
    //******************************************************************************************
    //
    //   C R E A T I O N     T R A I N I N G    D A T A  /   V A L I D A T I O N    D A T A
    //
    //******************************************************************************************
    val Array(trainingData, validationData) = df.randomSplit(Array(0.9, 0.1))
    trainingData.cache()
    validationData.cache()

    //*****************************************************************************************************************
    //
    //       C R E A T I O N     D'U N      M O D E L E   D E     R E G R E S S I O N      L O G I S T I Q U E
    //
    //*****************************************************************************************************************
    val lr = new LogisticRegression()
      .setElasticNetParam(1.0)  // L1-norm regularization : LASSO
      .setLabelCol("label")
      //.setFeaturesCol("scaled_features")
      .setStandardization(true)  // to scale each feature of the model
      .setFitIntercept(true)  // we want an affine regression (with false, it is a linear regression)
      .setTol(1.0e-5)  // stop criterion of the algorithm based on its convergence
      .setMaxIter(300)  // a security stop criterion to avoid infinite loops


    //********************************************************************************
    //
    //    C A L C U L     D E     L  H Y P E R P A R A M E T R E       O P T I M A L
    //
    //********************************************************************************

    // Creating the array of Search Grid with Logarithmic Scale
    println("Initializing the Grid array ***********")
    val array = -6.0 to (0.0, 0.5) toArray
    val arrayLog = array.map(x => math.pow(10,x))
    //println(arrayLog.deep.mkString("\n"))
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, arrayLog)
      .build()

    // Creating the BinaryClassificationEvaluator
    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")

    // Spliting the data for the second time (70 30)
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.7)

    //********************************************************************************
    //
    //                          P R E D I C T I O N S
    //
    //********************************************************************************

    val lrModel = trainValidationSplit.fit(trainingData)
    //Réalise les prédictions sur le validation set à partir du meilleur hyperparamètre évalué précédemment.
    val df_WithPredictions = lrModel.transform(validationData).select("features", "label", "prediction")

    //********************************************************************************
    //
    //            A F F I C H A G E    D E S      P E R F O R M A N C E S
    //
    //********************************************************************************

    // Affichage de la matrice de confusion
    df_WithPredictions.groupBy("label", "prediction").count.show()

    // Affichage de notre score de prédiction
    evaluator.setRawPredictionCol("prediction")
    println("Model accuracy : "  + evaluator.evaluate(df_WithPredictions).toString())
    return lrModel

  }



  def main(args: Array[String]): Unit = {
    implicit val spark = loadSparkSesssion()
    implicit val sc1 =  spark.sparkContext
    import spark.implicits._


    val df = loadDataFrame(spark, args(1))
    val cleanDF = dataCleaning(df)
    val model = myLogisticRegression(cleanDF)

    // Saving the file
    sc1.parallelize(Seq(model), 1).saveAsObjectFile(args(0))
  }
}
