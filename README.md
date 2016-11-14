<h2> Spark-planets </h2> 
Tp Spark INF729 - Daphné Pertsekos - Adam Khouiy

<h4> Ligne de commande utilisée pour lancer le job : </h4> 
   <b>./spark-submit --master local --class com.sparkProject.JobML /home/daphne/TP_Spark/tp_spark/target/scala-2.11/tp_spark-assembly-1.0.jar </b>

<h4> Fonctionnement du programme </h4> 
La classe JobML est constituée d'une méthode main qui appelle les méthodes : 
<ul>
   <li> loadSparkSesssion(): SparkSession  </li>
   <li> loadDataFrame(spark:SparkSession):DataFrame</li>
   <li> dataCleaning(df: DataFrame): DataFrame </li>
   <li> myLogisticRegression(df:DataFrame):TrainValidationSplitModel</li>
</ul>

<h5> loadSparkSesssion(): SparkSession </h5>
Crée et renvoie une sparkSession

<h5> loadDataFrame(spark:SparkSession):DataFrame</h5>
Charge les données à traiter à partir d'un fichier au format parquet. Ce format n'est pas destiné à être lisible par des 
humains mais permet de compresser au mieux les données. 

<h5> dataCleaning(df: DataFrame): DataFrame </h5>
Packages les features à traiter dans un vecteur unique au lieu de les laisser dispersées sur plusieurs colonnes. 
Création de dummy variables pour la colonne de koi_disposition qui prend les valeurs “CONFIRMED” ou “FALSE-POSITIVE" au lieu de valeurs 0/1.

<h5> myLogisticRegression(df:DataFrame):TrainValidationSplitModel </h5>
Création du modèle de regression logistique. Ici, nous sommes dans le cas binaire, la régression logistique peut se ramener 
à un lasso. En scala, on utilise l'algo ElasticNet avec un hyperparamètre alpha = 1, ce qui revient à implémenter un lasso.

Calcul de l'hyperparamètre lambda optimal à travers une grille de recherche.

Affichage des performances.
