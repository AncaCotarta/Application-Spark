package TestUnitaire

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import sda.traitement.ServiceVente.DataFrameUtils // Import de la classe implicite

class ServiceVenteTest extends AnyFunSuite {

  // Créer une session Spark pour les tests
  val spark: SparkSession = SparkSession.builder()
    .appName("ServiceVenteTest")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  // Test pour formatter()
  test("formatter() should split HTT_TVA into HTT and TVA columns") {
    val inputDF: DataFrame = Seq(
      ("100,5|0,19", "1"),
      ("120,546|0,20", "2"),
      ("123,6|0,201", "3"),
      ("5,546|0,15", "4")
    ).toDF("HTT_TVA", "Id_Client")

    val resultDF = inputDF.formatter()
    val expectedDF = Seq(
      ("100,5|0,19", "1", "100,5", "0,19"),
      ("120,546|0,20", "2", "120,546", "0,20"),
      ("123,6|0,201", "3", "123,6", "0,201"),
      ("5,546|0,15", "4", "5,546", "0,15")
    ).toDF("HTT_TVA", "Id_Client", "HTT", "TVA")

    assert(resultDF.columns.toSet === expectedDF.columns.toSet)
    assert(resultDF.collect() === expectedDF.collect())
  }

  // Test pour calculTTC()
  test("calculTTC() should calculate the correct TTC values") {
    val inputDF: DataFrame = Seq(
      ("100,5|0,19", "1"),
      ("120,546|0,20", "2"),
      ("123,6|0,201", "3"),
      ("5,546|0,15", "4")
    ).toDF("HTT_TVA", "Id_Client")

    val resultDF = inputDF.formatter().calculTTC()
    val expectedDF = Seq(
      ("100,5|0,19", "1", 119.6),
      ("120,546|0,20", "2", 144.66),
      ("123,6|0,201", "3", 148.44),
      ("5,546|0,15", "4", 6.38)
    ).toDF("HTT_TVA", "Id_Client", "TTC")

    assert(resultDF.columns.toSet === expectedDF.columns.toSet)
    assert(resultDF.collect() === expectedDF.collect())
  }

  // Test pour la configuration CSV
  test("MainBatch should correctly handle CSV configuration") {
    val csvDF = spark.read
      .option("header", "true")
      .option("delimiter", "#")
      .csv("src/main/resources/DataforTest/data.csv") // Chemin vers le fichier CSV

    val preparedDF = csvDF.formatter().calculTTC()
    val resultDF = preparedDF.extractDateEndContratVille()

    val expectedDF = Seq(
      ("1", "100,5|0,19", 119.6, "Paris", java.sql.Date.valueOf("2020-12-23")),
      ("2", "120,546|0,20", 144.66, "Alger", java.sql.Date.valueOf("2023-12-23")),
      ("3", "123,6|0,201", 148.44, "Dakar", java.sql.Date.valueOf("2020-12-23")),
      ("4", "5,546|0,15", 6.38, "Abidjan", java.sql.Date.valueOf("2024-12-23"))
    ).toDF("Id_Client", "HTT_TVA", "TTC", "Ville", "Date_End_contrat")

    assert(resultDF.columns.toSet === expectedDF.columns.toSet)
    assert(resultDF.collect() === expectedDF.collect())
  }

  // Test pour la configuration JSON
  test("MainBatch should correctly handle JSON configuration") {
    val jsonDF = spark.read
      .option("mode", "FAILFAST") // Arrête en cas d'erreur dans le JSON
      .option("multiline", "true") // Indique que le fichier JSON est multi-lignes
      .json("src/main/resources/DataforTest/data.json") // Chemin vers le fichier JSON

    val preparedDF = jsonDF.formatter().calculTTC()
    val resultDF = preparedDF.extractDateEndContratVille()

    val expectedDF = Seq(
      ("1", "100,5|0,19", 119.6, "Paris", java.sql.Date.valueOf("2024-12-23")),
      ("2", "120,546|0,20", 144.66, "Alger", java.sql.Date.valueOf("2024-12-23")),
      ("3", "123,6|0,201", 148.44, "Dakar", java.sql.Date.valueOf("2020-12-23")),
      ("4", "5,546|0,15", 6.38, "Abidjan", java.sql.Date.valueOf("2024-12-23"))
    ).toDF("Id_Client", "HTT_TVA", "TTC", "Ville", "Date_End_contrat")

    // Vérification des colonnes et des données
    assert(resultDF.columns.toSet === expectedDF.columns.toSet)
    assert(resultDF.collect() === expectedDF.collect())
  }

  // Test pour la configuration XML
  test("MainBatch should correctly handle XML configuration") {
    val xmlDF = spark.read
      .format("com.databricks.spark.xml") // Utiliser Spark-XML
      .option("rowTag", "Client") // Spécifier le tag de ligne
      .load("src/main/resources/DataforTest/data.xml") // Chemin vers le fichier XML

    // Préparer le DataFrame
    val preparedDF = xmlDF
      .withColumnRenamed("_Id_Client", "Id_Client") // Si les colonnes XML sont mal formatées
      .formatter()
      .calculTTC()

    // Extraire les données nécessaires
    val resultDF = preparedDF.extractDateEndContratVille()

    // Résultat attendu
    val expectedDF = Seq(
      ("1", "100,5|0,19", 119.6, "Paris", java.sql.Date.valueOf("2024-12-23")),
      ("2", "120,546|0,20", 144.66, "Alger", java.sql.Date.valueOf("2024-12-23")),
      ("3", "123,6|0,201", 148.44, "Dakar", java.sql.Date.valueOf("2020-12-23")),
      ("4", "5,546|0,15", 6.38, "Abidjan", java.sql.Date.valueOf("2024-12-23"))
    ).toDF("Id_Client", "HTT_TVA", "TTC", "Ville", "Date_End_contrat")

    // Vérification robuste en triant les DataFrames pour garantir l'ordre
    val sortedResultDF = resultDF.orderBy("Id_Client")
    val sortedExpectedDF = expectedDF.orderBy("Id_Client")

    assert(sortedResultDF.columns === sortedExpectedDF.columns) // Vérifie les colonnes
    assert(sortedResultDF.except(sortedExpectedDF).isEmpty) // Vérifie les données
  }

  test("contratStatus() should correctly add Contrat_Status column") {
    import spark.implicits._

    // Exemple de DataFrame d'entrée
    val inputDF = Seq(
      ("1", "100,5|0,19", 119.6, "Paris", java.sql.Date.valueOf("2020-12-23")),
      ("2", "120,546|0,20", 144.66, "Alger", java.sql.Date.valueOf("2024-12-23")),
      ("3", "123,6|0,201", 148.44, "Dakar", java.sql.Date.valueOf("2023-12-23")), // Supposons que c'est dans le passé
      ("4", "5,546|0,15", 6.38, "Abidjan", java.sql.Date.valueOf("2025-01-01")) // Date future
    ).toDF("Id_Client", "HTT_TVA", "TTC", "Ville", "Date_End_contrat")

    // Appel de la méthode contratStatus()
    val resultDF = inputDF.contratStatus()

    // Résultats attendus
    val expectedDF = Seq(
      ("1", "100,5|0,19", 119.6, "Paris", java.sql.Date.valueOf("2020-12-23"), "Expired"),
      ("2", "120,546|0,20", 144.66, "Alger", java.sql.Date.valueOf("2024-12-23"), "Expired"),
      ("3", "123,6|0,201", 148.44, "Dakar", java.sql.Date.valueOf("2023-12-23"), "Expired"),
      ("4", "5,546|0,15", 6.38, "Abidjan", java.sql.Date.valueOf("2025-01-01"), "Active")
    ).toDF("Id_Client", "HTT_TVA", "TTC", "Ville", "Date_End_contrat", "Contrat_Status")

    // Vérification des colonnes et des données
    assert(resultDF.columns.toSet === expectedDF.columns.toSet)
    assert(resultDF.collect() === expectedDF.collect())
  }



}
