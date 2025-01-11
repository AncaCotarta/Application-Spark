package sda.traitement

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructType, StructField}
import org.apache.spark.sql.functions.{col, from_json, expr, to_date, regexp_replace}

import org.apache.spark.sql.types._


object ServiceVente {

  implicit class DataFrameUtils(dataFrame: DataFrame) {

    def formatter()= {
      dataFrame.withColumn("HTT", split(col("HTT_TVA"), "\\|")(0))
        .withColumn("TVA", split(col("HTT_TVA"), "\\|")(1))
    }

    def calculTTC(): DataFrame = {
      dataFrame
        // Extraire et convertir les colonnes HTT et TVA à partir de HTT_TVA
        .withColumn("HTT", regexp_replace(split(col("HTT_TVA"), "\\|")(0), ",", ".").cast("double"))
        .withColumn("TVA", regexp_replace(split(col("HTT_TVA"), "\\|")(1), ",", ".").cast("double"))
        // Calculer le TTC
        .withColumn("TTC", round(col("HTT") + (col("HTT") * col("TVA")), 2))
        // Supprimer les colonnes intermédiaires HTT et TVA
        .drop("HTT", "TVA")
    }

    def extractDateEndContratVille(): DataFrame = {
      // Détection du type de la colonne MetaData
      dataFrame.schema("MetaData").dataType match {
        case _: StringType => // Cas CSV/JSON : MetaData est une chaîne JSON
          val jsonSchema = StructType(Seq(
            StructField("MetaTransaction", ArrayType(
              StructType(Seq(
                StructField("Ville", StringType, true),
                StructField("Date_End_contrat", StringType, true)
              ))
            ))
          ))

          dataFrame
            .withColumn("parsedMetaData", from_json(col("MetaData"), jsonSchema)) // Parser JSON
            .withColumn("explodedMetaData", expr("explode(parsedMetaData.MetaTransaction)")) // Exploser le tableau
            .withColumn("Ville", col("explodedMetaData.Ville"))
            .withColumn(
              "Date_End_contrat",
              to_date(regexp_replace(col("explodedMetaData.Date_End_contrat"), " .*", ""), "yyyy-MM-dd")
            ) // Nettoyer et convertir la date
            .filter(col("Ville").isNotNull && col("Date_End_contrat").isNotNull) // Supprimer les lignes avec des valeurs nulles
            .select("Id_Client", "HTT_TVA", "TTC", "Ville", "Date_End_contrat") // Garder les colonnes nécessaires

        case _: StructType => // Cas XML : MetaData est une structure
          dataFrame
            .select(
              col("Id_Client"),
              col("HTT_TVA"),
              col("TTC"),
              col("MetaData.MetaTransaction.Ville").as("Ville"),
              to_date(col("MetaData.MetaTransaction.Date_End_contrat"), "yyyy-MM-dd").as("Date_End_contrat")
            )
            .filter(col("Ville").isNotNull && col("Date_End_contrat").isNotNull) // Supprimer les lignes avec des valeurs nulles

        case _ =>
          throw new IllegalArgumentException("Format de la colonne MetaData non supporté")
      }
    }


    def contratStatus(): DataFrame = {
    dataFrame.withColumn(
      "Contrat_Status",
      when(to_date(col("Date_End_contrat"), "yyyy-MM-dd").lt(current_date()), "Expired")
        .otherwise("Active")
    )
  }


}

}
