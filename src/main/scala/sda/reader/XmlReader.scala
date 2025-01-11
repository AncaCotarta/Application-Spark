package sda.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

case class XmlReader(
                      path: String,
                      rowTag: Option[String] = None
                    ) extends Reader {

  // Valeur par défaut pour le format
  val format: String = "xml"

  // Méthode pour lire un fichier XML et retourner un DataFrame
  def read()(implicit spark: SparkSession): DataFrame = {
    spark.read
      .format(format) // Spécifie que c'est un fichier XML
      .option("rowTag", rowTag.getOrElse("row")) // Définit le tag pour les lignes, "row" par défaut
      .load(path) // Charge le fichier XML depuis le chemin donné
  }
}
