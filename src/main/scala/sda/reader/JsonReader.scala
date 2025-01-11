package sda.reader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

case class JsonReader(path: String,
                      schema: Option[StructType] = None, // Schéma optionnel
                      multiline: Option[Boolean] = None, // Permettre les JSON multilignes
                      mode: Option[String] = Some("PERMISSIVE")) // Mode de gestion des erreurs
  extends Reader {

  val format: String = "json"

  def read()(implicit spark: SparkSession): DataFrame = {
    // Lecture des données avec les options configurables
    val reader = spark.read.format(format)

    // Appliquer un schéma si fourni
    schema.foreach(reader.schema)

    reader
      .option("multiline", multiline.getOrElse(false)) // Par défaut, JSON simple ligne
      .option("mode", mode.getOrElse("PERMISSIVE")) // Mode par défaut : permissif
      .load(path)
  }
}
