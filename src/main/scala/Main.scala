import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext

    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return // scopt prints error messages
    }

    // Load subscriptions and handle errors
    val subscriptionOpts = try {
      FileIO.readSubscriptions(cmdArgs.subscriptionFile)
    } catch {
      case _: java.io.FileNotFoundException =>
        println(s"Error: Could not load ${cmdArgs.subscriptionFile} file not found")
        return
      case _: org.json4s.ParserUtil.ParseException | _: Exception =>
        println(s"Error: Could not load ${cmdArgs.subscriptionFile} invalid JSON format")
        return
    }

    // Validate subscriptions and print warnings for malformed entries
    subscriptionOpts.foreach {
      case None => println("Warning: Skipping malformed subscription (missing 'name' or 'url' field)")
      case Some(_) => // Valid subscription, do nothing
    }

    // Filter out malformed subscriptions (None values)
    val subscriptions = subscriptionOpts.flatten

    // Check if there are any valid subscriptions to process
    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      return
    }

    // Parallelize subscriptions for Spark processing
    val subscriptionRDD = sc.parallelize(subscriptions)

    // Download feeds and parse posts
    val postsRDD = subscriptionRDD.flatMap { subscription => 
      try {
        val feedOpt = FileIO.downloadFeed(subscription.url)
        // Try parsing posts, if fails log a warning and return empty list for this subscription
        val posts = feedOpt.fold(List[Post]()) { jsonContent =>
          try {
            JsonParser.parsePosts(jsonContent, subscription.name)
          } catch {
            case _: Exception =>
              println(s"Warning: Failed to parse posts from ${subscription.name}' (${subscription.url})")
              List.empty[Post]
          }
        }
        Analyzer.filterEmptyPosts(posts)
      } catch {
        case _: Exception => 
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          List.empty[Post]
      }
    }
   
   // Check if entities directory exists before loading dictionaries
    val dirFile = new java.io.File(cmdArgs.entitiesDir)
    if (!dirFile.exists() || !dirFile.isDirectory) {
      println(s"Error: entities directory '${cmdArgs.entitiesDir}' not found")
      return
    }

    // Load dictionaries (Ejercicio 3 sigue desde aca)
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    val dictionaryBroadcast = sc.broadcast(dictionary)
    
    // a) Extraer entidades de título y cuerpo en paralelo en los Workers
    val entitiesRDD = postsRDD.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      val entitiesList = Analyzer.detectEntities(combinedText, dictionaryBroadcast.value)
      entitiesList.iterator 
    }

    // b) Mapear a par clave-valor ((tipo, nombre), 1)
    val pairsRDD = entitiesRDD.map { entity =>
      ((entity.entityType, entity.text), 1)
    }

    // c) Reducir de forma distribuida sumando las apariciones (Shuffle)
    val countsRDD = pairsRDD.reduceByKey((contador1, contador2) => contador1 + contador2)

    // d) Ordenar globalmente de mayor a menor y recolectar los resultados refinados
    val sortedResults = countsRDD
      .map { case ((tipo, nombre), count) => (count, (tipo, nombre)) }
      .sortByKey(ascending = false) 
      .collect() // Única acción que trae los datos finales calculados al Driver


   // Validar si bajaron datos antes de formatear
    if (sortedResults.isEmpty) {
      println("Error: No entities found or downloaded posts are empty")
      return
    }

    // ========================================================================
    // COMODATO DE DATOS PARA FORMATEADORES (Post-Cómputo Distribuido)
    // ========================================================================
    
    // Convertimos sortedResults al formato Map[(String, String), Int] que espera formatEntityStats
    val finalEntityCounts = sortedResults.map { case (count, (tipo, nombre)) => ((tipo, nombre), count) }.toMap

    // Re-expandimos localmente en el Driver para calcular las estadísticas por tipo de categoría
    val finalEntitiesList = sortedResults.flatMap { case (count, (tipo, nombre)) => List.fill(count)((tipo, nombre)) }
    val typeStats = finalEntitiesList.groupBy(_._1).view.mapValues(_.size).toMap + ("total" -> finalEntitiesList.length)

    // TODO (Ejercicio 4): Reemplazar este mapa provisional por los Accumulators reales
    // Dejamos este placeholder fijo para que compile y mantenga el diseño de la cátedra
    val stats = Map(
      "feedsSuccess"  -> 0,
      "feedsFailed"   -> 0,
      "postsSuccess"  -> finalEntitiesList.length, // Estimación temporal basada en el conteo final
      "postsFailed"   -> 0,
      "postsFiltered" -> 0,
      "avgChars"      -> 1110                      // Valor promedio hardcodeado temporalmente
    )

    // ========================================================================
    // IMPRESIONES FINALES EN CONSOLA
    // ========================================================================
    
    // 1. Estadísticas Generales de Procesamiento (Pendiente completar Ejercicio 4)
    println(Formatters.formatProcessingStats(stats))
    println()

    // 2. Estadísticas de Categorías y Ranking de Entidades Nombradas (Ejercicio 3 Completado)
    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(finalEntityCounts, cmdArgs.topK))
  }
}
