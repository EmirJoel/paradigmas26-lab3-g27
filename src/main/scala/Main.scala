import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    // Inicializa Acumuladores
    val feedsSuccessAcc  = sc.longAccumulator("feedsSuccess")
    val feedsFailedAcc   = sc.longAccumulator("feedsFailed")

    val postsSuccessAcc  = sc.longAccumulator("postsSuccess")
    val postsFailedAcc   = sc.longAccumulator("postsFailed")

    val postsFilteredAcc = sc.longAccumulator("postsFiltered")

    val totalCharsAcc    = sc.longAccumulator("totalChars")

    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => 
        spark.stop()
        return // scopt prints error messages
    }

    // Load subscriptions and handle errors
    val subscriptionOpts = try {
      FileIO.readSubscriptions(cmdArgs.subscriptionFile)
    } catch {
      case _: java.io.FileNotFoundException =>
        println(s"Error: Could not load ${cmdArgs.subscriptionFile} file not found")
        spark.stop()
        return
      case _: org.json4s.ParserUtil.ParseException | _: Exception =>
        println(s"Error: Could not load ${cmdArgs.subscriptionFile} invalid JSON format")
        spark.stop()
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
      spark.stop()
      return
    }

    // Parallelize subscriptions for Spark processing
    val subscriptionRDD = sc.parallelize(subscriptions)
    val postsRDD = subscriptionRDD.flatMap { subscription =>
      try {
        val feedOpt = FileIO.downloadFeed(subscription.url)
        feedOpt match {
          case Some(jsonContent) =>
            feedsSuccessAcc.add(1)
            val posts =
              try {
                JsonParser.parsePosts(jsonContent, subscription.name)
              } catch {
                case _: Exception =>
                  postsFailedAcc.add(1)
                  println(s"Warning: Failed to parse posts from ${subscription.name} (${subscription.url})")
                  List.empty[Post]
              }
            postsSuccessAcc.add(posts.size)
            val filteredPosts = Analyzer.filterEmptyPosts(posts)
            postsFilteredAcc.add(posts.size - filteredPosts.size)
            filteredPosts.foreach { post =>
              totalCharsAcc.add((post.title + post.selftext).length
              )
            }
            filteredPosts
          case None =>
            feedsFailedAcc.add(1)
            List.empty[Post]
        }
      } catch {
        case _: Exception =>
          feedsFailedAcc.add(1)
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          List.empty[Post]
      }
    }.cache()
   // Check if entities directory exists before loading dictionaries
    val dirFile = new java.io.File(cmdArgs.entitiesDir)
    if (!dirFile.exists() || !dirFile.isDirectory) {
      println(s"Error: entities directory '${cmdArgs.entitiesDir}' not found")
      spark.stop()
      return
    }

    // Load dictionaries (Ejercicio 3 sigue desde aca)
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    if (dictionary.isEmpty) {
      println("Error: No entities loaded from dictionaries")
      spark.stop()
      return
    }

    val dictionaryBroadcast = sc.broadcast(dictionary)
    
    // a) Extraer entidades de título y cuerpo en paralelo en los Workers
    val entitiesRDD = postsRDD.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      val entitiesList = Analyzer.detectEntities(combinedText, dictionaryBroadcast.value)
      entitiesList.iterator 
    }.cache()

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


    // ========================================================================
    // COMODATO DE DATOS PARA FORMATEADORES (Post-Cómputo Distribuido)
    // ========================================================================
    
    // Convertimos sortedResults al formato Map[(String, String), Int] que espera formatEntityStats
    val finalEntityCounts = sortedResults.map { case (count, (tipo, nombre)) => ((tipo, nombre), count) }.toMap

    // Re-expandimos localmente en el Driver para calcular las estadísticas por tipo de categoría
    val finalEntitiesList = sortedResults.flatMap { case (count, (tipo, nombre)) => List.fill(count)((tipo, nombre)) }
    val typeStats = finalEntitiesList.groupBy(_._1).view.mapValues(_.size).toMap + ("total" -> finalEntitiesList.length)

    val avgChars =
      if (postsSuccessAcc.value > 0)
        totalCharsAcc.value / postsSuccessAcc.value
      else
        0

    val stats = Map(
      "feedsSuccess"  -> feedsSuccessAcc.value.toInt,
      "feedsFailed"   -> feedsFailedAcc.value.toInt,
      "postsSuccess"  -> postsSuccessAcc.value.toInt,
      "postsFailed"   -> postsFailedAcc.value.toInt,
      "postsFiltered" -> postsFilteredAcc.value.toInt,
      "avgChars"      -> avgChars.toInt
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
  
    spark.stop()
  }
}
