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
        val posts = feedOpt.fold(List[Post]())(JsonParser.parsePosts(_, subscription.name))
        Analyzer.filterEmptyPosts(posts)
      } catch {
        case _: Exception => 
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
          List.empty[Post]
      }
    }

    // EJERCICIO 2 COMPLETADO: traemos los datos del cluster al Driver para que el resto del programa secuencial siga funcionando hasta hacer el Ej. 3
    val filteredPosts = postsRDD.collect().toList

    // Check if we have any posts to process
    if (filteredPosts.isEmpty) {
      println("Error: No valid posts downloaded after filtering")
      return
    }

    // TODO (Ejercicio 4): Reemplazar estos valores por Accumulators de Spark
    // Por ahora los calculamos sobre la lista local para que compile
    val totalChars = filteredPosts.map(post => post.title.length + post.selftext.length).sum
    val avgChars = if (filteredPosts.nonEmpty) totalChars / filteredPosts.length else 0

    // Prepare statistics
    val stats = Map(
      "feedsSuccess" -> 0 /* feedsSuccess (Pendiente Ejercicio 4)*/,
      "feedsFailed" -> 0 /* feedsFailed (Pendiente Ejercicio 4)*/,
      "postsSuccess" -> filteredPosts.length,
      "postsFailed" -> 0 /* postsFailed (Pendiente Ejercicio 4)*/,
      "postsFiltered" -> 0 /* postsFiltered (Pendiente Ejercicio 4)*/,
      "avgChars" -> avgChars
    )

    // Print output
    println(Formatters.formatProcessingStats(stats))
    println()

    // Load dictionaries (Ejercicio 3 sigue desde aca)
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Detect entities in all posts (combine title and selftext)
    val allEntities = filteredPosts.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }

    // Count entities
    val entityCounts = Analyzer.countEntities(allEntities)
    val typeStats = Analyzer.countByType(allEntities)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))
  }
}
