# Grupo 27:
* Aquino, Emir Martin Joel
* Rivadeneira, Patricio Tobias
* Simonian, Fabrizio Joaquin
* Wortley, Tiziano Angel

# (a)(Emir)
main(args: Array[String])
    |
    v
CommandLineArgs.parse(args)
    |
    v
Option[CommandLineArgs]
    |
    v
readSubscriptions(cmdArgs.subscriptionFile)
    |
    v
List[Option[Subscription]]
    |
    v
flatten
    |
    v
List[Subscription]
    |
    v
FileIO.downloadFeed(subscription.url)
    |
    v
Option[String]
    |
    v
JsonParser.parsePosts(json)
    |
    v
List[Post]
    |
    v
flatMap
    |
    v
List[Post]
    |
    v
filterEmptyPosts
    |
    v
List[Post]
    |
    +----------------------+
    |                      |
    v                      v
estadísticas         Analyzer.detectEntities(combinedText,EntityDir)
                            |
                            v
                    List[NamedEntity]
                            |
                +-----------+-----------+
                |                       |
                v                       v
         countEntities           countByType
                |                       |
                v                       v
 Map[(String,String),Int]     Map[String,Int]
                |
                v
      formatEntityStats
                |
                v
             Output
# (b)(Tizi)

# (c)(Pato)

# (d)(Simo)
