package io.elegans.calc_es_lda

import org.apache.spark.mllib.clustering.{LDA, DistributedLDAModel, OnlineLDAOptimizer, EMLDAOptimizer}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.elasticsearch.spark._

import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql._

import scala.collection.mutable.Buffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.MutableList
import scala.collection.mutable.LinkedHashMap

import scopt.OptionParser

/* import core nlp */
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._
/* these are necessary since core nlp is a java library */
import java.util.Properties
import scala.collection.JavaConversions._

import org.apache.spark.storage.StorageLevel

object EsSparkApp {

  def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize, ssplit, pos, lemma")
    val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)
    pipeline
  }

  def isOnlyLetters(str: String): Boolean = {
    str.forall(c => Character.isLetter(c))
  }

  def plainTextToLemmas(text: String, stopWords: Set[String], pipeline: StanfordCoreNLP): Seq[String] = {
    val doc: Annotation = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences;
         token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.getString(classOf[LemmaAnnotation])
  	  val lc_lemma = lemma.toLowerCase
      if (lc_lemma.length > 2 && !stopWords.contains(lc_lemma) && isOnlyLetters(lc_lemma)) {
        lemmas += lc_lemma.toLowerCase
      }
    }
	lemmas
  }

  private case class Params(
    hostname: String = "localhost",
    port: String = "9200",
    search_path: String = "jenny-en-0/question",
    query: String = """{ "fields":["question", "answer", "conversation", "index_in_conversation", "_id" ] }""",
    used_fields: Seq[String] = Seq[String]("question", "answer"),
    group_by_field: Option[String] = None,
    max_k: Int = 10,
    min_k: Int = 8,
    maxIterations: Int = 100,
    outputDir: String = "/tmp",
    stopwordFile: Option[String] = Option("stopwords/en_stopwords.txt"),
    maxTermsPerTopic: Int = 10)

  private def doLDA(params: Params) {
    val conf = new SparkConf().setAppName("LDA from ES data")
    conf.set("es.nodes.wan.only", "true")
    conf.set("es.nodes", params.hostname)
    conf.set("es.port", params.port)

    val query: String = params.query
    conf.set("es.query", query)

    val sc = new SparkContext(conf)
    val search_res = sc.esRDD(params.search_path, "?q=*")

    val stopWords: Set[String] = params.stopwordFile match {
      case Some(stopwordFile) => sc.broadcast(scala.io.Source.fromFile(stopwordFile)
        .getLines().map(_.trim).toSet).value
      case None => Set.empty
    }

    /* docTerms: map of (docid, list_of_lemmas) */
    val used_fields = params.used_fields
    val docTerms = params.group_by_field match {
      case Some(group_by_field) =>
        val tmpDocTerms = search_res.map(s => {
          val key = s._2.getOrElse(group_by_field, "")
          (key, s._2)
        }
        ).groupByKey().map( s => {
          val conversation : String = s._2.foldRight("")((a, b) =>
            try {
              val c = used_fields.map( v => { a.getOrElse(v, None) } )
                .filter(x => x != None).mkString(" ") + " " + b
              c
            } catch {
              case e: Exception => ""
            }
          )
          try {
            val pipeline = createNLPPipeline()
            val doc_lemmas = (s._1, plainTextToLemmas(conversation, stopWords, pipeline))
            doc_lemmas
          } catch {
            case e: Exception => (None, List[String]())
          }
        })
        tmpDocTerms
      case None =>
        val tmpDocTerms = search_res.map(s => {
          try {
            val pipeline = createNLPPipeline()
            val doctext = used_fields.map( v => {
                s._2.getOrElse(v, "")
              } ).mkString(" ")
            val doc_lemmas = (s._1, plainTextToLemmas(doctext, stopWords, pipeline))
            doc_lemmas
          } catch {
            case e: Exception => (None, List[String]())
          }
        })
        tmpDocTerms
    }

    /* docTermFreqs: mapping <doc_id> -> (doc_id_str, Map(term, freq)) ; term frequencies for each doc */
    val docTermFreqs = docTerms.filter(_._1 != None).zipWithIndex.map {
      case ((conv_id_str, terms), conv_id) => {
        /* termFreqs: mapping term -> freq ; frequency for each term in a doc */
        val termFreqs = terms.foldLeft(new HashMap[String, Int]()) {
          (term_freq_map, term) => {
            term_freq_map += term -> (term_freq_map.getOrElse(term, 0) + 1)
            term_freq_map
          }
        }
        val doc_data = (conv_id, (conv_id_str, termFreqs))
        doc_data
      }
    }

    docTermFreqs.persist(StorageLevel.MEMORY_AND_DISK)

    /* terms_id_map: mapping term -> index */
    val terms_id_map = docTermFreqs.flatMap(_._2._2.keySet).distinct().zipWithIndex.collectAsMap()

    /* terms_id_list: ordered by index, list of all terms (term, index)*/
    val terms_id_list = terms_id_map.toSeq.sortBy {_._2}

    /* doc_id_map: mapping doc_id -> terms_freq */
    val doc_id_map = docTermFreqs.collectAsMap()

    sc.broadcast(terms_id_map)
    sc.broadcast(terms_id_list)

    /* entries: mapping doc_id -> List(<term_index, term_frequency_in_doc>)*/
    val entries = docTermFreqs.map(doc => {
      (doc._1, terms_id_list.map({term =>
        (term._2, doc._2._2.getOrElse(term._1, 0))
      }))
    })

    val num_of_terms : Int = terms_id_map.size
    val num_of_docs : Long = doc_id_map.size
    val corpus = entries.map ( { entry =>
      val seq : Seq[(Int, Double)] = entry._2.map(v => {
        val term_index = v._1.asInstanceOf[Int]
        val term_occourrence = v._2.asInstanceOf[Double]
        (term_index, term_occourrence)
      }).filter(_._2 != 0.0)
      val vector = Vectors.sparse(num_of_terms, seq)
      (entry._1, vector)
    } )

    corpus.persist(StorageLevel.MEMORY_AND_DISK)

    /* COMPUTE LDA */
    val max_k : Int = params.max_k
    val min_k : Int = params.min_k
    var k : Int = min_k

    var k_all_values : MutableList[(Int, Double)] = MutableList[(Int, Double)]()
    var iterate : Boolean = true
    do {
      val lda = new LDA()
      val optimizer = new EMLDAOptimizer

      lda.setOptimizer(optimizer).setK(k).setMaxIterations(100)
      val ldaModel = lda.run(corpus)

      //val logPerplexity = ldaModel.asInstanceOf[DistributedLDAModel].logPerplexity
      val logPrior = ldaModel.asInstanceOf[DistributedLDAModel].logPrior
      val logLikelihood = ldaModel.asInstanceOf[DistributedLDAModel].logLikelihood
      val avglogLikelihood = logLikelihood / num_of_docs
      val avglogPrior = logPrior / num_of_docs
      println("K(" + k + ") LOGLIKELIHOOD(" + logLikelihood + ") LOG_PRIOR(" +
        logPrior + ") AVGLOGLIKELIHOOD(" + avglogLikelihood + ") AVGLOG_PRIOR(" + avglogPrior + ") NumDocs(" +
        num_of_docs + ") VOCABULAR_SIZE(" + ldaModel.vocabSize + ")")

      // Check: log probabilities
      assert(logLikelihood < 0.0)
      assert(logPrior < 0.0)

      iterate = if (k < max_k) true else false

      /* begin print topics */
      //TODO: to implement //val outTopicDirname = "TOPICS_K." + k + "_DN." + num_of_docs + "_VS." + ldaModel.vocabSize
      //TODO: to implement //val outTopicFilePath = params.outputDir + "/" + outTopicDirname
      val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = params.maxTermsPerTopic)

      topicIndices.zipWithIndex.foreach { case ((terms, termWeights), topic_i) =>
        println("TOPIC: (#" + k + "): " + topic_i)
        terms.zip(termWeights).foreach { case (term, weight) =>
          println(s"${terms_id_list(term)}\t$weight")
        }
        println()
      }
      //TODO: to implement //val topics_data_serializer = sc.parallelize(XXXXDATAXXXX)
      //TODO: to implement //topics_data_serializer.saveAsTextFile(outTopicFilePath)

      println("#BEGIN DOC_TOPIC_DIST K(" + k + ")")
      //TODO: to implement //val outTopicPerDocumentDirname = "TOPICSxDOC_K." + k + "_DN." + num_of_docs + "_VS." + ldaModel.vocabSize
      //TODO: to implement //val outTopicPerDocumentFilePath = params.outputDir + "/" + outTopicPerDocumentDirname
      val topKTopicsPerDoc = ldaModel.asInstanceOf[DistributedLDAModel]
        .topTopicsPerDocument(k).map(t => (t._1, (t._2, t._3)) /* (doc_id, (topic_i, weight)) */)

      topKTopicsPerDoc.foreach { d =>
        val doc_topic_ids: List[Int] = d._2._1.toList
        val doc_topic_weights: List[Double] = d._2._2.toList
        val doc_id = doc_id_map(d._1)._1
        //val doc_terms_freq = doc_id_map(d._1)._2
        val topics_weights: List[(Int, Double)] = doc_topic_ids.zip(doc_topic_weights)
        println("DOC: (" + doc_id + ") -> [topics_weights(" + topics_weights + ")]" )
      }
      println("#END DOC_TOPIC_DIST K(" + k + ")")

      k += 1
    } while (iterate)
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("LDA with ES data") {
      head("calculate LDA with data from an elasticsearch index.")
      opt[String]("hostname")
        .text(s"the hostname of the elasticsearch instance" +
          s"  default: ${defaultParams.hostname}")
        .action((x, c) => c.copy(hostname = x))
      opt[String]("port")
        .text(s"the port of the elasticsearch instance" +
          s"  default: ${defaultParams.port}")
        .action((x, c) => c.copy(port = x))
      opt[String]("group_by_field")
        .text(s"group the search results by field e.g. conversation, None => no grouping" +
          s"  default: ${defaultParams.group_by_field}")
        .action((x, c) => c.copy(group_by_field = Option(x)))
      opt[String]("search_path")
        .text(s"the search path on elasticsearch e.g. <index name>/<type name>" +
          s"  default: ${defaultParams.search_path}")
        .action((x, c) => c.copy(search_path = x))
      opt[String]("query")
        .text(s"a json string with the query" +
          s"  default: ${defaultParams.query}")
        .action((x, c) => c.copy(query = x))
      opt[Int]("min_k")
        .text(s"min number of topics. default: ${defaultParams.min_k}")
        .action((x, c) => c.copy(min_k = x))
      opt[Int]("max_k")
        .text(s"max number of topics. default: ${defaultParams.max_k}")
        .action((x, c) => c.copy(max_k = x))
      opt[Int]("maxTermsPerTopic")
        .text(s"the max number of terms per topic. default: ${defaultParams.maxTermsPerTopic}")
        .action((x, c) => c.copy(maxTermsPerTopic = x))
      opt[Int]("maxIterations")
        .text(s"number of iterations of learning. default: ${defaultParams.maxIterations}")
        .action((x, c) => c.copy(maxIterations = x))
      opt[String]("stopwordFile")
        .text(s"filepath for a list of stopwords. Note: This must fit on a single machine." +
          s"  default: ${defaultParams.stopwordFile}")
        .action((x, c) => c.copy(stopwordFile = Option(x)))
      opt[Seq[String]]("used_fields")
        .text(s"list of fields to use for LDA, if more than one they will be merged" +
          s"  default: ${defaultParams.used_fields}")
        .action((x, c) => c.copy(used_fields = x))
      opt[String]("outputDir")
        .text(s"TO BE IMPLEMENTED: the where to store the output files: topics and document per topics" +
          s"  default: ${defaultParams.outputDir}")
        .action((x, c) => c.copy(outputDir = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        doLDA(params)
      case _ =>
        sys.exit(1)
    }
  }
}
