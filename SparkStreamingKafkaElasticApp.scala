import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.elasticsearch.spark.streaming._



object SparkStreamingKafkaElasticApp {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCounet")
    conf.set("es.index.auto.create", "true");
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "my_group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("textresults")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val lines=stream.map(x=>x.value())
    val words = lines.flatMap(_.split(" "))

    // not necessary since Spark 1.3
    // Count each word in each batch
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _).map(x=>Words(x._1,x._2))

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()
    wordCounts.saveToEs("words/default", Map("es.mapping.id" -> "Mot","es.write.operation" -> "upsert","es.update.script" -> "ctx._source.NbOccurence+=params.para1","es.update.script.params"->"para1:NbOccurence"))

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
  case class Words(Mot: String,NbOccurence: Int)

  }
