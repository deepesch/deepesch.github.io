/* wikimediabot.scala */

/*
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
*/

import org.apache.spark.SparkConf

import java.util.Properties
import kafka.producer._


/* spark streaming */
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

/* kafka imports */
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.kafka.KafkaUtils._


/* json parsing */
import net.liftweb.json._

// Get default date formats

object WikimediaBotReader{
	
  implicit val formats = DefaultFormats

  case class WikimediaIRCMessage(
	timestamp: java.util.Date,
	topic: String,
	url: String,
	user: String,
	size: Int,
	comment: String,
	flags: String,

	minor: Boolean,
	new_page: Boolean,
	robot: Boolean,
	unpatrolled: Boolean,

	anonymous: Boolean,
	topic_talk: Boolean,
	topic_user: Boolean,
	topic_wikipedia: Boolean

	)

  def main(args: Array[String]){

	val appName = "wikimediabot"
	val cycleInterval = Seconds(5)

	// Check if calling arguments were provided
	if (args.length < 4) {
		System.err.println("Usage: [PROGRAM_NAME] <zkQuorum> <group> <topics> <numThreads>")
		System.exit(1)
	}

	// Convert calling argumnets to array
	val Array(zkQuorum, group, topics, numThreads) = args

	// Declare long-integer counter for our test
	var totalCount = 0L


	// Initialize new Spark Configuration
    val sparkConf = new SparkConf().setAppName(appName)

	// Initialize new Spark Streaming Context
	// !! The second argument here determines our polling interval !!
    val ssc =  new StreamingContext(sparkConf, cycleInterval)

	// Declare starting checkpoint for streaming context
    ssc.checkpoint("checkpoint")



	// Split 'topic' argument into "Map" type
	// (<topic name>, <numThreads>)
    val topicpMap = topics.split(",").map((_,numThreads.toInt)).toMap


	// Create Kafka stream as consumer 
    val stream = KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap).map(_._2)


	// A counter for the consumer interval we are running (e.g. keep track of n-th batch)
	var cycles = 1L

    // Print out the count of events received from this server in each microbatch
    stream.foreachRDD((rdd: RDD[_], time: Time) => { 
      val count = rdd.count()

	  println("\n** START OF CYCLE " + cycles + " **")

	  println("-------------------")
      println("Time: " + time)
      println("Received " + count + " events")
      println("-------------------")
      totalCount += count 

	// Perform transformation steps if microbatch contains events
	if (count > 0){

	// Display contents of each microbatch	  
	//rdd.foreach(println)

	// turn rdd elements into an array
	//	val lines = rdd.collect()
	val lines = rdd.take(1)
	//	lines.foreach(println)

	lines.foreach(println)

	println(lines.length)	

	// Convert array elements to strings	
	val derp = lines.mkString

	// Parse strings as JSON objects
	val json = parse(derp)
	val temp = json.extract[WikimediaIRCMessage]
	println(temp.topic)

	}

	  println("\n** END OF CYCLE " + cycles + " **\tEvents in cycle: " + count + "\t Total seen: " + totalCount + "\n")
	  cycles += 1

    })


	// Begin the micro-batch streaming process
    ssc.start()
		// Time is measured in milliseconds (ms) -- Currently set to run for 30 seconds
	    Thread.sleep(30 * 1000)
    ssc.stop()


	// Determine if kafka consumer was successful
    if (totalCount > 0) {
      println("PASSED: Consumed " + totalCount + " message(s)\n")
    } else {
      println("FAILED: Check Kafka broker and publisher(s)\n")
    }


	/* SUMMARY STATISTICS GO HERE */
	println(appName + " Monitored the following topics:")
	
	topicpMap.foreach(println)

	println("Total Messages Processed in Listen Window: " + totalCount + "\n")
	println("Total cycles: " + cycles + "\n")

  }
}

