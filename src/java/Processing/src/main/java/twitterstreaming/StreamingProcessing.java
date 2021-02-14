package twitterstreaming;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;

import org.bson.Document;
import org.json.JSONObject;
import util.ReceiverUtil;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
/*
    Receive messages from kafka and form a spark streaming for processing, then store the data into mongodb
 */
public class StreamingProcessing {

    public static void main(String[] args) {
        /* hard code some keywords related to music area */
        String[]keywords = new String[]{"music","song","listen","billboard","singer"};
        ReceiverUtil ru = new ReceiverUtil();
        /* initialize mongodb */
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase db =  mongoClient.getDatabase("twitter");
        MongoCollection<Document> collection = db.getCollection("streaming");

        /* setting up spark and kafka integration*/
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("TwitterStreaming").setMaster("local[*]");

        JavaStreamingContext streamingContext = new JavaStreamingContext(
                sparkConf, Durations.seconds(5));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "test");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        /* for this POC we only have one topic */
        Collection<String> topics = Arrays.asList("TwitterStream");

        /* getting spark streaming from kafka */
        JavaInputDStream<ConsumerRecord<String, String>> lines =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );
        /* another option is receiving spark streaming from socket */
        //JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("localhost", 8181);

        /* convert stream to one dimension DStream */
        JavaDStream<String> data = lines.map(ConsumerRecord::value);
        /* start counting the total number of tweets received */
        AtomicInteger totalCount = new AtomicInteger();
        /* start RDD processing */
        data.foreachRDD(rdd -> rdd.collect().forEach(x->{
            totalCount.getAndIncrement();
            /* convert string to json object */
            JSONObject jo = new JSONObject(x);
            /* get text content */
            String content = (String) jo.get("text");
            /* check if the tweet contains music related keywords */
            for (String keyword : keywords) {
                if (!content.toLowerCase().contains(keyword)) {
                    /*check if it's duplicated, if not then save it to MongoDB */
                    ru.saveDB(x,collection);
                    break;
                }
            }
            /* print out the total number of tweets consumed at current time */
            System.out.println("Total Consumed: "+totalCount);
        }));
        /* Starting Streaming. */
        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        /* Closing Streaming context. */
        streamingContext.close();
    }
}
