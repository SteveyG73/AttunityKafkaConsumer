package steveyg.kafka.attunity;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.Arrays;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.flipkart.zjsonpatch.JsonDiff;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

class AttunityKafkaConsumer implements Runnable {

    private final String topicName; //The topic name to listen for changes on
    private final KafkaConsumer<String, String> consumer; //Key and Value types are both string, as per the consumer properties
    final Signal signal; //The signal class that will tell us when someone has shut the app down

    /**
     *
     * @param topicName Topic name to listen for changes on
     * @param props The consumer properties to use
     * @param signal The thread shutdown signal class
     */
    AttunityKafkaConsumer(String topicName, Properties props, Signal signal) {
        this.topicName = topicName;
        this.consumer = new KafkaConsumer<>(props);
        this.signal = signal;
    }

    /**
     * Cleanly shut the consumer down
     */
    void closeConsumer() {
        consumer.close();
    }

    /**
     * Listen for changes on the specified topic, compare the before and after images of the rows
     * and create a new JSON message showing just the changes.
     *
     */
    void consumeMessages() {
        //Use Jackson to read and write JSON data
        ObjectMapper jackson = new ObjectMapper();
        System.out.println("Subscribing to topic <" + topicName + ">");
        System.out.println("Set to end of topic");
        TopicPartition part0 = new TopicPartition(topicName, 0);
        consumer.assign(Arrays.asList(part0));
        consumer.seekToEnd(Collections.singleton(part0));
        System.out.println("Waiting for next message");
        try {
            //Loop forever
            for (; ; ) {
                //Wait for new messages from Kafka
                ConsumerRecords<String, String> records = consumer.poll(100);
                //Loop through any records that are returned
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        //Read the JSON payload
                        JsonNode root = jackson.readTree(record.value());
                        JsonNode beforeNode = root.path("message").get("beforeData");
                        JsonNode afterNode = root.path("message").get("data");
                        //Create a JSON patch message by comparing the before and after images of the data
                        JsonNode patchNode = JsonDiff.asJson(beforeNode, afterNode);
                        //If differences exist, create a new message with some interesting header information
                        if (patchNode.size() > 0) {
                            ObjectNode output = jackson.createObjectNode();
                            output.put("correlation_id", root.path("message").path("headers").get("changeSequence").asText());
                            output.put("operation", root.path("message").path("headers").get("operation").asText());
                            output.put("timestamp", root.path("message").path("headers").get("timestamp").asText());
                            output.put("source_table", record.key());
                            //Look for the standard primary key column that we have configured Attunity to add for us
                            if (root.path("message").path("data").has("PK_Column")) {
                                output.put("source_pk", root.path("message").path("data").get("PK_Column").asInt());
                            }
                            else {
                                output.put("source_pk",0);
                            }
                            //Embed the differences we found in the JsonDiff operation
                            ArrayNode nested = output.putArray("changes");
                            nested.addAll((ArrayNode) patchNode);
                            //Spit the message out to the console
                            System.out.println(output.toString());
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                //Come up for air for a few milliseconds
                Thread.sleep(10);
                //If someone has shutdown the app, break out of the loop
                if (signal.isStopped()) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            System.out.println(Thread.currentThread().getName() + " interrupted");
        } finally {
            //Tidy up and go home
            closeConsumer();
            System.out.print(Thread.currentThread().getName() + " stopped");
        }

    }

    @Override
    public void run() {
        consumeMessages();
    }


}
