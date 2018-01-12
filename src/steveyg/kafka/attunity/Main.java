package steveyg.kafka.attunity;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Main {

    /**
     * Reads the consumer.properties file in the resources directory and returns a Properties object
     * @return Consumer properties object
     */

    static Signal signal;
    static Thread kafkaConsumer;

    /**
     * A shutdown hook so we close the kafka consumer cleanly on exit
     */
    static class ShutdownThreads extends Thread {

        @Override
        public void run()  {
            System.out.println("Shutdown detected");
            signal.stop();

            try {
                kafkaConsumer.join();
            } catch (InterruptedException e) {
                System.out.println("Error joining consumer thread");
            }

        }
    }

    /**
     * Load the Kafka consumer properties from the file in the resources directory
     * @return Kafka consumer properties from resources directory
     */
    private static Properties getProps() {
        Properties props = new Properties();
        InputStream input = null;

        try {
            input = ClassLoader.class.getResourceAsStream("/resources/consumer.properties");
            props.load(input);
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
        finally {
            if (input != null) {
                try {
                    input.close();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return(props);

    }

    /**
     * Start the application
     * Run the Kafka consumer in a thread simply so we have a bit more control when a user aborts the program
     * @param args  The Kafka topic name to subscribe to
     */
    public static void main(String[] args) throws InterruptedException {
        //Load the kafka consumer properties
        Properties props = getProps();
        //Initialise the thread signal class
        signal = new Signal();

        //If we have a single parameter, assume it's the topic name
        if (args.length==1) {
            //Register the shutdown hook
            Runtime.getRuntime().addShutdownHook(new ShutdownThreads());
            //Create a new thread for the consumer
            kafkaConsumer = new Thread(new AttunityKafkaConsumer(args[0],props, signal), "AttunityKafkaConsumer");
            //Start the thread up
            kafkaConsumer.start();
            //Wait for the thread to finish
            kafkaConsumer.join();
        }
        else {
            System.out.println("Invalid parameters");
            System.exit(1);
        }


    }
}
