package example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Im Kafka consumer");
        String groupId = "my-first-application";
        String topic = "third_topic";
        // create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // create consumer config
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset","earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // add the shutdown hook

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup().....");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {

                }
            }
        });

       try{
           // subscribe to a topic
           consumer.subscribe(Arrays.asList(topic));

           // poll for data
           while (true){
               ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

               for (ConsumerRecord<String, String> record: records){
                   log.info("Key:"+ record.key() + ", value: "+ record.value()+
                           ", Partition: "+ record.partition()+ ", Offset: "+ record.offset()
                   );
               }
           }
       } catch (WakeupException e) {
           log.info("Consumer is starting to shut down");
       } catch (Exception e) {
           log.info("Unexpected exception: " + e);
       } finally {
            consumer.close(); //close consumer, this also close the commit offset
            log.info("The consumer is shut down!!!");
       }
    }
}
