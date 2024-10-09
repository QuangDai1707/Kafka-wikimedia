package example;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());
    public static void main(String[] args) throws InterruptedException {
        log.info("I am callback producer");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer  = new KafkaProducer<>(properties);
        for (int j = 0; j < 3; j++) {
            for (int i = 0; i < 10; i++) {
                String topic = "third_topic";
                String key = "id_"+i;
                String value = "value_"+i+"-"+j;
                // create the producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // execute every time a record successfully sent or exception is thrown
                        if(e == null){
                            // the record was successfully sent
                            String rs =
//                                    "Receive new metadata \n" +
//                                    "Topic: "+ recordMetadata.topic()+
                                    "Key: "+ key +" | Partition: "+ recordMetadata.partition();
//                                    "\nOffset: "+ recordMetadata.offset()+
//                                    "\nTimestamp: "+ recordMetadata.timestamp();
                            log.info(rs);
                        }
                    }
                });
            }
            Thread.sleep(500);
        }

        // tell the producer to send all data and block util done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
