package kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.StreamException;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer  = new KafkaProducer<>(properties);
        // create the producer record
        String topic = "wikimedia.recentchange";

        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder eventSourceBuilder = new EventSource.Builder(URI.create(url));

        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(eventHandler,eventSourceBuilder);

        BackgroundEventSource eventSource = builder.build();

        //start the producer in another thread
        eventSource.start();

        // product for 10m and blocks the program until then
        TimeUnit.MINUTES.sleep(10);
//        // send data
//        producer.send(producerRecord);
//
//        // tell the producer to send all data and block util done -- synchronous
//        producer.flush();
//
//        // flush and close the producer
//        producer.close();
    }
}
