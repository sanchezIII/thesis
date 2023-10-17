package broker;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.producer.RecordMetadata;
import io.vertx.kafka.client.serialization.JsonObjectSerializer;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class JsonKafkaProducer extends JsonKafkaInstance {

  private final KafkaProducer<String, JsonObject> producer;

  public JsonKafkaProducer(Vertx vertx, String host, int port) {
    super(vertx, host, port);
    Properties config = new Properties();

    config.put(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      String.format("%s:%d", host, port)
    );
    config.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      StringSerializer.class
    );
    config.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      JsonObjectSerializer.class
    );
    config.put(ProducerConfig.ACKS_CONFIG, "1");

    producer = KafkaProducer.create(vertx, config);
  }

  public Future<RecordMetadata> send(
    String topic,
    String key,
    JsonObject value,
    int partitionId
  ) {
    KafkaProducerRecord<String, JsonObject> record = KafkaProducerRecord.create(
      topic,
      key,
      value,
      partitionId
    );

    return producer.send(record);
  }
}
