package com.uclv.test.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import lombok.val;

import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerService extends AbstractVerticle {

  @Override
  public void start() throws Exception {

    Map<String, String> config = new HashMap<>();
    config.put("bootstrap.servers", config().getString("bootstrap-servers"));
    config.put("key.deserializer", config().getString("key-deserializer"));
    config.put("value.deserializer", config().getString("value-deserializer"));
    config.put("group.id", config().getString("group-id"));
    config.put("auto.offset.reset", config().getString("auto-offset-reset"));
    config.put("enable.auto.commit", config().getString("enable-auto-commit"));

    KafkaConsumer<String, String> observationConsumer = KafkaConsumer.create(vertx, config);

    observationConsumer.handler(record -> {
      System.out.println("Processing key=" + record.key() + ",value=" + record.value() +
          ",partition=" + record.partition() + ",offset=" + record.offset());
    });

    val topics = config().getJsonObject("topics");

    observationConsumer.subscribe(topics.getString("observations-topic-name"));

    observationConsumer.listTopics()
        .compose(result -> {
          System.out.println("=========   TOPICS   ==========");
          for (String s : result.keySet()) {
            System.out.println(s);
          }
          System.out.println("===============================");
          return Future.succeededFuture(result);
        })
        .onFailure(err -> {
          System.out.println(err.getMessage());
        });
  }
}
