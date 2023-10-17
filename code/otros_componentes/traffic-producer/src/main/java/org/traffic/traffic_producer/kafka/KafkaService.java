package org.traffic.traffic_producer.kafka;

import io.vertx.core.*;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.regex.Pattern;

@Slf4j
public class KafkaService extends AbstractVerticle {
  private Map<String, String> config;
  private KafkaConsumer<String, String> consumer;

  @Override
  public void start() throws Exception {

//    consumer.handler(record -> {
//      log.info("Processing key=" + record.key() + ",value=" + record.value() +
//        ",partition=" + record.partition() + ",offset=" + record.offset());
//    });

//// subscribe to several topics with list
//    Set<String> topics = new HashSet<>();
//    topics.add("topic1");
//    topics.add("topic2");
//    topics.add("topic3");
//    consumer.subscribe(topics);
//
//// or using a Java regex
//    Pattern pattern = Pattern.compile("topic\\d");
//    consumer.subscribe(pattern);
//
//// or just subscribe to a single topic

//    consumer
//      .subscribe("a-single-topic")
//      .onSuccess(
//        __ -> log.info("Subscribed to channel {}", "a-single-topic")
//      );

  }
}
