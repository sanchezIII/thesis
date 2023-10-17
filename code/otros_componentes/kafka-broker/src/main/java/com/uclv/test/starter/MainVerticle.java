package com.uclv.test.starter;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import io.vertx.config.ConfigRetriever;
import java.util.HashMap;
import java.util.Map;

//@Slf4j
public class MainVerticle extends AbstractVerticle {

  private JsonObject config;

  public static void main(String[] args) {
    Launcher.executeCommand("run", MainVerticle.class.getName(), "-cluster");
  }


  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    val configRetriever = ConfigRetriever.create(vertx);

    configRetriever.getConfig()
        .map(config -> {
//          log.info("Configuration loaded");
          return this.config = config;
        })
        .compose(config -> {
          if (config.getBoolean("start-kafka-consumer-service")) {
            vertx.deployVerticle(
                new KafkaConsumerService(),
                new DeploymentOptions()
                    .setWorker(true)
                    .setConfig(config.getJsonObject("kafka-consumer-service")));
          }
          return Future.succeededFuture(config);
        })
        .onSuccess(__ -> {
//          log.info("Started Kafka Consumer Service");
          System.out.println("Started Kafka Consumer Service");
        })
        .onFailure(fail -> {
          System.out.println("Failure in deploying kafka consumer. Cause: " + fail.getMessage());
        });


  }
}
