package cu.uclv.mfc.enigma;

import cu.uclv.mfc.enigma.api.ApiService;
import cu.uclv.mfc.enigma.logger.Log;
import cu.uclv.mfc.enigma.observations.ObservationLoggingService;
import cu.uclv.mfc.enigma.observations.ObservationService;
import cu.uclv.mfc.enigma.sensors.SensorService;
import cu.uclv.mfc.enigma.streams.StreamService;
import cu.uclv.mfc.enigma.topics.TopicCollection;
import cu.uclv.mfc.enigma.topics.TopicService;
import io.vertx.config.ConfigRetriever;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import java.util.stream.Collectors;
import lombok.val;

public class MainVerticle extends AbstractVerticle {

  private static final TopicCollection topics = new TopicCollection();
  private Log log = new Log(MainVerticle.class);

  public static void main(String[] args) throws Exception {
    Launcher.executeCommand("run", MainVerticle.class.getName(), "-cluster");
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    val configRetriever = ConfigRetriever.create(vertx);

    configRetriever
      .getConfig()
      .compose(config -> {
        if (config.getBoolean("drop-databases-before-start")) {
          MongoClient client = MongoClient.create(
            vertx,
            new JsonObject()
              .put("connection_string", "mongodb://localhost:27017")
              .put("db_name", "traffic")
          );

          return client
            .getCollections()
            .compose(stringList ->
              CompositeFuture.all(
                stringList
                  .stream()
                  .map(client::dropCollection)
                  .collect(Collectors.toList())
              )
            )
            .compose(__ -> {
              log.info("All databases dropped.");
              return Future.succeededFuture(__);
            })
            .onFailure(fail -> {
              log.info("Fail: " + fail.getMessage());
            });
        }

        return Future.succeededFuture();
      })
      .compose(__ -> {
        return configRetriever
          .getConfig()
          .compose(config -> {
            /* Start API Service */
            if (config.getBoolean("start-api-service")) {
              vertx.deployVerticle(
                new ApiService(topics),
                new DeploymentOptions()
                  .setWorker(true)
                  .setConfig(config.getJsonObject("api-service"))
              );

              log.info("Started API Service");
            } else log.warn("Not deploying API Service.");

            return Future.succeededFuture(config);
          })
          .compose(config -> {
            /* Start Sensor Service */
            if (config.getBoolean("start-sensor-service")) {
              vertx.deployVerticle(
                new SensorService(),
                new DeploymentOptions()
                  .setWorker(true)
                  .setConfig(config.getJsonObject("sensor-service"))
              );
              log.info("Started Sensor Service");
            } else log.warn("Not deploying Sensor Service.");

            return Future.succeededFuture(config);
          })
          .compose(config -> {
            /* Start Stream Service */
            if (config.getBoolean("start-stream-service")) {
              vertx.deployVerticle(
                new StreamService(),
                new DeploymentOptions()
                  .setWorker(true)
                  .setConfig(config.getJsonObject("stream-service"))
              );
              log.info("Started Stream Service");
            } else log.warn("Not deploying Stream Service.");

            return Future.succeededFuture(config);
          })
          .compose(config -> {
            /* Start Observation Service */
            if (config.getBoolean("start-observation-service")) {
              vertx.deployVerticle(
                new ObservationService(),
                new DeploymentOptions()
                  .setWorker(true)
                  .setConfig(config.getJsonObject("observation-service"))
              );
              log.info("Started Observation Service");
            } else log.warn("Not deploying Observation Service.");

            return Future.succeededFuture(config);
          })
          .compose(config -> {
            /* Start Observation Logging Service */
            if (config.getBoolean("start-observation-logging-service")) {
              vertx.deployVerticle(
                new ObservationLoggingService(topics),
                new DeploymentOptions()
                  .setWorker(true)
                  .setConfig(
                    config.getJsonObject("observation-logging-service")
                  )
              );
              log.info("Started Observation Logging Service");
            } else log.warn("Not deploying Observation Logging Service.");

            return Future.succeededFuture(config);
          })
          .compose(config -> {
            /* Start Topics Service */
            if (config.getBoolean("start-topic-service")) {
              vertx.deployVerticle(
                new TopicService(topics),
                new DeploymentOptions()
                  .setWorker(true)
                  .setConfig(config.getJsonObject("topic-service"))
              );
              log.info("Started Topic Service");
            } else log.warn("Not deploying Topic Service.");

            return Future.succeededFuture();
          });
      });
  }
}
