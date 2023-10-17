package org.traffic.traffic_producer;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.BulkOperation;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisAPI;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.ResponseType;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.types.HttpEndpoint;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.traffic.traffic_producer.sensors.Sensor;
import org.traffic.traffic_producer.sensors.SensorMongoRepository;
import org.traffic.traffic_producer.streams.Stream;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.traffic.traffic_producer.streams.StreamMongoRepository.COLLECTION_NAME;

@ExtendWith(VertxExtension.class)
@Slf4j
class MainVerticleTest {

  private static JsonObject defaultConfig;

  private static JsonObject registryConfig;

  private static Set<Sensor> sampleSensors;

  private static JsonObject iotServiceConfig;

  private static JsonObject sampleSensorsResponseBody;

  private static JsonObject sampleObservationResponseBody;

  private static List<Stream> streams;

  private static MongoClient mongoClient;

  @BeforeAll
  static void configure(VertxTestContext testContext) {

    Vertx vertx = Vertx.vertx();

    defaultConfig = new JsonObject(vertx.fileSystem().readFileBlocking("conf/config.json"));

    sampleSensorsResponseBody =
        new JsonObject(vertx.fileSystem().readFileBlocking("sample-sensors-response.json"));

    // Twice the number of features in 'sampleSensorResponseBody' since each feature
    // produces two datasets
    sampleSensors = parseSensors(sampleSensorsResponseBody);

    sampleObservationResponseBody =
        new JsonObject(vertx.fileSystem().readFileBlocking("sample-observations-response.json"));

    iotServiceConfig =
        new JsonObject(vertx.fileSystem().readFileBlocking("iot-server-config.json"));

    registryConfig = new JsonObject(vertx.fileSystem().readFileBlocking("registry-config.json"));

    val manyStreams =
        new JsonObject(vertx.fileSystem().readFileBlocking("streams-matching-sensor-ids.json"))
            .getJsonArray("list");

    streams =
        manyStreams.stream()
            .map(__ -> ((JsonObject) __))
            .map(Stream::fromJson)
            .collect(Collectors.toList());

    mongoClient =
        MongoClient.create(
            vertx,
            new JsonObject()
                .put("connection_string", "mongodb://localhost:27017")
                .put("db_name", "traffic"));

    vertx.close().onComplete(testContext.succeedingThenComplete());
  }

  @BeforeEach
  void cleanBefore(Vertx vertx, VertxTestContext testContext) throws Throwable {

    mongoClient =
        MongoClient.create(
            vertx,
            new JsonObject()
                .put("connection_string", "mongodb://localhost:27017")
                .put("db_name", "traffic"));
    val dropSensorCollectionFuture =
        mongoClient.dropCollection(SensorMongoRepository.COLLECTION_NAME);
    val dropStreamCollectionFuture = mongoClient.dropCollection(COLLECTION_NAME);
    CompositeFuture.all(dropSensorCollectionFuture, dropStreamCollectionFuture)
        .onComplete(testContext.succeedingThenComplete());

    if (testContext.failed()) throw testContext.causeOfFailure();
  }

  @AfterEach
  void cleanAfter(VertxTestContext testContext) throws Throwable {
    val dropSensorCollectionFuture =
        mongoClient.dropCollection(SensorMongoRepository.COLLECTION_NAME);
    val dropStreamCollectionFuture = mongoClient.dropCollection(COLLECTION_NAME);

    CompositeFuture.all(dropSensorCollectionFuture, dropStreamCollectionFuture)
        .onComplete(testContext.succeedingThenComplete());

    if (testContext.failed()) throw testContext.causeOfFailure();
  }

  @Test
  @DisplayName("When main verticle is deployed, sensors and streams must be registered")
  void registryMustReceivedSensorsAndStreams(Vertx vertx, VertxTestContext testContext)
      throws InterruptedException {

    System.setProperty("vertx-config-path", "conf/config.json");

    // Setting checkpoints
    val sensorsRequestReceived = testContext.checkpoint();
    val registryConfigured = testContext.checkpoint();
    val iotServerConfigured = testContext.checkpoint();
    val sensorsRegistrationReceived = testContext.checkpoint(sampleSensors.size());
    val streamsRegistrationReceived = testContext.checkpoint(sampleSensors.size());
    val mainVerticleDeployed = testContext.checkpoint();

    // Setting up iot server
    val iotRouterConfig = Router.router(vertx);
    iotRouterConfig
        .get(iotServiceConfig.getString("url"))
        .produces("application/json")
        .handler(
            routingContext -> {
              val response = routingContext.response();
              response.setStatusCode(200);
              if (routingContext.request().getParam("request").equals("devices")) {
                response.end(sampleSensorsResponseBody.toBuffer());
                sensorsRequestReceived.flag();
              } else {
                response.end(sampleObservationResponseBody.toBuffer());
              }
            });

    val iotServerFuture =
        vertx
            .createHttpServer()
            .requestHandler(iotRouterConfig)
            .listen(iotServiceConfig.getInteger("port"))
            .onComplete(
                testContext.succeeding(
                    server -> {
                      log.info("IoT server has been created!");
                      iotServerConfigured.flag();
                    }));

    // Setting up iot registry
    // Note: Since each sensor produces to datasets, two sensors needs to be registered
    //  for each dataset
    val registryRouter = Router.router(vertx);
    registryRouter
        .post("/api/v1/sensors")
        .consumes("application/json")
        .handler(BodyHandler.create())
        .handler(
            routingContext -> {
              val sensor = routingContext.getBodyAsJson();
              val response = routingContext.response();
              if (sampleSensors.contains(Sensor.fromJson(sensor))) {
                log.debug("Sensor registered correctly: {}", sensor);
                response.setStatusCode(201);
                response.end(sensor.toBuffer());
                sensorsRegistrationReceived.flag();
              } else {
                log.debug("Sensor registration failed: {}", sensor);
                response.setStatusCode(400);
                response.end();
              }
            });
    registryRouter
        .post("/api/v1/streams")
        .consumes("application/json")
        .handler(BodyHandler.create())
        .handler(
            routingContext -> {
              routingContext.response().setStatusCode(201).end();
              streamsRegistrationReceived.flag();
            });
    registryRouter
        .patch("/api/v1/streams")
        .handler(request -> request.response().setStatusCode(204).end());

    val serviceDiscovery = new ServiceDiscovery[1];
    val registryServerFuture =
        vertx
            .createHttpServer()
            .requestHandler(registryRouter)
            .listen(registryConfig.getInteger("port"))
            .compose(
                httpServer -> {
                  log.info("Registry server created!");
                  serviceDiscovery[0] = ServiceDiscovery.create(vertx);
                  val registryServerRecord =
                      HttpEndpoint.createRecord(
                          "traffic-registry",
                          registryConfig.getString("host"),
                          registryConfig.getInteger("port"),
                          "/api");
                  return serviceDiscovery[0].publish(registryServerRecord);
                })
            .onComplete(testContext.succeeding(__ -> registryConfigured.flag()));

    CompositeFuture.all(iotServerFuture, registryServerFuture)
        .compose(__ -> vertx.deployVerticle(new MainVerticle()))
        .onComplete(testContext.succeeding(id -> mainVerticleDeployed.flag()));

    Assertions.assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
  }

  @Test
  @DisplayName("When main verticle is deployed observations are published to the broker two times")
  void observationsArePublish(Vertx vertx, VertxTestContext testContext)
      throws InterruptedException {
    System.setProperty("vertx-config-path", "conf/config-without-registration.json");

    val iotServerConfigured = testContext.checkpoint();
    val mainVerticleDeployed = testContext.checkpoint();
    val observationsReceivedTwoTimes =
        testContext.checkpoint(2 * countObservationsFromOnlineSensors());
    val subscriberReady = testContext.checkpoint();

    // Setting up iot server
    val iotRouterConfig = Router.router(vertx);
    iotRouterConfig
        .get(iotServiceConfig.getString("url"))
        .produces("application/json")
        .handler(
            routingContext ->
                routingContext
                    .response()
                    .setStatusCode(200)
                    .end(
                        routingContext.request().getParam("request").equals("devices")
                            ? sampleSensorsResponseBody.toBuffer()
                            : sampleObservationResponseBody.toBuffer()));

    val iotServerFuture =
        vertx
            .createHttpServer()
            .requestHandler(iotRouterConfig)
            .listen(iotServiceConfig.getInteger("port"))
            .onComplete(
                testContext.succeeding(
                    server -> {
                      log.info("IoT server has been created!");
                      iotServerConfigured.flag();
                    }));

    val uri = defaultConfig.getJsonObject("observation-service").getString("uri");
    val redisClient = Redis.createClient(vertx, new RedisOptions().setConnectionString(uri));
    val subscriberReadyFuture =
        redisClient
            .connect()
            .compose(
                connection -> {
                  connection.handler(
                      message -> {
                        if (message.type().equals(ResponseType.PUSH)
                            && message.get(0).toString().equals("pmessage")) {
                          observationsReceivedTwoTimes.flag();
                        }
                      });
                  return RedisAPI.api(connection).psubscribe(List.of("*"));
                })
            .onComplete(testContext.succeeding(__ -> subscriberReady.flag()));

    CompositeFuture.all(iotServerFuture, subscriberReadyFuture)
        .compose(__ -> vertx.deployVerticle(new MainVerticle()))
        .onComplete(testContext.succeeding(id -> mainVerticleDeployed.flag()));

    Assertions.assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
  }

  @Test
  @DisplayName(
      "When main verticle is deployed and the database contains streams from previous runs,"
          + " then for each streams a request is made to the public repository mark them as"
          + " closed  and the same streams gets removed from the local database")
  void closeStreamsWhenStartingApplication(Vertx vertx, VertxTestContext testContext)
      throws InterruptedException {

    System.setProperty("vertx-config-path", "conf/config-without-autofetch.json");

    val registryConfigured = testContext.checkpoint();
    val iotServerConfigured = testContext.checkpoint();
    val populateDatabase = testContext.checkpoint();
    val mainVerticleDeployed = testContext.checkpoint();
    val closeAllExistingStreams = testContext.checkpoint(streams.size());
    log.debug("Stream inserted in the test: ");
    streams.forEach(stream -> log.debug("{}", stream.toString()));

    // Setting up iot server
    val iotRouterConfig = Router.router(vertx);
    iotRouterConfig
        .get(iotServiceConfig.getString("url"))
        .produces("application/json")
        .handler(
            routingContext -> {
              val response = routingContext.response();
              response.setStatusCode(200);
              if (routingContext.request().getParam("request").equals("devices")) {
                response.end(sampleSensorsResponseBody.toBuffer());
              } else {
                response.end(sampleObservationResponseBody.toBuffer());
              }
            });

    val iotServerFuture =
        vertx
            .createHttpServer()
            .requestHandler(iotRouterConfig)
            .listen(iotServiceConfig.getInteger("port"))
            .onComplete(
                testContext.succeeding(
                    server -> {
                      log.info("IoT server has been created!");
                      iotServerConfigured.flag();
                    }));

    // Setting up iot registry
    // Note: Since each sensor produces two datasets, two sensors needs to be registered
    //  for each dataset
    val registryRouter = Router.router(vertx);
    registryRouter
        .post("/api/v1/*")
        .consumes("application/json")
        .produces("text/turtle")
        .handler(BodyHandler.create())
        .handler(
            routingContext ->
                routingContext
                    .response()
                    .setStatusCode(201)
                    .end(routingContext.getBodyAsJson().toBuffer()));

    registryRouter
        .patch("/api/v1/streams/:streamId")
        .handler(
            routingContext -> {
              log.debug("Patched: {}", routingContext.pathParam("streamId"));
              closeAllExistingStreams.flag();
              routingContext.response().setStatusCode(204).end();
            });

    val serviceDiscovery = new ServiceDiscovery[1];
    val registryServerFuture =
        vertx
            .createHttpServer()
            .requestHandler(registryRouter)
            .listen(registryConfig.getInteger("port"))
            .compose(
                httpServer -> {
                  log.info("Registry server created!");
                  serviceDiscovery[0] = ServiceDiscovery.create(vertx);
                  val registryServerRecord =
                      HttpEndpoint.createRecord(
                          "traffic-registry",
                          registryConfig.getString("host"),
                          registryConfig.getInteger("port"),
                          "/api");
                  return serviceDiscovery[0].publish(registryServerRecord);
                })
            .onComplete(testContext.succeeding(__ -> registryConfigured.flag()));

    val populateDatabaseFuture =
        mongoClient
            .dropCollection(COLLECTION_NAME)
            .compose(
                __ ->
                    mongoClient.bulkWrite(
                        COLLECTION_NAME,
                        streams.stream()
                            .map(Stream::asJson)
                            .map(BulkOperation::createInsert)
                            .collect(Collectors.toList())))
            .onComplete(
                testContext.succeeding(
                    __ -> {
                      log.info("Database populated");
                      populateDatabase.flag();
                    }));

    CompositeFuture.all(iotServerFuture, registryServerFuture, populateDatabaseFuture)
        .compose(__ -> vertx.deployVerticle(new MainVerticle()))
        .onComplete(testContext.succeeding(id -> testContext.verify(mainVerticleDeployed::flag)));

    Assertions.assertTrue(testContext.awaitCompletion(5, TimeUnit.SECONDS));
  }

  private static Set<Sensor> parseSensors(JsonObject sensorsRaw) {
    val sensors = new HashSet<Sensor>();
    sensorsRaw.getJsonArray("features").stream()
        .map(__ -> ((JsonObject) __))
        .forEach(
            raw -> {
              val latitude = raw.getJsonObject("geometry").getJsonArray("coordinates").getDouble(0);
              val longitude =
                  raw.getJsonObject("geometry").getJsonArray("coordinates").getDouble(1);
              val id = raw.getJsonObject("properties").getString("traverse_name");
              sensors.add(new Sensor(id + "_SPEED", latitude, longitude, "speed", "km"));
              sensors.add(new Sensor(id + "_COUNT", latitude, longitude, "count", "unit"));
            });
    return sensors;
  }

  private static int countObservationsFromOnlineSensors() {
    val data = sampleObservationResponseBody.getJsonObject("data");
    return 2
        * ((int)
            data.fieldNames().stream()
                .filter(
                    sensorId -> {
                      val read =
                          data.getJsonObject(sensorId).getJsonObject("results").getJsonObject("5m");
                      return read.getValue("count") != null && read.getValue("speed") != null;
                    })
                .count());
  }
}
