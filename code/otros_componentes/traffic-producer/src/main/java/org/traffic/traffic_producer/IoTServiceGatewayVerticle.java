package org.traffic.traffic_producer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.types.HttpEndpoint;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.traffic.traffic_producer.observations.Observation;
import org.traffic.traffic_producer.sensors.Sensor;
import org.traffic.traffic_producer.streams.Stream;
import org.traffic.traffic_producer.util.JsonCollector;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static org.traffic.traffic_producer.observations.ObservationService.OBSERVATION_SERVICE_ADDRESS;
import static org.traffic.traffic_producer.sensors.SensorService.SENSOR_SERVICE_ADDRESS;
import static org.traffic.traffic_producer.streams.StreamService.STREAM_SERVICE_ADDRESS;

@Slf4j
public final class IoTServiceGatewayVerticle extends AbstractVerticle {

  private WebClient webClient;

  private HttpRequest<JsonObject> observationFetchRequest;

  private ServiceDiscovery serviceDiscovery;

  @Override
  public void start(Promise<Void> startPromise) {

    // Setting service configuration

    val useSsl = config().getBoolean("use-ssl");

    val dataSet = config().getJsonObject("dataset");

    webClient = WebClient.create(vertx);
    serviceDiscovery = ServiceDiscovery.create(vertx);

    val endpoint = dataSet.getString("endpoint");
    log.info("Using endpoint: [{}] to fetch sensors and observations ", endpoint);

    val observationRequestParam = dataSet.getString("observation-request-param");
    val outputFormatParam = dataSet.getString("outputFormat");
    val intervalParam = dataSet.getInteger("interval");
    val includeLanesParam = config().getBoolean("includeLanes");
    val singleValueParam = config().getBoolean("singleValue");
    val frequencyMillis = config().getInteger("frequency");

    val registrationEnabled = config().getBoolean("registration-enabled");

    log.info("Registration enabled: {}", registrationEnabled);

    observationFetchRequest =
      webClient
        .getAbs(endpoint)
        .putHeader("Accept", "application/json")
        .addQueryParam("request", observationRequestParam)
        .addQueryParam("outputFormat", outputFormatParam)
        .addQueryParam("interval", valueOf(intervalParam))
        .addQueryParam("includeLanes", valueOf(includeLanesParam))
        .addQueryParam("singleValue", valueOf(singleValueParam))
        .as(BodyCodec.jsonObject())
        .expect(ResponsePredicate.SC_OK);

    val sensorRequestParam = dataSet.getString("sensor-request-param");

    HttpRequest<JsonObject> sensorFetchRequest =
      webClient
        .getAbs(endpoint)
        //            .ssl(useSsl)
        .putHeader("Accept", "application/json")
        .addQueryParam("request", sensorRequestParam)
        .as(BodyCodec.jsonObject())
        .expect(ResponsePredicate.SC_OK);

    sensorFetchRequest
      .send()
      .map(HttpResponse::body)
      .map(this::extractSensors)
      .compose(this::saveSensors)

      .compose(
        sensorSet ->
          registrationEnabled
            ? this.registerSensors(sensorSet)
            : Future.succeededFuture(sensorSet))

      .compose(
        sensorSet ->
          registrationEnabled
            ? this.closeExistingStreams(sensorSet)
            : Future.succeededFuture(sensorSet))

      .compose(this::saveStreams)
      .compose(
        streamSet ->
          registrationEnabled
            ? this.registerStreams(streamSet)
            : Future.succeededFuture(streamSet))
      .onSuccess(
        __ -> {
          log.info("Auto-fetch: [{}]", config().getBoolean("auto-fetch-observations"));
          if (config().getBoolean("auto-fetch-observations")) {
            vertx.setPeriodic(
              frequencyMillis,
              timerId ->
                fetchObservations(intervalParam)
                  .compose(this::publishObservations)
                  .onSuccess(
                    result -> log.info("Fetching observations periodic task completed"))
                  .onFailure(
                    throwable -> {
                      log.error("Fetching observations periodic task failed");
                      throwable.printStackTrace();
                    }));
            log.info(
              "Device data has been fetched. Real time data update interval is set to: {} seconds",
              intervalParam);
          }
          startPromise.complete();
        })
      .onFailure(
        throwable -> {
          log.error("Unable to fetch devices data. Cause: {}", throwable.getMessage());
          throwable.printStackTrace();
          startPromise.fail(throwable);
        });
  }

  @Override
  public void stop() {
    webClient.close();
    serviceDiscovery.close();
  }

  private Set<Sensor> extractSensors(JsonObject payload) {

    val features = payload.getJsonArray("features");

    log.info("Found [{}] feature(s)", features.size());
    return features.stream()
      .map(__ -> (JsonObject) __)
      .map(
        feature -> {
          val coordinates = feature.getJsonObject("geometry").getJsonArray("coordinates");
          val latitude = coordinates.getDouble(0);
          val longitude = coordinates.getDouble(1);
          val id = feature.getJsonObject("properties").getString("traverse_name");

          return List.of(
            new Sensor(id + "_SPEED", latitude, longitude, "speed", "km"),
            new Sensor(id + "_COUNT", latitude, longitude, "count", "unit"));
        })
      .flatMap(Collection::stream)
      .collect(Collectors.toSet());
  }

  private Future<Set<Sensor>> saveSensors(Set<Sensor> sensors) {

    val payload =
      new JsonObject()
        .put("list", sensors.stream().map(Sensor::asJson).collect(JsonCollector.toJsonArray()));

    return vertx
      .eventBus()
      .<JsonObject>request(SENSOR_SERVICE_ADDRESS, payload, action("saveAll"))
      .map(reply -> sensors)
      .onSuccess(__ -> log.info("Saved: [{}] sensor(s)", sensors.size()));
  }

  private Future<Set<Sensor>> registerSensors(Set<Sensor> sensors) {
    return HttpEndpoint.getWebClient(
        serviceDiscovery, new JsonObject().put("name", "traffic-registry"))
      .compose(
        webClient -> {
          @SuppressWarnings("rawtypes")
          List<Future> requests =
            sensors.stream()
              .map(
                sensor ->
                  webClient
                    .post("/api/v1/sensors")
                    .putHeader("Content-Type", "application/json")
                    .expect(ResponsePredicate.SC_CREATED)
                    .sendBuffer(Sensor.asJson(sensor).toBuffer()))
              .collect(Collectors.toList());
          return CompositeFuture.all(requests);
        })
      .map(result -> sensors)
      .onSuccess(__ -> log.info("Registered: [{}] sensor(s)", sensors.size()));
  }

  private Future<Set<Sensor>> closeExistingStreams(Set<Sensor> sensors) {

    val payload =
      new JsonObject()
        .put("list", sensors.stream().map(Sensor::getId).collect(JsonCollector.toJsonArray()));

    return vertx
      .eventBus()
      .<JsonObject>request(STREAM_SERVICE_ADDRESS, payload, action("deleteAllBy"))
      .map(Message::body)
      .map(body -> body.getJsonArray("list"))
      .map(
        array ->
          array.stream()
            .map(__ -> (JsonObject) __)
            .map(Stream::fromJson)
            .collect(Collectors.toList()))
      .compose(
        streams ->
          HttpEndpoint.getWebClient(
              serviceDiscovery, new JsonObject().put("name", "traffic-registry"))
            .compose(
              webClient -> {
                @SuppressWarnings("rawtypes")
                List<Future> requests =
                  streams.stream()
                    .map(
                      stream -> {
                        log.debug(
                          "Patching stream: [{}]",
                          Stream.asJson(stream).encodePrettily());
                        return webClient
                          .patch(format("/api/v1/streams/%s", stream.getId()))
                          .expect(ResponsePredicate.SC_NO_CONTENT)
                          .send();
                      })
                    .collect(Collectors.toList());
                return CompositeFuture.all(requests);
              })
            .map(responses -> sensors)
            .onSuccess(__ -> log.info("Closed: {} stream(s)", streams.size()))
            .onFailure(throwable -> log.error("Failed closing streams")));
  }

  private Future<Set<Stream>> saveStreams(Set<Sensor> sensors) {

    val streams = sensors.stream().map(Stream::new).collect(Collectors.toSet());

    val payload =
      new JsonObject()
        .put("list", streams.stream().map(Stream::asJson).collect(JsonCollector.toJsonArray()));
    return vertx
      .eventBus()
      .<JsonObject>request(STREAM_SERVICE_ADDRESS, payload, action("saveAll"))
      .map(reply -> streams)
      .onSuccess(__ -> log.info("Saved [{}] stream(s)", streams.size()));
  }

  private Future<Set<Stream>> registerStreams(Set<Stream> streams) {

    return HttpEndpoint.getWebClient(
        serviceDiscovery, new JsonObject().put("name", "traffic-registry"))
      .compose(
        webClient -> {
          @SuppressWarnings("rawtypes")
          List<Future> requests =
            streams.stream()
              .map(
                stream ->
                  webClient
                    .post("/api/v1/streams")
                    .putHeader("Content-Type", "application/json")
                    .expect(ResponsePredicate.SC_CREATED)
                    .sendBuffer(Stream.asJson(stream).toBuffer()))
              .collect(Collectors.toList());
          return CompositeFuture.all(requests);
        })
      .map(responses -> streams)
      .onSuccess(__ -> log.info("Registered: {} stream(s)", streams.size()))
      .onFailure(throwable -> log.error("Failed registering streams"));
  }

  private Future<List<Observation>> fetchObservations(Integer interval) {

    log.info("Periodic task. Fetching observations...");

    return observationFetchRequest
      .send()
      .map(HttpResponse::body)
//      .compose(data -> {
//        log.info(data.toString());
//        return Future.succeededFuture(data);
//      })
      .flatMap(data -> parseObservations(data, interval))
      .onSuccess(observations -> log.info("Fetched [{}] observation(s)", observations.size()));
  }

  private Future<List<Observation>> parseObservations(JsonObject body, int interval) {
    val data = body.getJsonObject("data");
    val sensorIds = data.fieldNames();

    return (Future<List<Observation>>) vertx
      .eventBus()
      .<JsonObject>request(STREAM_SERVICE_ADDRESS, new JsonObject(), action("findAll"))
      .map(Message::body)
      .map(messageBody -> messageBody.getJsonArray("list"))
      .map(
        array ->
          array.stream()
            .map(__ -> ((JsonObject) __))
            .map(Stream::fromJson)
            .collect(Collectors.toList()))
      .map(
        list ->
          list.stream()
            .collect(
              Collectors.groupingBy(
                stream ->
                  ImmutablePair.of(stream.getGeneratedBy(), stream.getFeature()))))
      .map(
        sensorIdAndFeatureToStream ->
          sensorIds.stream()
            // Ignore unavailable sensors
            .filter(
              sensorId -> {
                val sensorRead =
                  data.getJsonObject(sensorId)
                    .getJsonObject("results")
                    .getJsonObject(format("%dm", interval))
                    .getJsonObject("t1");
//                ver lo de t1 y t2
                return sensorRead.getValue("count") != null
                  && sensorRead.getValue("speed") != null;
//                return sensorRead.getJsonObject("t1").getValue("count") != null
//                  && sensorRead.getJsonObject("t1").getValue("speed") != null;
              })
            .map(
              sensorId -> {
                val timeKey = format("%dm", interval);
                val countStreamId =
                  sensorIdAndFeatureToStream
                    .get(ImmutablePair.of(format("%s_COUNT", sensorId), "count"))
                    .get(0)
                    .getId();
                val speedStreamId =
                  sensorIdAndFeatureToStream
                    .get(ImmutablePair.of(format("%s_SPEED", sensorId), "speed"))
                    .get(0)
                    .getId();
                val sensorRead =
                  data.getJsonObject(sensorId)
                    .getJsonObject("results")
                    .getJsonObject(timeKey)
                    .getJsonObject("t1");;

                val resultTime = sensorRead.getString("end_time");
                val countResult = sensorRead.getInteger("count");
                val speedResult = sensorRead.getDouble("speed");

                val countObservation =
                  new Observation(countStreamId, countResult, resultTime);
                val speedObservation =
                  new Observation(speedStreamId, speedResult, resultTime);

                return List.of(countObservation, speedObservation);
              })
            .flatMap(Collection::stream)
            .collect(Collectors.toList()));
  }

  private Future<Message<JsonObject>> publishObservations(List<Observation> observations) {
    val payload =
      observations.stream().map(Observation::asJson).collect(JsonCollector.toJsonArray());
    return vertx
      .eventBus()
      .<JsonObject>request(
        OBSERVATION_SERVICE_ADDRESS,
        new JsonObject().put("list", payload),
        action("publishAll"))
      .onFailure(
        throwable -> {
          log.error("Unable to publish observations. Cause: {}", throwable.getMessage());
          throwable.printStackTrace();
        });
  }

  private DeliveryOptions action(String saveAll) {
    return new DeliveryOptions().addHeader("action", saveAll);
  }
}
