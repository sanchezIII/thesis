package org.traffic.traffic_producer.observations;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import static org.traffic.traffic_producer.observations.Observation.DATE_TIME_FORMATTER;

@Slf4j
@ExtendWith(VertxExtension.class)
class ObservationTest {

  private static JsonObject sampleObservation;

  @BeforeAll
  static void configure(Vertx vertx) {
    sampleObservation =
        new JsonObject(vertx.fileSystem().readFileBlocking("sample-raw-observation.json"));
  }

  @Test
  @DisplayName("When serializing Observation class, serial versions match")
  void fromJsonTest() {
    val givenObservation =
        new Observation(
            sampleObservation.getString("id"),
            sampleObservation.getString("streamId"),
            sampleObservation.getNumber("result"),
            LocalDateTime.parse(sampleObservation.getString("resultTime"), DATE_TIME_FORMATTER));

    Assertions.assertEquals(sampleObservation, Observation.asJson(givenObservation));
  }

  @Test
  @DisplayName("When de-serializing an Observation, instances match")
  void asJsonTest() {
    val now = LocalDateTime.now();
    val givenObservation =
        new JsonObject()
            .put("id", sampleObservation.getString("id"))
            .put("streamId", sampleObservation.getString("streamId"))
            .put("result", sampleObservation.getNumber("result"))
            .put("resultTime", now.truncatedTo(ChronoUnit.MINUTES).format(DATE_TIME_FORMATTER));

    val expected =
        new Observation(
            sampleObservation.getString("id"),
            sampleObservation.getString("streamId"),
            sampleObservation.getNumber("result"),
            now);

    Assertions.assertEquals(expected, Observation.fromJson(givenObservation));
  }
}
