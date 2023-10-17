package org.traffic.traffic_producer.observations;

import io.vertx.core.json.JsonObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.UUID;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Observation {

  public static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm");

  String id;
  String streamId;
  Number result;
  LocalDateTime resultTime;

  public Observation(String streamId, Number result, String resultTimeString) {
    this(streamId, result, LocalDateTime.parse(resultTimeString, DATE_TIME_FORMATTER));
  }

  public Observation(String streamId, Number result, LocalDateTime resultTime) {
    this.id = UUID.randomUUID().toString();
    this.streamId = streamId;
    this.result = result;
    this.resultTime = resultTime;
  }

  public static Observation fromJson(JsonObject json) {
    return new Observation(
        json.containsKey("id") ? json.getString("id") : UUID.randomUUID().toString(),
        json.getString("streamId"),
        json.getNumber("result"),
        LocalDateTime.parse(json.getString("resultTime"), DATE_TIME_FORMATTER));
  }

  public static JsonObject asJson(Observation observation) {
    return new JsonObject()
        .put("id", observation.id)
        .put("streamId", observation.streamId)
        .put("result", observation.result)
        .put("resultTime", observation.resultTime.format(DATE_TIME_FORMATTER));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Observation that = (Observation) o;
    return id.equals(that.id)
        && streamId.equals(that.streamId)
        && result.equals(that.result)
        && resultTime
            .truncatedTo(ChronoUnit.MINUTES)
            .isEqual(that.resultTime.truncatedTo(ChronoUnit.MINUTES));
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, streamId, result, resultTime);
  }
}
