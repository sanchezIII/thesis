package cu.uclv.mfc.enigma.observations.data;

import cu.uclv.mfc.enigma.observations.factory.ObservationDataFactory;
import io.vertx.core.json.JsonObject;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Observation {

  public static final DateTimeFormatter DATE_TIME_FORMATTER =
    DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm");

  String id;
  String streamId;
  ObservationData data;
  LocalDateTime resultTime;

  public Observation(
    String streamId,
    ObservationData data,
    String resultTimeString
  ) {
    this(
      streamId,
      data,
      LocalDateTime.parse(resultTimeString, DATE_TIME_FORMATTER)
    );
  }

  public Observation(
    String streamId,
    ObservationData data,
    LocalDateTime resultTime
  ) {
    this.id = UUID.randomUUID().toString();
    this.streamId = streamId;
    this.data = data;
    this.resultTime = resultTime;
  }

  public static Observation fromJson(
    JsonObject json,
    ObservationDataFactory factory
  ) {
    return new Observation(
      json.containsKey("id")
        ? json.getString("id")
        : UUID.randomUUID().toString(),
      json.getString("streamId"),
      factory.createObservationData(json.getJsonObject("data")),
      LocalDateTime.parse(json.getString("resultTime"), DATE_TIME_FORMATTER)
    );
  }

  public static JsonObject asJson(
    Observation observation,
    ObservationDataFactory factory
  ) {
    return new JsonObject()
      .put("id", observation.id)
      .put("streamId", observation.streamId)
      .put("data", observation.getData().getJsonRepresentation())
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
    return (
      id.equals(that.id) &&
      streamId.equals(that.streamId) &&
      data.equals(that.data) &&
      resultTime
        .truncatedTo(ChronoUnit.MINUTES)
        .isEqual(that.resultTime.truncatedTo(ChronoUnit.MINUTES))
    );
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, streamId, data, resultTime);
  }
}
