package cu.uclv.mfc.enigma.streams;

import cu.uclv.mfc.enigma.point.Point;
import cu.uclv.mfc.enigma.sensors.Sensor;
import io.vertx.core.json.JsonObject;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Stream {

  /**
   *  Objeto nativo de Java que permite
   */
  public static final DateTimeFormatter DATE_TIME_FORMATTER =
    DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm");

  private String id;
  private LocalDateTime streamStart;

  /**
   * Id del sensor que genero el stream
   */
  private String generatedBy;
  private String feature;
  private Point location;

  public Stream(Sensor sensor) {
    this.feature = sensor.getQuantityKind();
    this.id = UUID.randomUUID() + "-" + feature;
    this.streamStart = LocalDateTime.now();
    this.generatedBy = sensor.getId();
    this.location = new Point(sensor.getLatitude(), sensor.getLongitude());
  }

  public static Stream fromJson(JsonObject json) {
    return new Stream(
      json.getString("id"),
      LocalDateTime.parse(json.getString("streamStart"), DATE_TIME_FORMATTER),
      json.getString("generatedBy"),
      json.getString("feature"),
      Point.fromJson(json.getJsonObject("location"))
    );
  }

  public static JsonObject asJson(Stream stream) {
    return new JsonObject()
      .put("id", stream.id)
      .put("location", Point.asJson(stream.location))
      .put("streamStart", stream.streamStart.format(DATE_TIME_FORMATTER))
      .put("generatedBy", stream.generatedBy)
      .put("feature", stream.feature);
  }

  /**
   *
   * @param o
   * @return
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Stream stream = (Stream) o;
    return (
      id.equals(stream.id) &&
      streamStart
        .truncatedTo(ChronoUnit.MINUTES)
        .isEqual(stream.streamStart.truncatedTo(ChronoUnit.MINUTES)) &&
      generatedBy.equals(stream.generatedBy) &&
      feature.equals(stream.feature) &&
      location.equals(stream.location)
    );
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, streamStart, generatedBy, feature, location);
  }
}
