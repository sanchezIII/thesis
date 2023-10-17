package org.traffic.traffic_registry.stream;

import io.vertx.core.json.JsonObject;
import lombok.Value;
import lombok.val;
import org.traffic.traffic_registry.point.Point;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Value
public class Stream {

  String id;
  String derivedFrom;
  String generatedBy;
  Point location;
  LocalDateTime streamStart;
  LocalDateTime streamEnd;

  public static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm");

  public static Stream fromJson(JsonObject json) {
    val streamStart = LocalDateTime.parse(json.getString("streamStart"), DATE_TIME_FORMATTER);
    LocalDateTime streamEnd =
        json.containsKey("streamEnd")
            ? LocalDateTime.parse(json.getString("streamEnd"), DATE_TIME_FORMATTER)
            : null;
    return new Stream(
        json.getString("id"),
        json.getString("derivedFrom"),
        json.getString("generatedBy"),
        Point.fromJson(json.getJsonObject("location")),
        streamStart,
        streamEnd);
  }

  public static JsonObject asJson(Stream stream) {
    val json = new JsonObject();
    json.put("id", stream.id);
    json.put("generatedBy", stream.generatedBy);
    if (stream.generatedBy != null) json.put("derivedFrom", stream.derivedFrom);
    json.put("location", Point.asJson(stream.location));
    json.put("streamStart", stream.streamStart.format(DATE_TIME_FORMATTER));
    if (stream.streamEnd != null)
      json.put("streamEnd", stream.streamEnd.format(DATE_TIME_FORMATTER));
    return json;
  }
}
