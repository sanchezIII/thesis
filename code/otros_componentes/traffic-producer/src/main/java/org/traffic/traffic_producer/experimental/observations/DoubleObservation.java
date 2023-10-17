//package org.traffic.traffic_producer.experimental.observations;
//
//import io.vertx.core.json.JsonObject;
//import lombok.AllArgsConstructor;
//import org.traffic.traffic_producer.experimental.core.ObservationInterface;
//
//import java.time.LocalDateTime;
//import java.time.format.DateTimeFormatter;
//import java.util.UUID;
//
//@AllArgsConstructor
//public class DoubleObservation extends ObservationInterface<DoubleObservationData> {
//  public static final DateTimeFormatter DATE_TIME_FORMATTER =
//    DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm");
//
//  String id;
//  String streamId;
//  double result;
//  LocalDateTime resultTime;
//
//  @Override
//  public String getId() {
//    return id;
//  }
//
//  @Override
//  public String getStreamId() {
//    return streamId;
//  }
//
//  @Override
//  public DoubleObservationData getData() {
//    return null;
//  }
//
//  @Override
//  public LocalDateTime getTimeStamp() {
//    return null;
//  }
//
//  @Override
//  public ObservationInterface<DoubleObservationData> fromJson(JsonObject json) {
//    return new DoubleObservation(
//      json.containsKey("id") ? json.getString("id") : UUID.randomUUID().toString(),
//      json.getString("streamId"),
//      json.getNumber("result").doubleValue(),
//      LocalDateTime.parse(json.getString("resultTime"), DATE_TIME_FORMATTER));
//  }
//
//  @Override
//  public JsonObject asJson(ObservationInterface<DoubleObservationData> observation) {
////    TODO
//    return null;
//  }
//}
