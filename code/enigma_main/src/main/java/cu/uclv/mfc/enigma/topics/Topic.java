package cu.uclv.mfc.enigma.topics;

import cu.uclv.mfc.enigma.observations.factory.ObservationDataFactory;
import io.vertx.core.json.JsonObject;
import java.util.UUID;
import lombok.Getter;

@Getter
public class Topic {

  private String id;
  private String name;
  private String author;
  private String description;
  private String databaseName;
  private ObservationDataFactory factory;
  private Class observationDataClass;
  private Long intervalInSeconds;

  public Topic(
    String name,
    String author,
    String description,
    String databaseName,
    Long intervalInSeconds,
    Class observationDataClass
  ) {
    this(
      UUID.randomUUID() + "",
      name,
      author,
      description,
      databaseName,
      intervalInSeconds,
      observationDataClass
    );
    this.author = author;
    this.description = description;
  }

  public Topic(
    String id,
    String name,
    String author,
    String description,
    String databaseName,
    Long intervalInSeconds,
    Class observationDataClass
  ) {
    this.id = id;
    this.databaseName = databaseName;
    this.observationDataClass = observationDataClass;
    this.name = name;
    this.author = author;
    this.description = description;
    this.intervalInSeconds = intervalInSeconds;

    this.factory = new ObservationDataFactory(observationDataClass);
  }

  public static Topic fromJson(JsonObject json) throws ClassNotFoundException {
    Topic topic = null;

    topic =
      new Topic(
        json.containsKey("id")
          ? json.getString("id")
          : UUID.randomUUID().toString(),
        json.getString("name"),
        json.getString("author"),
        json.getString("description"),
        json.getString("databaseName"),
        json.getLong("interval"),
        Class.forName(json.getString("observationDataClassName"))
      );

    return topic;
  }

  public static JsonObject asJson(Topic topic) {
    return new JsonObject()
      .put("id", topic.getId())
      .put("databaseName", topic.getDatabaseName())
      .put("author", topic.getAuthor())
      .put("description", topic.getDescription())
      .put(
        "observationDataClassName",
        topic.getObservationDataClass().getName()
      )
      .put("name", topic.getName())
      .put("interval", topic.getIntervalInSeconds());
  }

  @Override
  public String toString() {
    return (
      "Topic{" +
      "id='" +
      id +
      '\'' +
      ", name='" +
      name +
      '\'' +
      ", observationDataClass=" +
      observationDataClass +
      '}'
    );
  }
}
