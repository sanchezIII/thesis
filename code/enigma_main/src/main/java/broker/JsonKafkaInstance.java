package broker;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.NewTopic;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClientConfig;

public class JsonKafkaInstance {

  private final KafkaAdminClient adminClient;

  public JsonKafkaInstance(Vertx vertx, String host, int port) {
    Properties config = new Properties();
    config.put(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
      String.format("%s:%d", host, port)
    );

    adminClient = KafkaAdminClient.create(vertx, config);
  }

  public Future<Set<String>> listTopics() {
    return adminClient.listTopics();
  }

  public Future<NewTopic> createTopic(
    String name,
    int numPartitions,
    short replicationFactor
  ) {
    NewTopic topic = new NewTopic(name, numPartitions, replicationFactor);
    ArrayList<NewTopic> topicList = new ArrayList<>();

    return adminClient
      .createTopics(topicList)
      .compose(voidArg -> Future.succeededFuture(topic));
  }

  public Future<List<NewTopic>> createTopics(List<NewTopic> topicList) {
    return adminClient
      .createTopics(topicList)
      .compose(voidArg -> Future.succeededFuture(topicList));
  }
}
