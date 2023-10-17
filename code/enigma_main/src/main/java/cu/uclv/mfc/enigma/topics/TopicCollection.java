package cu.uclv.mfc.enigma.topics;

import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class TopicCollection {

  private Map<String, Topic> topics;

  public TopicCollection() {
    topics = new HashMap();
  }

  public Topic getTopicById(String id) throws TopicNotFoundException {
    if (topics.containsKey(id)) return topics.get(id);
    throw new TopicNotFoundException("No such topic: " + id);
  }

  public Topic add(Topic topic) {
    topics.put(topic.getId(), topic);
    return topic;
  }

  @Override
  public String toString() {
    return topics.toString();
  }

  public int size() {
    return topics.size();
  }
}
