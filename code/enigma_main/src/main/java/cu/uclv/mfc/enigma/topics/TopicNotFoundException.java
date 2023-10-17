package cu.uclv.mfc.enigma.topics;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class TopicNotFoundException extends Throwable {

  private String message;

  @Override
  public String getMessage() {
    return message;
  }
}
