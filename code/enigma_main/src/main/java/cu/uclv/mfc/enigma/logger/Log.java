package cu.uclv.mfc.enigma.logger;

import java.text.SimpleDateFormat;
import java.util.Date;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Log {

  public static final SimpleDateFormat DATE_TIME_FORMATTER =
    new SimpleDateFormat("yyyy/MM/dd HH:mm");

  private Class contextClass;

  public void info(String message, Object... args) {
    message = message.replace("{}", "%s");

    String[] strObs = new String[args.length];

    for (int i = 0; i < args.length; i++) strObs[i] = args[i].toString();

    message = String.format(message, args);

    System.out.println(
      Rbow.boldBlue("[INFO] ") +
      DATE_TIME_FORMATTER.format(new Date()) +
      " - " +
      contextClass +
      "\n   [*] " +
      message
    );
  }

  public void warn(String message, Object... args) {
    message = message.replace("{}", "%s");

    String[] strObs = new String[args.length];

    for (int i = 0; i < args.length; i++) strObs[i] = args[i].toString();

    message = String.format(message, args);

    System.out.println(
      Rbow.boldBrightYellow("[WARN] ") +
      DATE_TIME_FORMATTER.format(new Date()) +
      " - " +
      contextClass +
      "\n   [*] " +
      message
    );
  }

  public void error(String message, Object... args) {
    message = message.replace("{}", "%s");

    String[] strObs = new String[args.length];

    for (int i = 0; i < args.length; i++) strObs[i] = args[i].toString();

    message = String.format(message, args);

    System.out.println(
      Rbow.boldBrightRed("[FAIL] ") +
      DATE_TIME_FORMATTER.format(new Date()) +
      " - " +
      contextClass +
      "\n   [*] " +
      message
    );
  }

  public void sys(String message, Object... args) {
    message = message.replace("{}", "%s");

    String[] strObs = new String[args.length];

    for (int i = 0; i < args.length; i++) strObs[i] = args[i].toString();

    message = String.format(message, args);

    System.out.println(
      Rbow.boldBrightGreen("[SYST] ") +
      DATE_TIME_FORMATTER.format(new Date()) +
      " - " +
      contextClass +
      "\n   [*] " +
      message
    );
  }
}
