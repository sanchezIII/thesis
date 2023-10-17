package cu.uclv.mfc.enigma.compiler;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

public class Compiler {

  List<Path> classPaths;

  public Compiler() {
    this.classPaths = new ArrayList<>();
  }

  private String readCode(String sourcePath) throws FileNotFoundException {
    InputStream stream = new FileInputStream(sourcePath);
    String separator = System.getProperty("line.separator");
    BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
    return reader.lines().collect(Collectors.joining(separator));
  }

  private Path saveSource(String className, String source) throws IOException {
    //String tmpProperty = System.getProperty("java.io.tmpdir");

    String tmpProperty =
      "/home/luis/Projects/thesis/code/enigma_main/src/main/java/com/uclv/test/starter/observations/data/costum/";

    Path sourcePath = Paths.get(tmpProperty, className + ".java");
    Files.write(sourcePath, source.getBytes(StandardCharsets.UTF_8));
    return sourcePath;
  }

  private Path compileSource(String className, Path javaFile) {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();

    StringBuilder classPathArg = new StringBuilder("-classpath .");
    for (Path path : classPaths) classPathArg.append(
      ":" + path.toFile().getAbsolutePath()
    );

    if (classPaths.size() == 0) {
      compiler.run(null, null, null, javaFile.toFile().getAbsolutePath());
    } else {
      compiler.run(
        null,
        null,
        null,
        classPathArg.toString(),
        javaFile.toFile().getAbsolutePath()
      );
    }

    return javaFile.getParent().resolve(className + ".class");
  }

  private Class getClass(String className, Path javaClass)
    throws MalformedURLException, ClassNotFoundException, IllegalAccessException, InstantiationException {
    URL classUrl = javaClass.getParent().toFile().toURI().toURL();
    URLClassLoader classLoader = URLClassLoader.newInstance(
      new URL[] { classUrl }
    );
    return Class.forName(className, true, classLoader);
  }

  private String extractClassName(String classCode) throws TokenizeException {
    String[] codeTokens = classCode.split("[\\n ]");

    int idx = 0;
    while (idx < codeTokens.length && !codeTokens[idx].equals("public")) idx++;

    if (idx >= codeTokens.length) throw new TokenizeException();
    idx++;

    while (codeTokens.equals("")) idx++;

    if (!codeTokens[idx].equals("class")) throw new TokenizeException();
    idx++;

    return codeTokens[idx].split("\\{|' '")[0];
  }

  public Class compileClassFromString(String code)
    throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, TokenizeException {
    String className = extractClassName(code);

    Path javaFile = saveSource(className, code);
    Path classFile = compileSource(className, javaFile);

    return getClass(className, classFile);
  }

  //  public Class compile

  public void addClassPath(String stringPath) {
    Path path = Path.of(stringPath);
    addClassPath(path);
  }

  private void addClassPath(Path path) {
    classPaths.add(path);
  }
}
