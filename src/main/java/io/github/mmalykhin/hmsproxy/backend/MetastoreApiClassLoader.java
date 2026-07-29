package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.config.server.MetastoreRuntimeProfile;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashSet;

public final class MetastoreApiClassLoader extends URLClassLoader {
  private static final String[] CHILD_FIRST_PREFIXES = {
      "org.apache.hadoop.",
  };

  // The Hive 4 client runtime is generated against libthrift 0.16, whose transport classes
  // (org.apache.thrift.transport.layered.*) do not exist in the 0.9.3 the fat jar carries for
  // the 3.1 line. The matching libthrift travels as a companion jar next to the metastore jar
  // and is loaded CHILD-FIRST, so the isolated Hive 4 client is self-contained; every TBase
  // value and thrift exception crossing back is converted by ThriftValueConverter, which
  // handles foreign-loader thrift types. fb303 must follow thrift into the child: the Hive 4
  // client extends FacebookService$Client, and a parent-first fb303 is linked against the
  // parent's TProtocol ("loader constraint violation"). The 0.9.3/0.16 pairing is exactly what
  // upstream Hive 4 ships. The Hive 4 FRONT door bridge deliberately does not use this: its
  // generated processor must implement the parent's 0.9.3 TProcessor, because the proxy's
  // Thrift server stack lives in the parent loader.
  private static final String COMPANION_LIBTHRIFT_JAR = "libthrift-0.16.0.jar";
  private static final String COMPANION_LIBFB303_JAR = "libfb303-0.9.3.jar";
  // org.apache.hadoop.hive.common.TableName and friends: referenced by the Hive 4 client but
  // living in hive-storage-api, which the 3.1-line fat jar has no counterpart for. Already
  // covered by the default org.apache.hadoop. child-first prefix once the jar is on the URLs.
  private static final String COMPANION_STORAGE_API_JAR = "hive-storage-api-4.1.0.jar";

  private final String[] childFirstPrefixes;

  // Without this the child-first path below would serialize every isolated-runtime class load on
  // this loader's monitor; warmup pulls hundreds of thrift classes from concurrent requests.
  static {
    registerAsParallelCapable();
  }

  public MetastoreApiClassLoader(URL[] urls, ClassLoader parent) {
    this(urls, parent, new String[0]);
  }

  public MetastoreApiClassLoader(URL[] urls, ClassLoader parent, String[] extraChildFirstPrefixes) {
    super(urls, parent);
    String[] prefixes = new String[CHILD_FIRST_PREFIXES.length + extraChildFirstPrefixes.length];
    System.arraycopy(CHILD_FIRST_PREFIXES, 0, prefixes, 0, CHILD_FIRST_PREFIXES.length);
    System.arraycopy(extraChildFirstPrefixes, 0, prefixes, CHILD_FIRST_PREFIXES.length, extraChildFirstPrefixes.length);
    this.childFirstPrefixes = prefixes;
  }

  /**
   * Loader for an isolated BACKEND client runtime. For Hive 4 profiles this adds the companion
   * libthrift jar (expected next to the metastore jar) and makes {@code org.apache.thrift.}
   * child-first; the 3.1 profiles keep sharing the parent's 0.9.3 classes their generated code
   * was built against.
   */
  public static MetastoreApiClassLoader forBackendRuntime(
      Path metastoreJar,
      MetastoreRuntimeProfile runtimeProfile,
      ClassLoader parent
  ) throws MalformedURLException {
    if (!runtimeProfile.isHive4()) {
      return new MetastoreApiClassLoader(buildIsolatedRuntimeUrls(metastoreJar), parent);
    }
    Path libthrift = requireCompanion(metastoreJar, COMPANION_LIBTHRIFT_JAR, runtimeProfile);
    Path libfb303 = requireCompanion(metastoreJar, COMPANION_LIBFB303_JAR, runtimeProfile);
    Path storageApi = requireCompanion(metastoreJar, COMPANION_STORAGE_API_JAR, runtimeProfile);
    return new MetastoreApiClassLoader(
        buildIsolatedRuntimeUrls(metastoreJar, libthrift, libfb303, storageApi),
        parent,
        new String[] {"org.apache.thrift.", "com.facebook.fb303."});
  }

  private static Path requireCompanion(Path metastoreJar, String companionJar, MetastoreRuntimeProfile profile) {
    Path companion = metastoreJar.resolveSibling(companionJar);
    if (!Files.isReadable(companion)) {
      throw new IllegalStateException(
          "The " + profile + " client runtime needs its companion " + companionJar
              + " next to " + metastoreJar + ": the Hive 4 client is generated against libthrift 0.16,"
              + " which the proxy's own classpath (0.9.3 for the 3.1 line) cannot provide");
    }
    return companion;
  }

  public static URL[] buildIsolatedRuntimeUrls(Path metastoreJar, Path... companionJars)
      throws MalformedURLException {
    LinkedHashSet<URL> urls = new LinkedHashSet<>();
    urls.add(metastoreJar.toUri().toURL());
    for (Path companion : companionJars) {
      urls.add(companion.toUri().toURL());
    }
    addCodeSourceUrl(urls, org.apache.hadoop.classification.InterfaceAudience.class);
    addCodeSourceUrl(urls, org.apache.hadoop.conf.Configuration.class);
    addCodeSourceUrl(urls, org.apache.hadoop.security.UserGroupInformation.class);
    addCodeSourceUrl(urls, org.apache.hadoop.security.authentication.client.AuthenticationException.class);
    addCodeSourceUrl(urls, org.apache.hadoop.mapreduce.Job.class);
    return urls.toArray(new URL[0]);
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    if (!isChildFirst(name)) {
      return super.loadClass(name, resolve);
    }

    synchronized (getClassLoadingLock(name)) {
      Class<?> loaded = findLoadedClass(name);
      if (loaded == null) {
        try {
          loaded = findClass(name);
        } catch (ClassNotFoundException e) {
          loaded = super.loadClass(name, false);
        }
      }
      if (resolve) {
        resolveClass(loaded);
      }
      return loaded;
    }
  }

  private boolean isChildFirst(String name) {
    for (String prefix : childFirstPrefixes) {
      if (name.startsWith(prefix)) {
        return true;
      }
    }
    return false;
  }

  private static void addCodeSourceUrl(LinkedHashSet<URL> urls, Class<?> type) {
    if (type.getProtectionDomain() == null
        || type.getProtectionDomain().getCodeSource() == null
        || type.getProtectionDomain().getCodeSource().getLocation() == null) {
      return;
    }
    urls.add(type.getProtectionDomain().getCodeSource().getLocation());
  }
}
