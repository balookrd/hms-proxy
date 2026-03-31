package io.github.mmalykhin.hmsproxy.backend;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.LinkedHashSet;

public final class MetastoreApiClassLoader extends URLClassLoader {
  private static final String[] CHILD_FIRST_PREFIXES = {
      "org.apache.hadoop.",
  };

  public MetastoreApiClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
  }

  public static URL[] buildIsolatedRuntimeUrls(Path metastoreJar) throws MalformedURLException {
    LinkedHashSet<URL> urls = new LinkedHashSet<>();
    urls.add(metastoreJar.toUri().toURL());
    addCodeSourceUrl(urls, org.apache.hadoop.classification.InterfaceAudience.class);
    addCodeSourceUrl(urls, org.apache.hadoop.conf.Configuration.class);
    addCodeSourceUrl(urls, org.apache.hadoop.security.UserGroupInformation.class);
    addCodeSourceUrl(urls, org.apache.hadoop.security.authentication.client.AuthenticationException.class);
    addCodeSourceUrl(urls, org.apache.hadoop.mapreduce.Job.class);
    return urls.toArray(new URL[0]);
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    if (!isChildFirst(name)) {
      return super.loadClass(name, resolve);
    }

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

  private static boolean isChildFirst(String name) {
    for (String prefix : CHILD_FIRST_PREFIXES) {
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
