package io.github.mmalykhin.hmsproxy.backend;

import java.net.URL;
import java.net.URLClassLoader;

public final class MetastoreApiClassLoader extends URLClassLoader {
  private static final String[] CHILD_FIRST_PREFIXES = {
      "org.apache.hadoop.hive.metastore.",
      "org.apache.hadoop.conf.",
      "org.apache.hadoop.security.",
  };

  public MetastoreApiClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
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
}
