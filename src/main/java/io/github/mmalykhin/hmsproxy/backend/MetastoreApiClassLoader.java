package io.github.mmalykhin.hmsproxy.backend;

import java.net.URL;
import java.net.URLClassLoader;

public final class MetastoreApiClassLoader extends URLClassLoader {
  private static final String CHILD_FIRST_PREFIX = "org.apache.hadoop.hive.metastore.";
  private static final String CHILD_FIRST_PREFIX_HADOOP_CONF = "org.apache.hadoop.conf.";

  public MetastoreApiClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    if (!name.startsWith(CHILD_FIRST_PREFIX) && !name.startsWith(CHILD_FIRST_PREFIX_HADOOP_CONF)) {
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
}
