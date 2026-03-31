package io.github.mmalykhin.hmsproxy;

import java.net.URL;
import java.net.URLClassLoader;

final class MetastoreApiClassLoader extends URLClassLoader {
  private static final String CHILD_FIRST_PREFIX = "org.apache.hadoop.hive.metastore.";

  MetastoreApiClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    if (!name.startsWith(CHILD_FIRST_PREFIX)) {
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
