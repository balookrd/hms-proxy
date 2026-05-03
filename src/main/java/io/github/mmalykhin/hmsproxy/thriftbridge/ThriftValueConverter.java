package io.github.mmalykhin.hmsproxy.thriftbridge;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

public final class ThriftValueConverter {
  private static final ThreadLocal<TSerializer> SERIALIZER =
      ThreadLocal.withInitial(() -> new TSerializer(new TBinaryProtocol.Factory()));
  private static final ThreadLocal<TDeserializer> DESERIALIZER =
      ThreadLocal.withInitial(() -> new TDeserializer(new TBinaryProtocol.Factory()));
  private static final ConcurrentHashMap<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE = new ConcurrentHashMap<>();

  private ThriftValueConverter() {
  }

  public static Object convertValue(Object value, Class<?> targetType, ClassLoader collectionClassLoader)
      throws Exception {
    if (value == null) {
      return null;
    }
    if (targetType.isPrimitive()
        || Number.class.isAssignableFrom(targetType)
        || targetType == Boolean.class
        || targetType == String.class) {
      return value;
    }
    if (targetType.isEnum() && value instanceof Enum<?> enumValue) {
      @SuppressWarnings({"rawtypes", "unchecked"})
      Object converted = Enum.valueOf((Class<? extends Enum>) targetType.asSubclass(Enum.class), enumValue.name());
      return converted;
    }
    if (value instanceof List<?> || value instanceof Map<?, ?>) {
      ClassLoader cl = targetType.getClassLoader() != null ? targetType.getClassLoader() : collectionClassLoader;
      return convertDynamicValue(value, cl);
    }
    if (targetType.isInstance(value)) {
      return value;
    }
    if (value instanceof TBase<?, ?>) {
      return convertTBase(value, targetType);
    }
    return value;
  }

  public static Object convertDynamicValue(Object value, ClassLoader targetClassLoader) throws Exception {
    if (value == null) {
      return null;
    }
    if (value instanceof List<?> list) {
      List<Object> converted = new ArrayList<>(list.size());
      for (Object element : list) {
        converted.add(convertDynamicValue(element, targetClassLoader));
      }
      return converted;
    }
    if (value instanceof Map<?, ?> map) {
      Map<Object, Object> converted = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        converted.put(
            convertDynamicValue(entry.getKey(), targetClassLoader),
            convertDynamicValue(entry.getValue(), targetClassLoader));
      }
      return converted;
    }
    if (value instanceof String || value instanceof Number || value instanceof Boolean) {
      return value;
    }
    if (value.getClass().isEnum()) {
      Class<?> targetEnum = loadTargetClass(value.getClass().getName(), targetClassLoader);
      if (targetEnum != null && targetEnum.isEnum()) {
        @SuppressWarnings({"rawtypes", "unchecked"})
        Object converted = Enum.valueOf((Class<? extends Enum>) targetEnum.asSubclass(Enum.class),
            ((Enum<?>) value).name());
        return converted;
      }
    }
    if (value instanceof TBase<?, ?>) {
      Class<?> targetClass = loadTargetClass(value.getClass().getName(), targetClassLoader);
      if (targetClass != null && !targetClass.isInstance(value)) {
        return convertTBase(value, targetClass);
      }
    }
    return value;
  }

  public static Object convertTBase(Object value, Class<?> targetType) throws Exception {
    if (value == null || targetType.isInstance(value)) {
      return value;
    }
    Object target = newTargetInstance(targetType);
    byte[] bytes = SERIALIZER.get().serialize((TBase<?, ?>) value);
    DESERIALIZER.get().deserialize((TBase<?, ?>) target, bytes);
    return target;
  }

  public static Throwable convertThrowable(Throwable throwable, ClassLoader targetClassLoader) throws Exception {
    if (throwable == null) {
      return null;
    }
    if (!(throwable instanceof TBase<?, ?>)) {
      return throwable;
    }

    Class<?> targetClass = loadTargetClass(throwable.getClass().getName(), targetClassLoader);
    if (targetClass == null || !Throwable.class.isAssignableFrom(targetClass) || targetClass.isInstance(throwable)) {
      return throwable;
    }

    Throwable converted = (Throwable) convertTBase(throwable, targetClass);
    converted.setStackTrace(throwable.getStackTrace());
    Throwable convertedCause = convertThrowable(throwable.getCause(), targetClassLoader);
    if (convertedCause != null && converted.getCause() == null) {
      try {
        converted.initCause(convertedCause);
      } catch (IllegalStateException ignored) {
      }
    }
    return converted;
  }

  public static Class<?> loadTargetClass(String className, ClassLoader targetClassLoader) {
    try {
      return Class.forName(className, true, targetClassLoader);
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  private static Object newTargetInstance(Class<?> targetType) throws Exception {
    Constructor<?> constructor = CONSTRUCTOR_CACHE.get(targetType);
    if (constructor == null) {
      constructor = targetType.getConstructor();
      CONSTRUCTOR_CACHE.putIfAbsent(targetType, constructor);
    }
    return constructor.newInstance();
  }
}
