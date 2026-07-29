package io.github.mmalykhin.hmsproxy.thriftbridge;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;

public final class ThriftValueConverter {
  private static final ThreadLocal<TSerializer> SERIALIZER =
      ThreadLocal.withInitial(() -> new TSerializer(new TBinaryProtocol.Factory()));
  private static final ThreadLocal<TDeserializer> DESERIALIZER =
      ThreadLocal.withInitial(() -> new TDeserializer(new TBinaryProtocol.Factory()));

  private static final String TBASE_NAME = "org.apache.thrift.TBase";

  /**
   * Serializer/deserializer pair for thrift types living in a FOREIGN classloader - the Hive 4
   * backend runtime carries its own libthrift 0.16 child-first, so its structs do not implement
   * the parent's {@link TBase} and the shared (de)serializers above cannot touch them. The wire
   * format (binary protocol) is stable across libthrift versions, which is exactly what makes a
   * serialize-here/deserialize-there conversion between the two lines possible. Weak keys so a
   * retired catalog's classloader (and metaspace) is not pinned; ThreadLocal because thrift
   * (de)serializers are not thread-safe.
   */
  private static final ThreadLocal<Map<ClassLoader, ForeignThriftCodec>> FOREIGN_CODECS =
      ThreadLocal.withInitial(WeakHashMap::new);

  // ClassValue, not a map keyed by Class: the cached constructors belong to the isolated metastore
  // runtime, and a strong-referenced map would pin every retired catalog class loader (and its
  // metaspace) for the lifetime of the JVM. The lookup outcome is cached either way, failure
  // included, because it is a pure function of the class.
  private static final ClassValue<Object> CONSTRUCTOR_CACHE = new ClassValue<>() {
    @Override
    protected Object computeValue(Class<?> type) {
      try {
        return type.getConstructor();
      } catch (NoSuchMethodException e) {
        return e;
      }
    }
  };

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
    if (isThriftStruct(value)) {
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
    if (isThriftStruct(value)) {
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
    deserializeInto(target, serializeStruct(value));
    return target;
  }

  public static Throwable convertThrowable(Throwable throwable, ClassLoader targetClassLoader) throws Exception {
    if (throwable == null) {
      return null;
    }
    if (!isThriftStruct(throwable)) {
      return convertForeignThriftInfrastructureException(throwable, targetClassLoader);
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

  /**
   * True for any thrift struct, including one whose {@code TBase} interface lives in a foreign
   * classloader (the Hive 4 backend runtime loads its own libthrift child-first, so a plain
   * {@code instanceof} against the parent's interface misses its structs entirely).
   */
  private static boolean isThriftStruct(Object value) {
    return value instanceof TBase<?, ?> || implementsInterfaceNamed(value.getClass(), TBASE_NAME);
  }

  private static boolean implementsInterfaceNamed(Class<?> type, String interfaceName) {
    for (Class<?> current = type; current != null; current = current.getSuperclass()) {
      for (Class<?> iface : current.getInterfaces()) {
        if (iface.getName().equals(interfaceName) || implementsInterfaceNamed(iface, interfaceName)) {
          return true;
        }
      }
    }
    return false;
  }

  private static byte[] serializeStruct(Object value) throws Exception {
    if (value instanceof TBase<?, ?> parentStruct) {
      return SERIALIZER.get().serialize(parentStruct);
    }
    return foreignCodec(value.getClass().getClassLoader()).serialize(value);
  }

  private static void deserializeInto(Object target, byte[] bytes) throws Exception {
    if (target instanceof TBase<?, ?> parentStruct) {
      DESERIALIZER.get().deserialize(parentStruct, bytes);
      return;
    }
    foreignCodec(target.getClass().getClassLoader()).deserialize(target, bytes);
  }

  /**
   * Maps a thrift infrastructure exception of a foreign libthrift (TApplicationException,
   * TTransportException, ...) onto the parent's equivalent, preserving the type code where one
   * exists. Without this, {@code ThriftFailureClassifier}'s {@code instanceof} checks - the
   * single place deciding fallback, downgrade and reconnect behavior - would silently
   * misclassify every failure of a child-first thrift runtime.
   */
  private static Throwable convertForeignThriftInfrastructureException(
      Throwable throwable, ClassLoader targetClassLoader) throws Exception {
    String className = throwable.getClass().getName();
    if (!className.startsWith("org.apache.thrift.")) {
      return throwable;
    }
    Class<?> targetClass = loadTargetClass(className, targetClassLoader);
    if (targetClass == null || targetClass.isInstance(throwable)) {
      return throwable;
    }
    Throwable converted = switch (className) {
      case "org.apache.thrift.TApplicationException" ->
          new TApplicationException(reflectiveType(throwable), throwable.getMessage());
      case "org.apache.thrift.transport.TTransportException" ->
          new TTransportException(reflectiveType(throwable), throwable.getMessage());
      case "org.apache.thrift.protocol.TProtocolException" ->
          new TProtocolException(reflectiveType(throwable), throwable.getMessage());
      default -> new TException(throwable.getMessage());
    };
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

  private static int reflectiveType(Throwable throwable) {
    try {
      return (int) throwable.getClass().getMethod("getType").invoke(throwable);
    } catch (ReflectiveOperationException e) {
      return 0;
    }
  }

  private static ForeignThriftCodec foreignCodec(ClassLoader loader) throws Exception {
    Map<ClassLoader, ForeignThriftCodec> codecs = FOREIGN_CODECS.get();
    ForeignThriftCodec codec = codecs.get(loader);
    if (codec == null) {
      codec = ForeignThriftCodec.create(loader);
      codecs.put(loader, codec);
    }
    return codec;
  }

  private static Object newTargetInstance(Class<?> targetType) throws Exception {
    Object cached = CONSTRUCTOR_CACHE.get(targetType);
    if (cached instanceof NoSuchMethodException missingConstructor) {
      throw missingConstructor;
    }
    return ((Constructor<?>) cached).newInstance();
  }

  private record ForeignThriftCodec(
      Object serializer,
      Method serializeMethod,
      Object deserializer,
      Method deserializeMethod
  ) {
    static ForeignThriftCodec create(ClassLoader loader) throws Exception {
      Class<?> tbaseClass = Class.forName(TBASE_NAME, true, loader);
      Class<?> protocolFactoryClass = Class.forName("org.apache.thrift.protocol.TProtocolFactory", true, loader);
      Object binaryFactory = Class.forName("org.apache.thrift.protocol.TBinaryProtocol$Factory", true, loader)
          .getConstructor().newInstance();
      Class<?> serializerClass = Class.forName("org.apache.thrift.TSerializer", true, loader);
      Class<?> deserializerClass = Class.forName("org.apache.thrift.TDeserializer", true, loader);
      return new ForeignThriftCodec(
          serializerClass.getConstructor(protocolFactoryClass).newInstance(binaryFactory),
          serializerClass.getMethod("serialize", tbaseClass),
          deserializerClass.getConstructor(protocolFactoryClass).newInstance(binaryFactory),
          deserializerClass.getMethod("deserialize", tbaseClass, byte[].class));
    }

    byte[] serialize(Object struct) throws Exception {
      return (byte[]) serializeMethod.invoke(serializer, struct);
    }

    void deserialize(Object struct, byte[] bytes) throws Exception {
      deserializeMethod.invoke(deserializer, struct, bytes);
    }
  }
}
