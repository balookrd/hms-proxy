package io.github.mmalykhin.hmsproxy.backend;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

public final class IsolatedInvocationBridge {
  private final ClassLoader classLoader;
  private final Object delegate;
  private final Class<?> ifaceClass;
  private final TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
  private final TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());

  public IsolatedInvocationBridge(ClassLoader classLoader, Object delegate, Class<?> ifaceClass) {
    this.classLoader = classLoader;
    this.delegate = delegate;
    this.ifaceClass = ifaceClass;
  }

  String getVersion() throws Throwable {
    return (String) invokeByName("getVersion", new Class<?>[0], new Object[0]);
  }

  void setUgi(String userName, List<String> groupNames) throws Throwable {
    invokeByName("set_ugi", new Class<?>[] {String.class, List.class}, new Object[] {userName, groupNames});
  }

  Object invoke(Method method, Object[] args) throws Throwable {
    Method targetMethod = findMethod(method.getName(), method.getParameterTypes());
    Object[] convertedArgs = convertArguments(args, targetMethod.getParameterTypes());
    try {
      Object result = withContextClassLoader(() -> targetMethod.invoke(delegate, convertedArgs));
      return convertResult(result, method.getReturnType());
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  Object invokeByName(String methodName, Class<?>[] parameterTypes, Object[] args) throws Throwable {
    Method targetMethod = findMethod(methodName, parameterTypes);
    Object[] convertedArgs = convertArguments(args, targetMethod.getParameterTypes());
    try {
      Object result = withContextClassLoader(() -> targetMethod.invoke(delegate, convertedArgs));
      return convertDynamicValue(result, IsolatedInvocationBridge.class.getClassLoader());
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  private <T> T withContextClassLoader(ThrowingSupplier<T> supplier) throws Exception {
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    thread.setContextClassLoader(classLoader);
    try {
      return supplier.get();
    } finally {
      thread.setContextClassLoader(previous);
    }
  }

  private Method findMethod(String methodName, Class<?>[] parameterTypes) throws NoSuchMethodException {
    try {
      return ifaceClass.getMethod(methodName, remapParameterTypes(parameterTypes));
    } catch (NoSuchMethodException ignored) {
      for (Method candidate : ifaceClass.getMethods()) {
        if (!candidate.getName().equals(methodName)) {
          continue;
        }
        if (candidate.getParameterCount() != parameterTypes.length) {
          continue;
        }
        if (matchesParameterTypes(candidate.getParameterTypes(), parameterTypes)) {
          return candidate;
        }
      }
      throw new NoSuchMethodException(methodName);
    }
  }

  private Class<?>[] remapParameterTypes(Class<?>[] parameterTypes) throws NoSuchMethodException {
    Class<?>[] remapped = new Class<?>[parameterTypes.length];
    for (int index = 0; index < parameterTypes.length; index++) {
      remapped[index] = remapParameterType(parameterTypes[index]);
    }
    return remapped;
  }

  private Class<?> remapParameterType(Class<?> parameterType) throws NoSuchMethodException {
    if (parameterType.isPrimitive() || parameterType == String.class || parameterType == List.class
        || parameterType == Map.class || parameterType == Object.class) {
      return parameterType;
    }
    try {
      return Class.forName(parameterType.getName(), true, classLoader);
    } catch (ClassNotFoundException e) {
      throw new NoSuchMethodException(parameterType.getName());
    }
  }

  private boolean matchesParameterTypes(Class<?>[] candidateTypes, Class<?>[] requestedTypes) {
    for (int index = 0; index < candidateTypes.length; index++) {
      Class<?> candidate = candidateTypes[index];
      Class<?> requested = requestedTypes[index];
      if (candidate == requested) {
        continue;
      }
      if (candidate.getName().equals(requested.getName())) {
        continue;
      }
      if ((candidate == List.class || candidate == Map.class)
          && (requested == List.class || requested == Map.class)) {
        continue;
      }
      if (candidate.isPrimitive() && wrapPrimitive(candidate) == requested) {
        continue;
      }
      if (requested.isPrimitive() && wrapPrimitive(requested) == candidate) {
        continue;
      }
      return false;
    }
    return true;
  }

  private Class<?> wrapPrimitive(Class<?> primitiveType) {
    if (primitiveType == boolean.class) {
      return Boolean.class;
    }
    if (primitiveType == int.class) {
      return Integer.class;
    }
    if (primitiveType == long.class) {
      return Long.class;
    }
    if (primitiveType == short.class) {
      return Short.class;
    }
    if (primitiveType == byte.class) {
      return Byte.class;
    }
    if (primitiveType == double.class) {
      return Double.class;
    }
    if (primitiveType == float.class) {
      return Float.class;
    }
    if (primitiveType == char.class) {
      return Character.class;
    }
    return primitiveType;
  }

  private Object[] convertArguments(Object[] args, Class<?>[] parameterTypes) throws Exception {
    if (args == null || args.length == 0) {
      return args;
    }
    Object[] converted = new Object[args.length];
    for (int index = 0; index < args.length; index++) {
      converted[index] = convertValue(args[index], parameterTypes[index]);
    }
    return converted;
  }

  private Object convertResult(Object result, Class<?> returnType) throws Exception {
    if (returnType == void.class || result == null) {
      return null;
    }
    return convertValue(result, returnType);
  }

  private Object convertValue(Object value, Class<?> targetType) throws Exception {
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
      return convertDynamicValue(value, targetType.getClassLoader());
    }
    if (targetType.isInstance(value)) {
      return value;
    }
    if (value instanceof TBase<?, ?>) {
      return convertTBase(value, targetType);
    }
    return value;
  }

  private Object convertTBase(Object value, Class<?> targetType) throws Exception {
    if (value == null || targetType.isInstance(value)) {
      return value;
    }
    Object target = targetType.getConstructor().newInstance();
    byte[] bytes = serializer.serialize((TBase<?, ?>) value);
    deserializer.deserialize((TBase<?, ?>) target, bytes);
    return target;
  }

  private Object convertDynamicValue(Object value, ClassLoader targetClassLoader) throws Exception {
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

  private Class<?> loadTargetClass(String className, ClassLoader targetClassLoader) {
    try {
      return Class.forName(className, true, targetClassLoader);
    } catch (ClassNotFoundException e) {
      return null;
    }
  }

  @FunctionalInterface
  private interface ThrowingSupplier<T> {
    T get() throws Exception;
  }
}
