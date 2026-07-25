package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.thriftbridge.ThriftValueConverter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public final class IsolatedInvocationBridge {
  private final ClassLoader classLoader;
  private final Object delegate;
  private final Class<?> ifaceClass;

  public IsolatedInvocationBridge(ClassLoader classLoader, Object delegate, Class<?> ifaceClass) {
    this.classLoader = classLoader;
    this.delegate = delegate;
    this.ifaceClass = ifaceClass;
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
      throw ThriftValueConverter.convertThrowable(e.getCause(), IsolatedInvocationBridge.class.getClassLoader());
    }
  }

  Object invokeByName(String methodName, Class<?>[] parameterTypes, Object[] args) throws Throwable {
    Method targetMethod = findMethod(methodName, parameterTypes);
    Object[] convertedArgs = convertArguments(args, targetMethod.getParameterTypes());
    try {
      Object result = withContextClassLoader(() -> targetMethod.invoke(delegate, convertedArgs));
      return ThriftValueConverter.convertDynamicValue(result, IsolatedInvocationBridge.class.getClassLoader());
    } catch (InvocationTargetException e) {
      throw ThriftValueConverter.convertThrowable(e.getCause(), IsolatedInvocationBridge.class.getClassLoader());
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
    return ThriftMethodCache.lookup(
        ifaceClass, methodName, parameterTypes, () -> resolveMethod(methodName, parameterTypes));
  }

  private Method resolveMethod(String methodName, Class<?>[] parameterTypes) throws NoSuchMethodException {
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
      converted[index] = ThriftValueConverter.convertValue(args[index], parameterTypes[index], classLoader);
    }
    return converted;
  }

  private Object convertResult(Object result, Class<?> returnType) throws Exception {
    if (returnType == void.class || result == null) {
      return null;
    }
    return ThriftValueConverter.convertValue(result, returnType, IsolatedInvocationBridge.class.getClassLoader());
  }

  @FunctionalInterface
  private interface ThrowingSupplier<T> {
    T get() throws Exception;
  }
}
