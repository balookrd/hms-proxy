package io.github.mmalykhin.hmsproxy.routing;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.TFieldRequirementType;
import org.apache.thrift.meta_data.FieldMetaData;

final class ThriftReflectionCache {
  private static final ConcurrentMap<Class<?>, Field> METADATA_FIELD_CACHE = new ConcurrentHashMap<>();
  private static final ConcurrentMap<Class<?>, ConcurrentMap<String, Optional<Method>>> METHOD_CACHE =
      new ConcurrentHashMap<>();

  private ThriftReflectionCache() {}

  static String readString(Object target, String... getterNames) {
    ConcurrentMap<String, Optional<Method>> classCache = METHOD_CACHE
        .computeIfAbsent(target.getClass(), ignored -> new ConcurrentHashMap<>());
    for (String getterName : getterNames) {
      Optional<Method> cached = classCache.computeIfAbsent(getterName, name -> {
        try {
          return Optional.of(target.getClass().getMethod(name));
        } catch (NoSuchMethodException ignored) {
          return Optional.empty();
        }
      });
      if (cached.isEmpty()) {
        continue;
      }
      try {
        return (String) cached.get().invoke(target);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new IllegalStateException(
            "Unable to invoke " + getterName + " on " + target.getClass().getName(), e);
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  static List<String> readStringList(Object target, String getterName) {
    ConcurrentMap<String, Optional<Method>> classCache = METHOD_CACHE
        .computeIfAbsent(target.getClass(), ignored -> new ConcurrentHashMap<>());
    Optional<Method> cached = classCache.computeIfAbsent(getterName, name -> {
      try {
        return Optional.of(target.getClass().getMethod(name));
      } catch (NoSuchMethodException ignored) {
        return Optional.empty();
      }
    });
    if (cached.isEmpty()) {
      return null;
    }
    try {
      Object value = cached.get().invoke(target);
      return value == null ? null : (List<String>) value;
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException(
          "Unable to invoke " + getterName + " on " + target.getClass().getName(), e);
    }
  }

  static void invokeStringSetter(Object target, String methodName, String argument) {
    String cacheKey = methodName + "(String)";
    ConcurrentMap<String, Optional<Method>> classCache = METHOD_CACHE
        .computeIfAbsent(target.getClass(), ignored -> new ConcurrentHashMap<>());
    Optional<Method> cached = classCache.computeIfAbsent(cacheKey, ignored -> {
      try {
        return Optional.of(target.getClass().getMethod(methodName, String.class));
      } catch (NoSuchMethodException ignoredEx) {
        return Optional.empty();
      }
    });
    if (cached.isEmpty()) {
      return;
    }
    try {
      cached.get().invoke(target, argument);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException(
          "Unable to invoke " + methodName + " on " + target.getClass().getName(), e);
    }
  }

  static void invokeStringListSetter(Object target, String methodName, List<String> values) {
    String cacheKey = methodName + "(List)";
    ConcurrentMap<String, Optional<Method>> classCache = METHOD_CACHE
        .computeIfAbsent(target.getClass(), ignored -> new ConcurrentHashMap<>());
    Optional<Method> cached = classCache.computeIfAbsent(cacheKey, ignored -> {
      try {
        return Optional.of(target.getClass().getMethod(methodName, List.class));
      } catch (NoSuchMethodException ignoredEx) {
        return Optional.empty();
      }
    });
    if (cached.isEmpty()) {
      return;
    }
    try {
      cached.get().invoke(target, values);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException(
          "Unable to invoke " + methodName + " on " + target.getClass().getName(), e);
    }
  }

  @SuppressWarnings("unchecked")
  static List<TFieldIdEnum> fieldIds(TBase<?, ?> thriftValue) {
    try {
      Field metadataField = metadataField(thriftValue.getClass());
      Map<?, ?> metadata = (Map<?, ?>) metadataField.get(null);
      List<TFieldIdEnum> result = new ArrayList<>();
      for (Object fieldId : metadata.keySet()) {
        TFieldIdEnum typedFieldId = (TFieldIdEnum) fieldId;
        if (isFieldSet(thriftValue, typedFieldId)) {
          result.add(typedFieldId);
        }
      }
      return result;
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(
          "Unable to inspect thrift metadata for " + thriftValue.getClass().getName(), e);
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  static Object getField(TBase<?, ?> thriftValue, TFieldIdEnum fieldId) {
    return ((TBase) thriftValue).getFieldValue(fieldId);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  static void setField(TBase<?, ?> thriftValue, TFieldIdEnum fieldId, Object value) {
    ((TBase) thriftValue).setFieldValue(fieldId, value);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  static boolean isFieldSet(TBase<?, ?> thriftValue, TFieldIdEnum fieldId) {
    return ((TBase) thriftValue).isSet(fieldId);
  }

  static boolean hasRequiredField(TBase<?, ?> thriftValue, String expectedFieldName) {
    try {
      Map<?, ?> metadata = (Map<?, ?>) metadataField(thriftValue.getClass()).get(null);
      for (Map.Entry<?, ?> entry : metadata.entrySet()) {
        TFieldIdEnum fieldId = (TFieldIdEnum) entry.getKey();
        if (!fieldId.getFieldName().equals(expectedFieldName)) {
          continue;
        }
        FieldMetaData fieldMetaData = (FieldMetaData) entry.getValue();
        return fieldMetaData.requirementType == TFieldRequirementType.REQUIRED;
      }
      return false;
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(
          "Unable to inspect thrift metadata for " + thriftValue.getClass().getName(), e);
    }
  }

  private static Field metadataField(Class<?> cls) {
    return METADATA_FIELD_CACHE.computeIfAbsent(cls, c -> {
      try {
        return c.getField("metaDataMap");
      } catch (NoSuchFieldException e) {
        throw new IllegalStateException("Unable to locate metaDataMap on " + c.getName(), e);
      }
    });
  }
}
