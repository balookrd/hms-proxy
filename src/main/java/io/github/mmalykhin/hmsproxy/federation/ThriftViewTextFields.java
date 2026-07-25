package io.github.mmalykhin.hmsproxy.federation;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;

/**
 * Per-class cache of the thrift fields that can transitively reach a {@code Table}, that is the
 * only struct carrying view SQL text.
 *
 * <p>Without it every externalized result graph (thousands of partitions and storage descriptors
 * for a single {@code get_partitions}) would be walked field by field through reflection even
 * though no view text can possibly be there.
 */
final class ThriftViewTextFields {
  private static final String VIEW_TEXT_CARRIER = "org.apache.hadoop.hive.metastore.api.Table";

  private static final ClassValue<List<TFieldIdEnum>> FIELDS_REACHING_VIEW_TEXT = new ClassValue<>() {
    @Override
    protected List<TFieldIdEnum> computeValue(Class<?> type) {
      Map<?, ?> metadata = metaDataMap(type);
      if (metadata == null) {
        return List.of();
      }
      List<TFieldIdEnum> reaching = new ArrayList<>();
      for (Map.Entry<?, ?> entry : metadata.entrySet()) {
        FieldMetaData fieldMetaData = (FieldMetaData) entry.getValue();
        if (reachesViewText(fieldMetaData.valueMetaData, type, new HashSet<>())) {
          reaching.add((TFieldIdEnum) entry.getKey());
        }
      }
      return List.copyOf(reaching);
    }
  };

  private ThriftViewTextFields() {
  }

  /**
   * Returns the fields of {@code thriftValue} worth traversing. An empty list means the whole
   * subtree cannot contain view text.
   */
  static List<TFieldIdEnum> fieldsReachingViewText(TBase<?, ?> thriftValue) {
    return FIELDS_REACHING_VIEW_TEXT.get(thriftValue.getClass());
  }

  static boolean carriesViewText(Class<?> type) {
    return VIEW_TEXT_CARRIER.equals(type.getName());
  }

  private static boolean reachesViewText(
      FieldValueMetaData valueMetaData,
      Class<?> owner,
      Set<Class<?>> inProgress
  ) {
    if (valueMetaData instanceof StructMetaData structMetaData) {
      return structReachesViewText(structMetaData.structClass, inProgress);
    }
    if (valueMetaData instanceof ListMetaData listMetaData) {
      return reachesViewText(listMetaData.elemMetaData, owner, inProgress);
    }
    if (valueMetaData instanceof SetMetaData setMetaData) {
      return reachesViewText(setMetaData.elemMetaData, owner, inProgress);
    }
    if (valueMetaData instanceof MapMetaData mapMetaData) {
      return reachesViewText(mapMetaData.keyMetaData, owner, inProgress)
          || reachesViewText(mapMetaData.valueMetaData, owner, inProgress);
    }
    if (valueMetaData.isStruct()) {
      // Thrift typedefs carry the struct name instead of its class.
      Class<?> structClass = resolveTypedef(valueMetaData.getTypedefName(), owner);
      return structClass == null || structReachesViewText(structClass, inProgress);
    }
    // Containers without concrete metadata stay traversable.
    return valueMetaData.isContainer();
  }

  private static boolean structReachesViewText(Class<?> structClass, Set<Class<?>> inProgress) {
    if (carriesViewText(structClass)) {
      return true;
    }
    if (!inProgress.add(structClass)) {
      return false;
    }
    try {
      Map<?, ?> metadata = metaDataMap(structClass);
      if (metadata == null) {
        return true;
      }
      for (Object fieldMetaData : metadata.values()) {
        if (reachesViewText(((FieldMetaData) fieldMetaData).valueMetaData, structClass, inProgress)) {
          return true;
        }
      }
      return false;
    } finally {
      inProgress.remove(structClass);
    }
  }

  private static Class<?> resolveTypedef(String typedefName, Class<?> owner) {
    if (typedefName == null || typedefName.isBlank() || owner.getPackage() == null) {
      return null;
    }
    try {
      Class<?> resolved =
          Class.forName(owner.getPackage().getName() + "." + typedefName, false, owner.getClassLoader());
      return TBase.class.isAssignableFrom(resolved) ? resolved : null;
    } catch (ClassNotFoundException | LinkageError e) {
      return null;
    }
  }

  private static Map<?, ?> metaDataMap(Class<?> type) {
    try {
      Field metadataField = type.getField("metaDataMap");
      return (Map<?, ?>) metadataField.get(null);
    } catch (NoSuchFieldException | IllegalAccessException | ClassCastException e) {
      return null;
    }
  }
}
