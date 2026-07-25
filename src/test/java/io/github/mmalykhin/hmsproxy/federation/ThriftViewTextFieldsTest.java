package io.github.mmalykhin.hmsproxy.federation;

import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Assert;
import org.junit.Test;

public class ThriftViewTextFieldsTest {
  @Test
  public void partitionSubtreesAreNotTraversed() {
    Assert.assertTrue(ThriftViewTextFields.fieldsReachingViewText(new Partition()).isEmpty());
    Assert.assertTrue(
        ThriftViewTextFields.fieldsReachingViewText(new StorageDescriptor()).isEmpty());
  }

  @Test
  public void tableItselfCarriesViewTextButHasNoNestedTables() {
    Assert.assertTrue(ThriftViewTextFields.carriesViewText(Table.class));
    Assert.assertTrue(ThriftViewTextFields.fieldsReachingViewText(new Table()).isEmpty());
  }

  @Test
  public void wrappersAroundTableExposeOnlyTheTableField() {
    Assert.assertEquals(
        java.util.List.of(GetTableResult._Fields.TABLE),
        ThriftViewTextFields.fieldsReachingViewText(new GetTableResult()));
  }
}
