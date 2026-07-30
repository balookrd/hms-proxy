package io.github.mmalykhin.hmsproxy.restcatalog;

import io.github.mmalykhin.hmsproxy.config.restcatalog.RestCatalogPurgeMode;
import java.io.File;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IcebergPurgePolicyTest {
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void allowModeRefusesNothingAndWrapsNothing() throws Exception {
    IcebergPurgePolicy policy = new IcebergPurgePolicy(RestCatalogPurgeMode.ALLOW, List.of());
    HadoopFileIO io = new HadoopFileIO(new Configuration());

    Assert.assertTrue(policy.isDefaultBehaviour());
    Assert.assertNull(policy.refusalFor("default.t", metadataAt("t"), new Configuration()));
    Assert.assertSame(io, policy.guard(io, new Configuration(), "default.t"));
  }

  @Test
  public void refuseModeRefusesEveryPurge() throws Exception {
    IcebergPurgePolicy policy = new IcebergPurgePolicy(RestCatalogPurgeMode.REFUSE, List.of());

    String refusal = policy.refusalFor("default.t", metadataAt("t"), new Configuration());

    Assert.assertNotNull(refusal);
    Assert.assertTrue(refusal, refusal.contains("rest-catalog.purge.mode"));
    Assert.assertFalse(policy.isDefaultBehaviour());
  }

  @Test
  public void allowlistModeAcceptsATableInsideTheListAndRefusesOneOutside() throws Exception {
    File allowedRoot = tempFolder.newFolder("allowed");
    TableMetadata inside = metadataAt("allowed/t_inside");
    TableMetadata outside = metadataAt("elsewhere/t_outside");
    IcebergPurgePolicy policy = new IcebergPurgePolicy(
        RestCatalogPurgeMode.ALLOWLIST, List.of("file:" + allowedRoot.getAbsolutePath()));

    Assert.assertNull(policy.refusalFor("default.t_inside", inside, new Configuration()));
    String refusal = policy.refusalFor("default.t_outside", outside, new Configuration());
    Assert.assertNotNull(refusal);
    Assert.assertTrue(refusal, refusal.contains("rest-catalog.purge.allowed-prefixes"));
  }

  @Test
  public void allowlistModeRefusesWhenOnlyTheMetadataFileIsOutside() throws Exception {
    File allowedRoot = tempFolder.newFolder("allowed_meta");
    TableMetadata inside = metadataAt("allowed_meta/t");
    // A table whose data lives inside the allowlist but whose metadata.json sits outside it:
    // dropping it would still delete metadata JSON in the other tree. Reading the metadata back
    // from that file is what gives it a metadataFileLocation - a freshly built TableMetadata has
    // none, and the builder refuses to set one while it still carries pending changes.
    File outsideMetadataFile = new File(tempFolder.getRoot(), "other/v1.metadata.json");
    Assert.assertTrue(outsideMetadataFile.getParentFile().mkdirs());
    TableMetadataParser.write(inside, HadoopOutputFile.fromPath(
        new Path(outsideMetadataFile.getAbsolutePath()), new Configuration()));
    TableMetadata movedMetadata = TableMetadataParser.read(
        new HadoopFileIO(new Configuration()), "file:" + outsideMetadataFile.getAbsolutePath());
    IcebergPurgePolicy policy = new IcebergPurgePolicy(
        RestCatalogPurgeMode.ALLOWLIST, List.of("file:" + allowedRoot.getAbsolutePath()));

    Assert.assertNull(policy.refusalFor("default.t", inside, new Configuration()));
    Assert.assertNotNull(policy.refusalFor("default.t", movedMetadata, new Configuration()));
  }

  @Test
  public void allowlistModeWrapsTheFileIo() {
    IcebergPurgePolicy policy = new IcebergPurgePolicy(
        RestCatalogPurgeMode.ALLOWLIST, List.of("file:/allowed"));
    HadoopFileIO io = new HadoopFileIO(new Configuration());

    Assert.assertTrue(
        policy.guard(io, new Configuration(), "default.t") instanceof PrefixGuardedFileIO);
  }

  private TableMetadata metadataAt(String relativeDir) {
    File dir = new File(tempFolder.getRoot(), relativeDir);
    Assert.assertTrue(dir.mkdirs() || dir.isDirectory());
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
    return TableMetadata.newTableMetadata(
        schema, PartitionSpec.unpartitioned(), "file:" + dir.getAbsolutePath(), Map.of());
  }
}
