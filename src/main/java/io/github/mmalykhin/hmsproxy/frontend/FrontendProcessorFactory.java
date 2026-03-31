package io.github.mmalykhin.hmsproxy;

import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TProcessor;

final class FrontendProcessorFactory {
  private FrontendProcessorFactory() {
  }

  static TProcessor create(ProxyConfig config, ThriftHiveMetastore.Iface apacheHandler) throws Exception {
    MetastoreRuntimeProfile runtimeProfile =
        MetastoreRuntimeProfileResolver.forFrontendProfile(config.compatibility().frontendProfile());
    return switch (runtimeProfile) {
      case APACHE_3_1_3 -> ApacheFrontendBridge.createProcessor(config, apacheHandler);
      case HORTONWORKS_3_1_0_3_1_0_78 -> HortonworksFrontendBridge.createProcessor(config, apacheHandler);
    };
  }
}
