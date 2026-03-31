package io.github.mmalykhin.hmsproxy;

import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TProcessor;

final class ApacheFrontendBridge {
  private ApacheFrontendBridge() {
  }

  static TProcessor createProcessor(ProxyConfig config, ThriftHiveMetastore.Iface apacheHandler) {
    return new ThriftHiveMetastore.Processor<>(apacheHandler);
  }
}
