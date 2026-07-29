package io.github.mmalykhin.hmsproxy.restcatalog;

import java.util.Objects;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.iceberg.ClientPool;
import org.apache.thrift.TException;

/**
 * Stateless ClientPool that always hands the same IMetaStoreClient back to
 * HiveCatalog. The wrapped client is a Proxy over our ThriftHiveMetastore.Iface,
 * so there is no connection to acquire, recycle, or close.
 */
public final class RoutingClientPool implements ClientPool<IMetaStoreClient, TException> {
  private final IMetaStoreClient client;

  public RoutingClientPool(IMetaStoreClient client) {
    this.client = Objects.requireNonNull(client, "client");
  }

  @Override
  public <R> R run(Action<R, IMetaStoreClient, TException> action) throws TException, InterruptedException {
    return action.run(client);
  }

  @Override
  public <R> R run(Action<R, IMetaStoreClient, TException> action, boolean retry) throws TException, InterruptedException {
    return action.run(client);
  }
}
