package io.github.mmalykhin.hmsproxy.security;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.security.DelegationTokenIdentifier;
import org.apache.hadoop.hive.metastore.security.DelegationTokenStore;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
import org.junit.Assert;
import org.junit.Test;

public class LocalDelegationTokenStoreTest {
  @Test
  public void addTokenDecodesRpcPayloadBeforeWritingToStore() throws Exception {
    RecordingTokenStore tokenStore = new RecordingTokenStore();
    LocalDelegationTokenStore localTokenStore = new LocalDelegationTokenStore(tokenStore);
    DelegationTokenIdentifier identifier = tokenIdentifier("alice");
    DelegationTokenInformation information = new DelegationTokenInformation(1234L, new byte[] {1, 2, 3});

    boolean added = localTokenStore.addToken(
        LocalDelegationTokenStore.encodeTokenIdentifier(identifier),
        LocalDelegationTokenStore.encodeDelegationTokenInformation(information));

    Assert.assertTrue(added);
    Assert.assertEquals(
        LocalDelegationTokenStore.encodeTokenIdentifier(identifier),
        LocalDelegationTokenStore.encodeTokenIdentifier(tokenStore.lastAddedIdentifier));
    Assert.assertEquals(
        LocalDelegationTokenStore.encodeDelegationTokenInformation(information),
        LocalDelegationTokenStore.encodeDelegationTokenInformation(tokenStore.lastAddedInformation));
  }

  @Test
  public void getTokenEncodesStoredTokenInformation() throws Exception {
    RecordingTokenStore tokenStore = new RecordingTokenStore();
    LocalDelegationTokenStore localTokenStore = new LocalDelegationTokenStore(tokenStore);
    DelegationTokenIdentifier identifier = tokenIdentifier("alice");
    DelegationTokenInformation information = new DelegationTokenInformation(5678L, new byte[] {4, 5, 6});
    tokenStore.nextToken = information;

    String token = localTokenStore.getToken(LocalDelegationTokenStore.encodeTokenIdentifier(identifier));

    Assert.assertEquals(
        LocalDelegationTokenStore.encodeDelegationTokenInformation(information),
        token);
  }

  @Test
  public void getTokenReturnsEmptyStringWhenTokenDoesNotExist() throws Exception {
    RecordingTokenStore tokenStore = new RecordingTokenStore();
    LocalDelegationTokenStore localTokenStore = new LocalDelegationTokenStore(tokenStore);

    String token = localTokenStore.getToken(LocalDelegationTokenStore.encodeTokenIdentifier(tokenIdentifier("alice")));

    Assert.assertEquals("", token);
  }

  @Test
  public void getAllTokenIdentifiersEncodesUnderlyingIdentifiers() throws Exception {
    RecordingTokenStore tokenStore = new RecordingTokenStore();
    LocalDelegationTokenStore localTokenStore = new LocalDelegationTokenStore(tokenStore);
    DelegationTokenIdentifier first = tokenIdentifier("alice");
    DelegationTokenIdentifier second = tokenIdentifier("bob");
    tokenStore.allIdentifiers = List.of(first, second);

    List<String> identifiers = localTokenStore.getAllTokenIdentifiers();

    Assert.assertEquals(
        List.of(
            LocalDelegationTokenStore.encodeTokenIdentifier(first),
            LocalDelegationTokenStore.encodeTokenIdentifier(second)),
        identifiers);
  }

  @Test
  public void masterKeyOperationsPassThrough() throws Exception {
    RecordingTokenStore tokenStore = new RecordingTokenStore();
    LocalDelegationTokenStore localTokenStore = new LocalDelegationTokenStore(tokenStore);
    tokenStore.nextMasterKeyId = 42;
    tokenStore.masterKeys = new String[] {"k1", "k2"};
    tokenStore.removeMasterKeyResult = true;

    Assert.assertEquals(42, localTokenStore.addMasterKey("key"));
    localTokenStore.updateMasterKey(42, "updated");
    Assert.assertTrue(localTokenStore.removeMasterKey(42));
    Assert.assertEquals(List.of("k1", "k2"), localTokenStore.getMasterKeys());
    Assert.assertEquals(42, tokenStore.updatedMasterKeySeq);
    Assert.assertEquals("updated", tokenStore.updatedMasterKeyValue);
  }

  private static DelegationTokenIdentifier tokenIdentifier(String owner) throws IOException {
    return new DelegationTokenIdentifier(
        new org.apache.hadoop.io.Text(owner),
        new org.apache.hadoop.io.Text("hive"),
        new org.apache.hadoop.io.Text(owner));
  }

  private static final class RecordingTokenStore implements DelegationTokenStore {
    private DelegationTokenIdentifier lastAddedIdentifier;
    private DelegationTokenInformation lastAddedInformation;
    private DelegationTokenInformation nextToken;
    private List<DelegationTokenIdentifier> allIdentifiers = new ArrayList<>();
    private int nextMasterKeyId;
    private int updatedMasterKeySeq = -1;
    private String updatedMasterKeyValue;
    private boolean removeMasterKeyResult;
    private String[] masterKeys = new String[0];

    @Override
    public int addMasterKey(String s) {
      return nextMasterKeyId;
    }

    @Override
    public void updateMasterKey(int keySeq, String s) {
      updatedMasterKeySeq = keySeq;
      updatedMasterKeyValue = s;
    }

    @Override
    public boolean removeMasterKey(int keySeq) {
      return removeMasterKeyResult;
    }

    @Override
    public String[] getMasterKeys() {
      return masterKeys;
    }

    @Override
    public boolean addToken(DelegationTokenIdentifier tokenIdentifier, DelegationTokenInformation token) {
      lastAddedIdentifier = tokenIdentifier;
      lastAddedInformation = token;
      return true;
    }

    @Override
    public DelegationTokenInformation getToken(DelegationTokenIdentifier tokenIdentifier) {
      return nextToken;
    }

    @Override
    public boolean removeToken(DelegationTokenIdentifier tokenIdentifier) {
      return true;
    }

    @Override
    public List<DelegationTokenIdentifier> getAllDelegationTokenIdentifiers() {
      return allIdentifiers;
    }

    @Override
    public void init(Object handler, HadoopThriftAuthBridge.Server.ServerMode serverMode) {
    }

    @Override
    public void close() {
    }

    @Override
    public void setConf(Configuration conf) {
    }

    @Override
    public Configuration getConf() {
      return new Configuration(false);
    }
  }
}
