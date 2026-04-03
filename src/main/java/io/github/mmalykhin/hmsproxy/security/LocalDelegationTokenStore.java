package io.github.mmalykhin.hmsproxy.security;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.security.DelegationTokenIdentifier;
import org.apache.hadoop.hive.metastore.security.DelegationTokenSecretManager;
import org.apache.hadoop.hive.metastore.security.DelegationTokenStore;
import org.apache.hadoop.hive.metastore.security.TokenStoreDelegationTokenSecretManager;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
import org.apache.hadoop.security.token.delegation.MetastoreDelegationTokenSupport;

final class LocalDelegationTokenStore {
  private final DelegationTokenStore tokenStore;

  LocalDelegationTokenStore(DelegationTokenStore tokenStore) {
    this.tokenStore = tokenStore;
  }

  static LocalDelegationTokenStore fromSecretManager(DelegationTokenSecretManager secretManager) {
    if (secretManager == null) {
      throw new IllegalStateException("Delegation token secret manager is not initialized");
    }
    Class<?> type = secretManager.getClass();
    while (type != null) {
      try {
        Field tokenStoreField = type.getDeclaredField("tokenStore");
        tokenStoreField.setAccessible(true);
        return new LocalDelegationTokenStore((DelegationTokenStore) tokenStoreField.get(secretManager));
      } catch (NoSuchFieldException e) {
        type = type.getSuperclass();
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException("Unable to access delegation token store from secret manager", e);
      }
    }
    throw new IllegalStateException(
        "Delegation token secret manager does not expose an underlying token store: " + secretManager.getClass());
  }

  int addMasterKey(String key) throws MetaException {
    try {
      return tokenStore.addMasterKey(key);
    } catch (DelegationTokenStore.TokenStoreException e) {
      throw metaException("Unable to add delegation-token master key", e);
    }
  }

  void updateMasterKey(int keySeq, String key) throws MetaException {
    try {
      tokenStore.updateMasterKey(keySeq, key);
    } catch (DelegationTokenStore.TokenStoreException e) {
      throw metaException("Unable to update delegation-token master key " + keySeq, e);
    }
  }

  boolean removeMasterKey(int keySeq) {
    return tokenStore.removeMasterKey(keySeq);
  }

  List<String> getMasterKeys() throws MetaException {
    try {
      return Arrays.asList(tokenStore.getMasterKeys());
    } catch (DelegationTokenStore.TokenStoreException e) {
      throw metaException("Unable to list delegation-token master keys", e);
    }
  }

  boolean addToken(String tokenIdentifier, String token) throws MetaException {
    try {
      return tokenStore.addToken(
          decodeTokenIdentifier(tokenIdentifier),
          decodeDelegationTokenInformation(token));
    } catch (DelegationTokenStore.TokenStoreException | IOException e) {
      throw metaException("Unable to add delegation token", e);
    }
  }

  boolean removeToken(String tokenIdentifier) throws MetaException {
    try {
      return tokenStore.removeToken(decodeTokenIdentifier(tokenIdentifier));
    } catch (DelegationTokenStore.TokenStoreException | IOException e) {
      throw metaException("Unable to remove delegation token", e);
    }
  }

  String getToken(String tokenIdentifier) throws MetaException {
    try {
      DelegationTokenInformation info = tokenStore.getToken(decodeTokenIdentifier(tokenIdentifier));
      return info == null ? "" : encodeDelegationTokenInformation(info);
    } catch (DelegationTokenStore.TokenStoreException | IOException e) {
      throw metaException("Unable to read delegation token", e);
    }
  }

  List<String> getAllTokenIdentifiers() throws MetaException {
    try {
      List<DelegationTokenIdentifier> identifiers = tokenStore.getAllDelegationTokenIdentifiers();
      List<String> encoded = new ArrayList<>(identifiers.size());
      for (DelegationTokenIdentifier identifier : identifiers) {
        encoded.add(encodeTokenIdentifier(identifier));
      }
      return encoded;
    } catch (DelegationTokenStore.TokenStoreException | IOException e) {
      throw metaException("Unable to list delegation tokens", e);
    }
  }

  static String encodeTokenIdentifier(DelegationTokenIdentifier identifier) throws IOException {
    return TokenStoreDelegationTokenSecretManager.encodeWritable(identifier);
  }

  static DelegationTokenIdentifier decodeTokenIdentifier(String tokenIdentifier) throws IOException {
    DelegationTokenIdentifier identifier = new DelegationTokenIdentifier();
    TokenStoreDelegationTokenSecretManager.decodeWritable(identifier, tokenIdentifier);
    return identifier;
  }

  static String encodeDelegationTokenInformation(DelegationTokenInformation information) {
    return Base64.encodeBase64URLSafeString(
        MetastoreDelegationTokenSupport.encodeDelegationTokenInformation(information));
  }

  static DelegationTokenInformation decodeDelegationTokenInformation(String token) throws IOException {
    return MetastoreDelegationTokenSupport.decodeDelegationTokenInformation(Base64.decodeBase64(token));
  }

  private static MetaException metaException(String message, Exception cause) {
    MetaException metaException = new MetaException(message + ": " + cause.getMessage());
    metaException.initCause(cause);
    return metaException;
  }
}
