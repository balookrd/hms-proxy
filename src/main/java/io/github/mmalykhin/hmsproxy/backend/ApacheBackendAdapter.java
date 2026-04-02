package io.github.mmalykhin.hmsproxy.backend;

import io.github.mmalykhin.hmsproxy.compatibility.MetastoreRuntimeProfile;

public final class ApacheBackendAdapter extends AbstractBackendAdapter {
  public ApacheBackendAdapter() {
    super(MetastoreRuntimeProfile.APACHE_3_1_3);
  }
}
