package io.github.mmalykhin.hmsproxy;

enum MetastoreRuntimeProfile {
  APACHE_3_1_3("Apache Hive Metastore", "3.1.3", "hive-metastore/hive-standalone-metastore-3.1.3.jar"),
  HORTONWORKS_3_1_0_3_1_0_78(
      "Hortonworks Hive Metastore",
      "3.1.0.3.1.0.0-78",
      "hive-metastore/hive-standalone-metastore-3.1.0.3.1.0.0-78.jar");

  private final String displayName;
  private final String metastoreVersion;
  private final String defaultStandaloneMetastoreJar;

  MetastoreRuntimeProfile(String displayName, String metastoreVersion, String defaultStandaloneMetastoreJar) {
    this.displayName = displayName;
    this.metastoreVersion = metastoreVersion;
    this.defaultStandaloneMetastoreJar = defaultStandaloneMetastoreJar;
  }

  String displayName() {
    return displayName;
  }

  String metastoreVersion() {
    return metastoreVersion;
  }

  String defaultStandaloneMetastoreJar() {
    return defaultStandaloneMetastoreJar;
  }
}
