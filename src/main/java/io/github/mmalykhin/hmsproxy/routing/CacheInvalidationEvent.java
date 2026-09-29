package io.github.mmalykhin.hmsproxy.routing;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Immutable event payload for distributed DDL cache invalidations.
 */
record CacheInvalidationEvent(
    Type type,
    String catalogName,
    String backendDbName,
    String tableName,
    String originInstanceId,
    long timestampMs
) {
  static final int SERIALIZATION_VERSION = 1;

  enum Type {
    TABLE((byte) 1),
    DATABASE((byte) 2),
    CATALOG((byte) 3),
    ALL((byte) 4);

    private final byte code;

    Type(byte code) {
      this.code = code;
    }

    byte code() {
      return code;
    }

    static Type fromCode(byte code) {
      for (Type t : values()) {
        if (t.code == code) {
          return t;
        }
      }
      throw new IllegalArgumentException("Unknown CacheInvalidationEvent.Type code: " + code);
    }
  }

  byte[] serialize() throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(SERIALIZATION_VERSION);
      out.writeByte(type.code());
      writeNullableString(out, catalogName);
      writeNullableString(out, backendDbName);
      writeNullableString(out, tableName);
      writeNullableString(out, originInstanceId);
      out.writeLong(timestampMs);
    }
    return bytes.toByteArray();
  }

  static CacheInvalidationEvent deserialize(byte[] data) throws IOException {
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(data))) {
      int version = in.readInt();
      if (version != SERIALIZATION_VERSION) {
        throw new IOException("Unsupported CacheInvalidationEvent version: " + version);
      }
      Type type = Type.fromCode(in.readByte());
      String catalogName = readNullableString(in);
      String backendDbName = readNullableString(in);
      String tableName = readNullableString(in);
      String originInstanceId = readNullableString(in);
      long timestampMs = in.readLong();
      return new CacheInvalidationEvent(type, catalogName, backendDbName, tableName, originInstanceId, timestampMs);
    }
  }

  private static void writeNullableString(DataOutputStream out, String value) throws IOException {
    if (value == null) {
      out.writeInt(-1);
      return;
    }
    byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
    out.writeInt(encoded.length);
    out.write(encoded);
  }

  private static String readNullableString(DataInputStream in) throws IOException {
    int length = in.readInt();
    if (length == -1) {
      return null;
    }
    byte[] encoded = in.readNBytes(length);
    if (encoded.length != length) {
      throw new IOException("Unexpected EOF while reading string field in CacheInvalidationEvent");
    }
    return new String(encoded, StandardCharsets.UTF_8);
  }
}
