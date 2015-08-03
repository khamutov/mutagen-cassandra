package com.toddfast.mutagen.cassandra.table;

import org.springframework.cassandra.core.PrimaryKeyType;
import org.springframework.data.cassandra.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.mapping.Table;

import java.io.Serializable;
import java.nio.ByteBuffer;

@Table(SchemaConstants.TABLE_SCHEMA_VERSION)
public class SchemaVersion implements Serializable {

    @PrimaryKeyColumn(name = "key", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
    private String key;
    @PrimaryKeyColumn(name = "column1", ordinal = 1, type = PrimaryKeyType.CLUSTERED)
    private String column1;
    private ByteBuffer value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getColumn1() {
        return column1;
    }

    public void setColumn1(String column1) {
        this.column1 = column1;
    }

    public ByteBuffer getValue() {
        return value;
    }

    public void setValue(ByteBuffer value) {
        this.value = value;
    }
}
