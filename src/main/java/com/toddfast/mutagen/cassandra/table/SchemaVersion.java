package com.toddfast.mutagen.cassandra.table;


import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.toddfast.mutagen.cassandra.dao.SchemaVersionDao;

import java.io.Serializable;
import java.nio.ByteBuffer;

@Table(name = SchemaVersionDao.TABLE_SCHEMA_VERSION)
public class SchemaVersion implements Serializable {

    @PartitionKey
    private String key;
    @ClusteringColumn
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
