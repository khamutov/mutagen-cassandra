package com.toddfast.mutagen.cassandra.dao;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.toddfast.mutagen.cassandra.table.SchemaConstants;
import com.toddfast.mutagen.cassandra.table.SchemaVersion;
import org.springframework.data.cassandra.core.CassandraOperations;

import java.nio.ByteBuffer;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class SchemaVersionDao {

    private CassandraOperations cassandraOperations;

    public SchemaVersionDao(CassandraOperations cassandraOperations) {
        this.cassandraOperations = cassandraOperations;
    }

    public List<SchemaVersion> findAll() {
        Select select = QueryBuilder.select().from(SchemaConstants.TABLE_SCHEMA_VERSION);
        return cassandraOperations.select(select, SchemaVersion.class);
    }

    public SchemaVersion findLastVersion() {
        Select select = QueryBuilder.select().from(SchemaConstants.TABLE_SCHEMA_VERSION);
        select.where(eq("key", "state"));
        return cassandraOperations.selectOne(select, SchemaVersion.class);
    }

    public void add(String key, String column, ByteBuffer value) {
        SchemaVersion schemaVersion = new SchemaVersion();
        schemaVersion.setKey(key);
        schemaVersion.setColumn1(column);
        schemaVersion.setValue(value);

        cassandraOperations.insert(schemaVersion);
    }
}
