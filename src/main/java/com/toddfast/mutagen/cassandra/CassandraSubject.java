package com.toddfast.mutagen.cassandra;

import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;
import com.toddfast.mutagen.basic.SimpleState;
import com.toddfast.mutagen.cassandra.dao.SchemaVersionDao;
import com.toddfast.mutagen.cassandra.impl.SessionHolder;
import com.toddfast.mutagen.cassandra.table.SchemaConstants;
import com.toddfast.mutagen.cassandra.table.SchemaVersion;

import java.nio.ByteBuffer;

/**
 * @author Todd Fast, Aleksandr Khamutov
 */
public class CassandraSubject implements Subject<Integer> {

    private SessionHolder sessionHolder;

    private String tableCreationQuery = "CREATE TABLE IF NOT EXISTS %s.%s(key text, column1 text, value blob, PRIMARY KEY(key, column1));";

    private SchemaVersionDao schemaVersionDao;

    public CassandraSubject(SessionHolder sessionHolder, SchemaVersionDao schemaVersionDao) {
        this.sessionHolder = sessionHolder;
        createSchemaVersionTable();
        this.schemaVersionDao = schemaVersionDao;

    }

    private void createSchemaVersionTable() {
        sessionHolder.get().execute(String.format(tableCreationQuery, sessionHolder.get().getLoggedKeyspace(), SchemaConstants.TABLE_SCHEMA_VERSION));
    }

    @Override
    public State<Integer> getCurrentState() {

        Mapper<SchemaVersion> schemaVersionMapper = new MappingManager(sessionHolder.get()).mapper(SchemaVersion.class);

        TableMetadata tableMetadata = schemaVersionMapper.getTableMetadata();

        if (tableMetadata == null) {
            createSchemaVersionTable();
        }

        SchemaVersion schemaVersions = schemaVersionDao.findLastVersion();

        // Most likely the column family has only just been created
        Integer version = 0;
        if (schemaVersions != null) {
            ByteBuffer value = schemaVersions.getValue();
            if(value != null) {
                version = value.getInt();
            }
        }
        return new SimpleState<>(version);
    }
}
