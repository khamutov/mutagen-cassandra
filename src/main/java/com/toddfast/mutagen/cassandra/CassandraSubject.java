package com.toddfast.mutagen.cassandra;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;
import com.toddfast.mutagen.basic.SimpleState;
import com.toddfast.mutagen.cassandra.dao.SchemaVersionDao;
import com.toddfast.mutagen.cassandra.impl.CassandraMutagenConfig;
import com.toddfast.mutagen.cassandra.table.SchemaConstants;
import com.toddfast.mutagen.cassandra.table.SchemaVersion;

import java.nio.ByteBuffer;

/**
 * @author Todd Fast, Aleksandr Khamutov
 */
public class CassandraSubject implements Subject<Integer> {

    private Session session;
    private Mapper<SchemaVersion> schemaVersionMapper;

    private String tableCreationQuery = "CREATE TABLE IF NOT EXISTS %s.%s(key text, column1 text, value blob, PRIMARY KEY(key, column1));";

    private SchemaVersionDao schemaVersionDao;

    public CassandraSubject(Session session, SchemaVersionDao schemaVersionDao) {
        this.session = session;
        this.schemaVersionDao = schemaVersionDao;
        this.schemaVersionMapper = new MappingManager(session).mapper(SchemaVersion.class);
        tableCreationQuery = String.format(tableCreationQuery, session.getLoggedKeyspace(), SchemaConstants.TABLE_SCHEMA_VERSION);
    }

    /**
     *
     *
     */
    private void createSchemaVersionTable() {
        session.execute(tableCreationQuery);
    }

    /**
     *
     *
     */
    @Override
    public State<Integer> getCurrentState() {

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
