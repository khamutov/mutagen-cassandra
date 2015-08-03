package com.toddfast.mutagen.cassandra;

import com.datastax.driver.core.TableMetadata;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;
import com.toddfast.mutagen.basic.SimpleState;
import com.toddfast.mutagen.cassandra.dao.SchemaVersionDao;
import com.toddfast.mutagen.cassandra.table.SchemaConstants;
import com.toddfast.mutagen.cassandra.table.SchemaVersion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;

/**
 *
 * @author Todd Fast, Aleksandr Khamutov
 */
@Component
public class CassandraSubject implements Subject<Integer> {

    @Autowired
    private CassandraAdminOperations cassandraOperations;

    @Autowired
    private String cassandraKeyspace;

    @Autowired
    private SchemaVersionDao schemaVersionDao;

	/**
	 *
	 *
	 */
	public CassandraSubject() {
	}


	/**
	 *
	 *
	 */
	private void createSchemaVersionTable() {
        cassandraOperations.createTable(true, new CqlIdentifier(SchemaConstants.TABLE_SCHEMA_VERSION), SchemaVersion.class, null);
	}


	/**
	 * 
	 * 
	 */
	@Override
	public State<Integer> getCurrentState() {

        CqlIdentifier identifier = new CqlIdentifier(SchemaConstants.TABLE_SCHEMA_VERSION);
        TableMetadata tableMetadata = cassandraOperations.getTableMetadata(cassandraKeyspace, identifier);

        if (tableMetadata == null) {
            createSchemaVersionTable();
        }

        SchemaVersion schemaVersions = schemaVersionDao.findLastVersion();

        // Most likely the column family has only just been created
        Integer version = 0;
        if (schemaVersions != null) {
            ByteBuffer value = schemaVersions.getValue();
            version = value.getInt();
        }
        return new SimpleState<>(version);
	}
}
