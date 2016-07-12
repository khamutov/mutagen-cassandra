package com.toddfast.mutagen.cassandra.hash;

import com.toddfast.mutagen.cassandra.table.SchemaVersion;

import java.util.function.Supplier;

public class SchemaVersionHashSupplier implements Supplier<byte[]> {
    private SchemaVersion schemaVersion;

    public SchemaVersionHashSupplier(SchemaVersion schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    @Override
    public byte[] get() {
        if (!schemaVersion.getColumn1().equals("hash")) {
            throw new IllegalStateException("Provided schema version object does not correspond to hash.");
        }
        return schemaVersion.getValue().array();
    }

}
