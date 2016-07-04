package com.toddfast.mutagen.cassandra.premutation;

import java.util.HashMap;
import java.util.Map;

public class Record {
    private String table;
    private Map<String, Object> fields = new HashMap<>();

    private Record(String table) {
        this.table = table;
    }

    public Record value(String field, Object value) {
        fields.put(field, value);
        return this;
    }

    public String getTableName() {
        return table;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public static Record into(String tableName) {
        return new Record(tableName);
    }
}
