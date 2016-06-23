package com.toddfast.mutagen.cassandra.premutation;

import java.util.HashMap;
import java.util.Map;

public class Table {
    private String table;
    private Map<String, Object> fields = new HashMap<>();

    private Table(String table) {
        this.table = table;
    }

    public Table value(String field, Object value) {
        fields.put(field, value);
        return this;
    }

    public String getTableName() {
        return table;
    }

    public Map<String, Object> getFields() {
        return fields;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public static Table instance(String tableName) {
        return new Table(tableName);
    }
}
