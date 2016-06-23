package com.toddfast.mutagen.cassandra.premutation;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class Scheme {
    private String keyspace;
    private Set<Table> tables = new HashSet<>();

    private Scheme() {
    }

    private Scheme(String keyspace) {
        this.keyspace = keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public Scheme addTable(Table table) {
        tables.add(table);
        return this;
    }

    public Scheme addTables(Collection<Table> table) {
        tables.addAll(table);
        return this;
    }

    public static Scheme instance() {
        return new Scheme();
    }

    public static Scheme instance(String keyspace) {
        return new Scheme(keyspace);
    }

    public Scheme addTables(Table... tables) {
        return addTables(Arrays.asList(tables));
    }

    public Set<Table> getTables() {
        return tables;
    }

    public String getKeyspace() {
        return keyspace;
    }

}
