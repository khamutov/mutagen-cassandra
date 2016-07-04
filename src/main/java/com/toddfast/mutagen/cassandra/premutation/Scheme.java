package com.toddfast.mutagen.cassandra.premutation;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class Scheme {
    private String keyspace;
    private Set<Record> records = new HashSet<>();

    private Scheme() {
    }

    private Scheme(String keyspace) {
        this.keyspace = keyspace;
    }

    private Scheme(String keyspace, Collection<Record> records) {
        this(keyspace);
        this.records = new HashSet<>(records);
    }

    private Scheme(Collection<Record> records) {
        this.records = new HashSet<>(records);
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public Scheme addRecord(Record record) {
        records.add(record);
        return this;
    }

    public Scheme addRecords(Collection<Record> record) {
        records.addAll(record);
        return this;
    }

    public static Scheme instance() {
        return new Scheme();
    }

    public static Scheme instance(String keyspace) {
        return new Scheme(keyspace);
    }

    public static Scheme instance(Collection<Record> records) {
        return new Scheme(records);
    }

    public static Scheme instance(String keyspace, Collection<Record> records) {
        return new Scheme(keyspace, records);
    }

    public Scheme addRecords(Record... records) {
        return addRecords(Arrays.asList(records));
    }

    public Set<Record> getRecords() {
        return records;
    }

    public String getKeyspace() {
        return keyspace;
    }

}
