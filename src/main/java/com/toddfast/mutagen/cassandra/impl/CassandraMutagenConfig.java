package com.toddfast.mutagen.cassandra.impl;

/**
 * @author alexander.polischuk@tobox.com
 * @since 15/01/16.
 */
public class CassandraMutagenConfig {
    private Integer mutation = null;
    private Mode mode = Mode.STANDARD;

    public CassandraMutagenConfig(Integer mutation, Mode mode) {
        this.mutation = mutation;
        this.mode = mode;
    }

    public CassandraMutagenConfig() {
    }

    public CassandraMutagenConfig normal() {
        this.mode = Mode.STANDARD;
        return this;
    }

    public CassandraMutagenConfig forceMutation(int mutation) {
        this.mode = Mode.FORCE;
        this.mutation = mutation;
        return this;
    }

    public CassandraMutagenConfig forceVersion(int mutation) {
        this.mode = Mode.FORCE_VERSION;
        this.mutation = mutation;
        return this;
    }

    public Integer getMutation() {
        return mutation;
    }

    public Mode getMode() {
        return mode;
    }

    public CassandraMutagenConfig copy() {
        return new CassandraMutagenConfig(mutation, mode);
    }

    public enum Mode {
        STANDARD,
        FORCE,
        FORCE_VERSION
    }
}
