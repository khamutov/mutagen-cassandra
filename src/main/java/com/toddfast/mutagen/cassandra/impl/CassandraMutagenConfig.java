package com.toddfast.mutagen.cassandra.impl;

/**
 * @author alexander.polischuk@tobox.com
 * @since 15/01/16.
 */
public class CassandraMutagenConfig {
    private Integer mutation = null;
    private Mode mode = Mode.STANDARD;

    private Integer start;
    private Integer end;

    public CassandraMutagenConfig(Integer mutation, Mode mode, Integer start, Integer end) {
        this.mutation = mutation;
        this.mode = mode;
        this.start = start;
        this.end = end;
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

    public CassandraMutagenConfig forceRangeMutation(int start, int end) {
        if(start >= end) {
            throw new IllegalArgumentException("Start should be less than end! (start < end)");
        }
        this.mode = Mode.FORCE_RANGE;
        this.start = start;
        this.end = end;
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

    public Integer getStart() {
        return start;
    }

    public Integer getEnd() {
        return end;
    }

    public CassandraMutagenConfig copy() {
        return new CassandraMutagenConfig(mutation, mode, start, end);
    }

    public enum Mode {
        STANDARD,
        FORCE,
        FORCE_RANGE,
        FORCE_VERSION
    }
}
