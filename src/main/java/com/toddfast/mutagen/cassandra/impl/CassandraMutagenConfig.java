package com.toddfast.mutagen.cassandra.impl;

import com.datastax.driver.core.Session;

import java.util.function.UnaryOperator;

/**
 * @author alexander.polischuk@tobox.com
 * @since 15/01/16.
 */
public class CassandraMutagenConfig {
    private Integer mutation = null;
    private Mode mode = Mode.STANDARD;

    private Integer start;
    private Integer end;
    private boolean enablePremutations;
    private UnaryOperator<Session> action;

    public CassandraMutagenConfig(Integer mutation, Mode mode, Integer start,
                                  Integer end, boolean enablePremutations, UnaryOperator<Session> action) {
        this.mutation = mutation;
        this.mode = mode;
        this.start = start;
        this.end = end;
        this.enablePremutations = enablePremutations;
        this.action = action;
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

    public CassandraMutagenConfig withActionIfMutationChanged(UnaryOperator<Session> action) {
        this.action = action;
        return this;
    }

    public CassandraMutagenConfig enablePremutations() {
        this.enablePremutations = true;
        return this;
    }

    public CassandraMutagenConfig forceRangeMutation(int start, int end) {
        if (start >= end) {
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

    public boolean premutationsEnabled() {
        return enablePremutations;
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

    public UnaryOperator<Session> getChangedMutationAction() {
        return action;
    }

    public CassandraMutagenConfig copy() {
        return new CassandraMutagenConfig(mutation, mode, start, end, enablePremutations, action);
    }

    public enum Mode {
        STANDARD,
        FORCE,
        FORCE_RANGE,
        FORCE_VERSION
    }
}
