package com.toddfast.mutagen.cassandra.impl;

import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.State;

import java.util.List;

public class MutationResult<I extends Comparable<I>> implements Plan.Result<I> {

    private Plan<I> plan;
    private List<Mutation<I>> completedMutations;
    private List<Mutation<I>> remainingMutations;
    private State<I> lastState;
    private MutagenException exception;


    public MutationResult(Plan<I> plan, List<Mutation<I>> completedMutations,
                          List<Mutation<I>> remainingMutations, State<I> lastState, MutagenException exception) {
        this.plan = plan;
        this.completedMutations = completedMutations;
        this.remainingMutations = remainingMutations;
        this.lastState = lastState;
        this.exception = exception;
    }

    @Override
    public Plan<I> getPlan() {
        return plan;
    }

    @Override
    public boolean isMutationComplete() {
        return remainingMutations.isEmpty();
    }

    @SuppressWarnings("unchecked")
    @Override
    public State<I> getLastState() {
        return lastState;
    }

    @Override
    public List<Mutation<I>> getCompletedMutations() {
        return completedMutations;
    }

    @Override
    public List<Mutation<I>> getRemainingMutations() {
        return remainingMutations;
    }

    @Override
    public MutagenException getException() {
        return exception;
    }
}
