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
    private boolean skipped;


    public MutationResult(Plan<I> plan, List<Mutation<I>> completedMutations,
                          List<Mutation<I>> remainingMutations, State<I> lastState, MutagenException exception) {
        this.plan = plan;
        this.completedMutations = completedMutations;
        this.remainingMutations = remainingMutations;
        this.lastState = lastState;
        this.exception = exception;
    }

    public MutationResult(boolean skipped) {
        this.skipped = skipped;
    }

    @Override
    public Plan<I> getPlan() {
        return plan;
    }

    @Override
    public boolean isMutationComplete() {
        return skipped || remainingMutations.isEmpty();
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

    public boolean isSkipped() {
        return skipped;
    }

    public static <I extends Comparable<I>> MutationResult<I> empty() {
        return new MutationResult<>(true);
    }
}
