package com.toddfast.mutagen.cassandra.premutation;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.cassandra.impl.SessionHolder;

public abstract class Premutation {

    private int mutationNumber;
    private SessionHolder sessionHolder;

    public Premutation(SessionHolder sessionHolder) {
        this.sessionHolder = sessionHolder;
    }

    protected void setMutationNumber(int mutationNumber) {
        this.mutationNumber = mutationNumber;
    }

    public int getMutationNumber() {
        return mutationNumber;
    }

    /**
     * Use this method to form scheme to insert
     * before java based class mutation
     *
     * @return formed scheme
     */
    public abstract Scheme formScheme();

    /**
     * Evaluates after java based mutation and
     * before truncation. Check logical dao state
     * in this method.
     * @throws CheckStateException should be thrown
     * in case data model is corrupted after mutation
     */
    public abstract void check() throws CheckStateException;

    public Session getSession() {
        return sessionHolder.get();
    }
}
