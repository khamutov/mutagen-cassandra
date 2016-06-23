package com.toddfast.mutagen.cassandra.premutation;

import com.datastax.driver.core.Session;

public abstract class Premutation {

    private int mutationNumber;
    private Session session;

    public Premutation(Session session) {
        this.session = session;
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
        return session;
    }
}
