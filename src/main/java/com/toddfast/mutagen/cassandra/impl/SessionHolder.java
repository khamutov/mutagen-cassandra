package com.toddfast.mutagen.cassandra.impl;

import com.datastax.driver.core.Session;

public class SessionHolder {
    private Session session;

    public SessionHolder(Session session) {
        this.session = session;
    }

    public Session get() {
        return session;
    }

    public void set(Session session) {
        this.session = session;
    }
}
