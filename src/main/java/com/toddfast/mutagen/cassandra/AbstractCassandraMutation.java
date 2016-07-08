package com.toddfast.mutagen.cassandra;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.basic.SimpleState;
import com.toddfast.mutagen.cassandra.dao.SchemaVersionDao;
import com.toddfast.mutagen.cassandra.impl.CassandraMutagenConfig;
import com.toddfast.mutagen.cassandra.impl.SessionHolder;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author Todd Fast
 */
public abstract class AbstractCassandraMutation implements Mutation<Integer> {

    private SchemaVersionDao schemaVersionDao;
    private CassandraMutagenConfig config;
    private SessionHolder sessionHolder;

    protected AbstractCassandraMutation(SessionHolder sessionHolder, SchemaVersionDao schemaVersionDao) {
        this.sessionHolder = sessionHolder;
        this.schemaVersionDao = schemaVersionDao;
        this.config = new CassandraMutagenConfig();
    }

    public void setConfig(CassandraMutagenConfig config) {
        this.config = config;
    }

    @Override
    public String toString() {
        if (getResultingState() != null) {
            return super.toString() + "[state=" + getResultingState().getID() + "]";
        } else {
            return super.toString();
        }
    }

    protected final State<Integer> parseVersion(String resourceName) {
        String versionString = resourceName;
        int index = versionString.lastIndexOf("/");
        if (index != -1) {
            versionString = versionString.substring(index + 1);
        }

        index = versionString.lastIndexOf(".");
        if (index != -1) {
            versionString = versionString.substring(0, index);
        }

        StringBuilder buffer = new StringBuilder();
        for (Character c : versionString.toCharArray()) {
            // Skip all initial non-digit characters
            if (!Character.isDigit(c)) {
                if (buffer.length() != 0) {
                    // End when we reach the first non-digit
                    break;
                }
            } else {
                buffer.append(c);
            }
        }

        return new SimpleState<>(Integer.parseInt(buffer.toString()));
    }


    /**
     * Perform the actual mutation
     */
    protected abstract void performMutation(Context context);


    @Override
    public abstract State<Integer> getResultingState();


    /**
     * Return byt footprint of current file defining if it has changed
     */
    public abstract byte[] getFootprint();


    /**
     * Performs the actual mutation and then updates the recorded schema version
     */
    @Override
    public final void mutate(Context context)
        throws MutagenException {

        // Perform the mutation
        if (config.getMode() == CassandraMutagenConfig.Mode.FORCE_VERSION) {
            context.info("[Force version mode] Skipping mutation [{}]", config.getMutation());
        } else {
            try {
                performMutation(context);
            } catch (DriverException e) {
                context.error("Exception executing mutation ", e);
                throw new MutagenException("Exception executing mutation ", e);
            } catch (RuntimeException e) {
                context.error("Exception executing mutation", e);
                throw e;
            }
        }

        if (config.getMode() == CassandraMutagenConfig.Mode.FORCE_RANGE) {
            context.info("[Force range mode] Skipping version insertion [{}]", config.getMutation());
        } else if (config.getMode() == CassandraMutagenConfig.Mode.FORCE) {
            context.info("[Force mode] Skipping version insertion [{}]", config.getMutation());
        } else {
            int version = getResultingState().getID();

            byte[] classFootprint = getFootprint();

            // The straightforward way, without locking
            ByteBuffer versionByteBuffer = ByteBuffer.allocate(4).putInt(version);
            versionByteBuffer.rewind();
            schemaVersionDao.add("state", "version", versionByteBuffer);
            schemaVersionDao.add(String.format(SchemaVersionDao.VERSION_FORMATTER, version), "change", ByteBuffer.wrap(String.format("Mutation number: [%s]", version)
                                                                                                 .getBytes()));
            schemaVersionDao.add(String.format(SchemaVersionDao.VERSION_FORMATTER, version), "hash", ByteBuffer.wrap(md5(classFootprint)));
        }
    }

    public static byte[] md5(byte[] input) {
        MessageDigest algorithm;
        try {
            algorithm = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }

        algorithm.reset();
        algorithm.update(input);

        return algorithm.digest();
    }

    public Session getSession() {
        return sessionHolder.get();
    }
}
