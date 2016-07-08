package com.toddfast.mutagen.cassandra.premutation;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.cassandra.impl.SessionHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PremutationProcessor {
    private final static Logger log = LoggerFactory.getLogger(PremutationProcessor.class);
    private SessionHolder sessionHolder;
    private Premutation premutation;

    public PremutationProcessor(SessionHolder sessionHolder, Premutation premutation) {
        this.sessionHolder = sessionHolder;
        this.premutation = premutation;
    }

    public void execute() {
        log.info("Initializing scheme for [{}] mutation", premutation.getMutationNumber());
        Scheme scheme = premutation.formScheme();
        Optional<String> keyspace = Optional.ofNullable(scheme.getKeyspace());
        for (Record record : scheme.getRecords()) {
            Insert insert = QueryBuilder.insertInto(keyspace.orElse(sessionHolder.get().getLoggedKeyspace()), record.getTableName());
            for (Map.Entry<String, Object> entry : record.getFields().entrySet()) {
                insert.value(entry.getKey(), entry.getValue());
            }
            sessionHolder.get().execute(insert);
        }
    }

    public void truncate() {
        log.info("Cleaning scheme after [{}] mutation", premutation.getMutationNumber());
        Scheme scheme = premutation.formScheme();
        Optional<String> keyspace = Optional.ofNullable(scheme.getKeyspace());
        for (Record record : scheme.getRecords()) {
            sessionHolder.get().execute(QueryBuilder.truncate(keyspace.orElse(sessionHolder.get().getLoggedKeyspace()), record.getTableName()));
        }
    }

    public void check() {
        log.info("Checking scheme after [{}] mutation", premutation.getMutationNumber());
        try {
            premutation.check();
        } catch (CheckStateException ex) {
            log.info("Checking scheme after [{}] mutation has failed. Cleaning scheme...", premutation.getMutationNumber());
            truncate();
            throw ex;
        }

    }

    public static List<Premutation> loadPremutations(SessionHolder sessionHolder, List<String> resources) {
        List<Premutation> premutations = new ArrayList<>();
        resources.stream().filter(resource -> resource.endsWith(".class")).forEach(resource -> {
            int index = resource.indexOf(".class");
            String className = resource.substring(0, index).replace('/', '.');
            try {
                if (isPremutationResource(className)) {
                    premutations.add(loadPremutation(sessionHolder, className));
                }
            } catch (ClassNotFoundException e) {
                log.error("class [{}] was not found.", className);
            }
        });
        return premutations;
    }

    public static Premutation loadPremutation(SessionHolder sessionHolder, String className) {
        int mutationNumber;
        try {
            mutationNumber = Integer.valueOf(className.substring(className.lastIndexOf('.') + 2, className.indexOf('_')));
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            log.error("Malformed premutation class name. Class name must start with Vxxx_{name}", e);
            throw e;
        }
        try {
            Class<?> clazz = Class.forName(className);
            if (!isPremutationResource(className)) {
                throw new MutagenException("Class [" + className + "] doesn't inherit Premutation class");
            }
            Premutation premutation = (Premutation) clazz.getConstructor(SessionHolder.class).newInstance(sessionHolder);
            premutation.setMutationNumber(mutationNumber);
            return premutation;
        } catch (ReflectiveOperationException e) {
            log.error("Can not instantiate premutation class", e);
            throw new RuntimeException(e);
        }
    }

    public static boolean isPremutationResource(String className) throws ClassNotFoundException {
        return Premutation.class.isAssignableFrom(Class.forName(className));
    }
}
