package com.toddfast.mutagen.cassandra.premutation;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.toddfast.mutagen.cassandra.impl.SessionHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class SinglePremutationProcessor implements PremutationProcessor {
    private final static Logger log = LoggerFactory.getLogger(PremutationProcessor.class);
    private SessionHolder sessionHolder;
    private Premutation premutation;

    public SinglePremutationProcessor(SessionHolder sessionHolder, Premutation premutation) {
        this.sessionHolder = sessionHolder;
        this.premutation = premutation;
    }

    @Override
    public void execute() {
        log.info("Initializing scheme for [{}] mutation", premutation.getMutationNumber());
        Scheme scheme = premutation.formScheme();
        Optional<String> keyspace = Optional.ofNullable(scheme.getKeyspace());
        for (Record record : scheme.getRecords()) {
            Insert insert = QueryBuilder.insertInto(keyspace.orElse(sessionHolder.get()
                                                                                 .getLoggedKeyspace()), record.getTableName());
            for (Map.Entry<String, Object> entry : record.getFields().entrySet()) {
                insert.value(entry.getKey(), entry.getValue());
            }
            sessionHolder.get().execute(insert);
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

    public void truncate() {
        log.info("Cleaning scheme after [{}] mutation", premutation.getMutationNumber());
        Scheme scheme = premutation.formScheme();
        Optional<String> keyspace = Optional.ofNullable(scheme.getKeyspace());
        List<String> affectedTables = scheme.getRecords()
                                            .stream()
                                            .map(Record::getTableName)
                                            .distinct()
                                            .collect(Collectors.toList());
        for (String table : affectedTables) {
            sessionHolder.get().execute(QueryBuilder.truncate(keyspace.orElse(sessionHolder.get()
                                                                                           .getLoggedKeyspace()), table));
        }
    }
}
