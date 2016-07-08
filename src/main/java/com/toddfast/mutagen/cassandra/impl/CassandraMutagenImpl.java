package com.toddfast.mutagen.cassandra.impl;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.Planner;
import com.toddfast.mutagen.basic.ResourceScanner;
import com.toddfast.mutagen.cassandra.CassandraCoordinator;
import com.toddfast.mutagen.cassandra.CassandraMutagen;
import com.toddfast.mutagen.cassandra.CassandraSubject;
import com.toddfast.mutagen.cassandra.dao.SchemaVersionDao;
import com.toddfast.mutagen.cassandra.hash.ActionHashChecker;
import com.toddfast.mutagen.cassandra.premutation.Premutation;
import com.toddfast.mutagen.cassandra.premutation.PremutationProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author Todd Fast
 */
public class CassandraMutagenImpl implements CassandraMutagen {

    private static final Logger log = LoggerFactory.getLogger(CassandraMutagenImpl.class);

    private CassandraSubject subject;
    private CassandraCoordinator coordinator;
    private SchemaVersionDao schemaVersionDao;
    private SessionHolder sessionHolder;
    private CassandraMutagenConfig config;
    private List<String> mutationResources = new ArrayList<>();
    private List<String> premutationResources = new ArrayList<>();
    private List<String> rawMutationResources = new ArrayList<>();

    public CassandraMutagenImpl(Session session, CassandraMutagenConfig config) {
        if (Objects.isNull(session.getLoggedKeyspace())) {
            throw new IllegalArgumentException("Session must be started within keyspace.");
        }
        this.sessionHolder = new SessionHolder(session);
        this.config = config.copy();
        this.coordinator = new CassandraCoordinator(this.config);
        this.schemaVersionDao = new SchemaVersionDao(sessionHolder);
        this.subject = new CassandraSubject(sessionHolder, schemaVersionDao);
    }

    public CassandraMutagenImpl(Session session) {
        this(session, new CassandraMutagenConfig());
    }

    public void initialize(String mutationResourcePath, String premutationResourcePath, String rawMutationsPath)
        throws IOException {
        this.mutationResources = locateSources(mutationResourcePath).stream()
                                                                    .map(s -> s.substring(s.indexOf(mutationResourcePath)))
                                                                    .collect(Collectors.toList());
        if (config.premutationsEnabled()) {
            this.premutationResources = locateSources(premutationResourcePath).stream()
                                                                              .map(s -> s.substring(s.indexOf(premutationResourcePath)))
                                                                              .collect(Collectors.toList());
        }
        if (config.getChangedMutationAction() != null) {
            this.rawMutationResources = locateSources(rawMutationsPath);
        }
    }

    private List<String> locateSources(String path) {
        ResourceScanner resourceScanner = ResourceScanner.getInstance();
        List<String> mutations = new ArrayList<>();
        List<String> mutationResources;
        try {
            mutationResources = resourceScanner.getResources(path, Pattern.compile(".*"), getClass().getClassLoader());
        } catch (IOException | URISyntaxException e) {
            throw new IllegalArgumentException("Could not find mutationResources on " +
                                                   "path \"" + path + "\"", e);
        }
        for (String resource : mutationResources) {
            log.info("Found mutation resource {}", resource);
            if (resource.contains("$")) {
                continue;
            }
            mutations.add(resource);
        }
        return mutations;
    }

    @Override
    public MutationResult<Integer> mutate() {
        synchronized (System.class) {
            UnaryOperator<Session> hashAction = config.getChangedMutationAction();
            ActionHashChecker hashChecker = new ActionHashChecker(sessionHolder, schemaVersionDao, rawMutationResources);
            if (hashAction != null && !hashChecker.tryDrop(hashAction)) {
                return MutationResult.empty();
            }
            List<Mutation<Integer>> mutations = CassandraPlanner.loadMutations(sessionHolder, schemaVersionDao, config, mutationResources);
            List<Premutation> premutations = PremutationProcessor.loadPremutations(sessionHolder, premutationResources);
            Planner<Integer> planner = new CassandraPlanner(mutations, premutations, sessionHolder, config);
            Plan<Integer> plan = planner.getPlan(subject, coordinator);
            // Execute the plan
            return (MutationResult<Integer>) plan.execute();
        }
    }

}
