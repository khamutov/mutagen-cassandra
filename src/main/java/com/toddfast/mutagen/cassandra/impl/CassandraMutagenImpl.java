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
import com.toddfast.mutagen.cassandra.hash.HashChecker;
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
        this.mutationResources = locateSources(mutationResourcePath);
        this.mutationResources.forEach(res -> log.info("Found mutation resource {}", res));
        if (config.premutationsEnabled()) {
            this.premutationResources = locateSources(premutationResourcePath);
            this.premutationResources.forEach(res -> log.info("Found premutation resource {}", res));
        }
        this.rawMutationResources = locateSources(rawMutationsPath).stream()
                                                                   .filter(p -> p.endsWith(".cql") || p.endsWith(".java"))
                                                                   .collect(Collectors.toList());
        this.rawMutationResources.forEach(res -> log.info("Found hash resource {}", res));
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
            resource = resource.substring(resource.indexOf(path));
            if (resource.contains("$")) {
                continue;
            }
            mutations.add(resource);
        }
        return mutations;
    }

    @Override
    @SuppressWarnings("unchecked")
    public MutationResult<Integer> mutate() {
        synchronized (System.class) {
            HashChecker hashChecker = HashChecker.build(rawMutationResources, schemaVersionDao);
            if (!hashChecker.isValid()) {
                log.warn("Hashes of mutations differ. Action will be performed prior to mutations if it was stated.");
                UnaryOperator<Session> hashAction = config.getChangedMutationAction();
                if (hashAction != null) {
                    hashAction.apply(sessionHolder.get());
                }
                return (MutationResult<Integer>) getPlan().execute();
            }
            if (hashChecker.hasNewMutations()
                || config.getMode() == CassandraMutagenConfig.Mode.FORCE
                || config.getMode() == CassandraMutagenConfig.Mode.FORCE_RANGE
                || config.getMode() == CassandraMutagenConfig.Mode.FORCE_VERSION) {
                return (MutationResult<Integer>) getPlan().execute();
            }
            return MutationResult.empty();
        }
    }

    private Plan getPlan() {
        List<Mutation<Integer>> mutations = CassandraPlanner.loadMutations(sessionHolder, schemaVersionDao, config, mutationResources);
        List<Premutation> premutations = PremutationProcessor.loadPremutations(sessionHolder, premutationResources);
        Planner<Integer> planner = new CassandraPlanner(mutations, premutations, sessionHolder, config);
        return planner.getPlan(subject, coordinator);
    }

}
