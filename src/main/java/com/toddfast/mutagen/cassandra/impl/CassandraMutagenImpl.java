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
import com.toddfast.mutagen.cassandra.premutation.Premutation;
import com.toddfast.mutagen.cassandra.premutation.PremutationProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * @author Todd Fast
 */
public class CassandraMutagenImpl implements CassandraMutagen {

    private static final Logger log = LoggerFactory.getLogger(CassandraMutagenImpl.class);

    private CassandraSubject subject;
    private CassandraCoordinator coordinator;
    private SchemaVersionDao schemaVersionDao;
    private Session session;
    private CassandraMutagenConfig config;
    private List<String> mutationResources = new ArrayList<>();
    private List<String> premutationResources = new ArrayList<>();

    public CassandraMutagenImpl(Session session, CassandraMutagenConfig config) {
        if (Objects.isNull(session.getLoggedKeyspace())) {
            throw new IllegalArgumentException("Session must be started within keyspace.");
        }
        this.session = session;
        this.config = config.copy();
        this.coordinator = new CassandraCoordinator(this.config);
        this.schemaVersionDao = new SchemaVersionDao(session);
        this.subject = new CassandraSubject(session, schemaVersionDao);
    }

    public CassandraMutagenImpl(Session session) {
        this(session, new CassandraMutagenConfig());
    }

    /**
     *
     *
     */
    @Override
    public void initialize(String mutationResourcePath, String premutationResourcePath)
        throws IOException {

        try {
            ResourceScanner resourceScanner = ResourceScanner.getInstance();
            List<String> mutationResources =
                resourceScanner.getResources(mutationResourcePath, Pattern.compile(".*"), getClass().getClassLoader());
            List<String> premutationResources = resourceScanner.getResources(premutationResourcePath, Pattern.compile(".*"),
                                                                             getClass().getClassLoader());

            // Make sure we found some mutationResources
            if (mutationResources.isEmpty()) {
                throw new IllegalArgumentException("Could not find mutationResources " +
                                                       "on path \"" + mutationResourcePath + "\"");
            }

            Collections.sort(mutationResources, COMPARATOR);

            for (String resource : mutationResources) {
                log.info("Found mutation resource {}", resource);

                if (resource.endsWith(".class")) {
                    // Remove the file path for mutations
                    resource = resource.substring(
                        resource.indexOf(mutationResourcePath));

                    if (resource.contains("$")) {
                        // skip inner classes
                        continue;
                    }
                }
                this.mutationResources.add(resource);
            }

            for (String resource : premutationResources) {
                log.info("Found premutation resource {}", resource);
                resource = resource.substring(
                    resource.indexOf(premutationResourcePath));

                if (resource.contains("$")) {
                    // skip inner classes
                    continue;
                }
                this.premutationResources.add(resource);
            }
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Could not find mutationResources on " +
                                                   "path \"" + mutationResourcePath + "\"", e);
        }
    }


    /**
     *
     *
     */
    public List<String> getMutationResources() {
        return mutationResources;
    }

    public List<String> getPremutationResources() {
        return premutationResources;
    }

    /**
     *
     *
     */
    @Override
    public Plan.Result<Integer> mutate() {
        // Do this in a VM-wide critical section. External cluster-wide
        // synchronization is going to have to happen in the coordinator.
        synchronized (System.class) {
            List<Mutation<Integer>> mutations = CassandraPlanner.loadMutations(session, schemaVersionDao, config, mutationResources);
            List<Premutation> premutations = PremutationProcessor.loadPremutations(session, premutationResources);
            Planner<Integer> planner = new CassandraPlanner(mutations, premutations, session, config);
            Plan<Integer> plan = planner.getPlan(subject, coordinator);
            // Execute the plan
            return plan.execute();
        }
    }


    ////////////////////////////////////////////////////////////////////////////
    // Fields
    ////////////////////////////////////////////////////////////////////////////

    /**
     * Sorts by root file name, ignoring path and file extension
     */
    private static final Comparator<String> COMPARATOR =
        (path1, path2) -> {

            try {

                int index1 = path1.lastIndexOf("/");
                int index2 = path2.lastIndexOf("/");

                String file1;
                if (index1 != -1) {
                    file1 = path1.substring(index1 + 1);
                } else {
                    file1 = path1;
                }

                String file2;
                if (index2 != -1) {
                    file2 = path2.substring(index2 + 1);
                } else {
                    file2 = path2;
                }

                index1 = file1.lastIndexOf(".");
                index2 = file2.lastIndexOf(".");

                if (index1 > 1) {
                    file1 = file1.substring(0, index1);
                }

                if (index2 > 1) {
                    file2 = file2.substring(0, index2);
                }

                return file1.compareTo(file2);
            } catch (StringIndexOutOfBoundsException e) {
                throw new StringIndexOutOfBoundsException(e.getMessage() +
                                                              " (path1: \"" + path1 +
                                                              "\", path2: \"" + path2 + "\")");
            }
        };
}
