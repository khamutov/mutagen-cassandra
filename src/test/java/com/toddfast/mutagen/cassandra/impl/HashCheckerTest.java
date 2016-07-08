package com.toddfast.mutagen.cassandra.impl;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.toddfast.mutagen.basic.ResourceScanner;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

public class HashCheckerTest extends MutagenBaseTest {

    private static final UnaryOperator<Session> ACTION = s -> {
        s.execute("DROP KEYSPACE " + TEST_KEYSPACE + ";");
        Cluster cluster = s.getCluster();
        Session rawSession = cluster.connect();
        rawSession.execute("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        return cluster.connect(TEST_KEYSPACE);
    };

    @Test
    public void shouldSkipMutationsOnEqualHashes() throws IOException {
        MutationResult<Integer> result = mutate(config());
        Assert.assertTrue(result.isMutationComplete());
        Assert.assertFalse(result.isSkipped());

        result = mutate(config().withActionIfMutationChanged(ACTION));

        Assert.assertTrue(result.isMutationComplete());
        Assert.assertTrue(result.isSkipped());
    }

    @Test
    public void shouldPerformActionOnChangeFound() throws IOException, URISyntaxException {
        MutationResult<Integer> result = mutate(config());
        Assert.assertTrue(result.isMutationComplete());
        Assert.assertFalse(result.isSkipped());

        Path path = Paths.get(ResourceScanner.getInstance()
                                             .getResources("com/toddfast/mutagen/cassandra/test/mutations",
                                                           Pattern.compile(".*"), getClass().getClassLoader())
                                             .get(0));

        try {
            Files.write(path, "--trash comment".getBytes(), StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        result = mutate(config().withActionIfMutationChanged(ACTION));
        Assert.assertTrue(result.isMutationComplete());
        Assert.assertFalse(result.isSkipped());
    }

    @Test
    public void shouldPerformActionOnMissingMutationInDb() throws IOException {
        String pathWithMissingMutation = "com/toddfast/mutagen/cassandra/test/missing";
        //mutate skipping mutation #3
        MutationResult<Integer> result = mutate(pathWithMissingMutation, PREMUTATIONS,
                                                pathWithMissingMutation, config());
        Assert.assertTrue(result.getLastState().getID() == 5);
        //make sure mutation #3 was skipped
        Assert.assertFalse(result.getCompletedMutations()
                                 .stream()
                                 .anyMatch(m -> m.getResultingState().getID() == 3));

        //run full mutation
        result = mutate(RESOURCES, PREMUTATIONS, RAW_RESOURCES, config()
            .withActionIfMutationChanged(ACTION));

        Assert.assertFalse(result.isSkipped());
        //make sure mutation #3 was applied
        Assert.assertTrue(result.getCompletedMutations()
                                .stream()
                                .anyMatch(m -> m.getResultingState().getID() == 3));
    }

    @Override
    public CassandraMutagenConfig config() {
        return new CassandraMutagenConfig();
    }
}
