package com.toddfast.mutagen.cassandra.impl;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.toddfast.mutagen.basic.ResourceScanner;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.regex.Pattern;

public class ActionHashCheckerTest extends MutagenBaseTest {

    @Test
    public void shouldSkipMutationsOnEqualHashes() throws IOException {
        MutationResult<Integer> result = mutate(config());
        Assert.assertTrue(result.isMutationComplete());
        Assert.assertFalse(result.isSkipped());

        result = mutate(config().withActionIfMutationChanged(s -> s));

        Assert.assertTrue(result.isMutationComplete());
        Assert.assertTrue(result.isSkipped());
    }

    @Test
    public void shouldPerformActionOnChangeFound() throws IOException, URISyntaxException {
        MutationResult<Integer> result = mutate(config());
        Assert.assertTrue(result.isMutationComplete());
        Assert.assertFalse(result.isSkipped());

        Path path = Paths.get(ResourceScanner.getInstance().getResources("com/toddfast/mutagen/cassandra/test/mutations",
                                                                     Pattern.compile(".*"), getClass().getClassLoader()).get(0));

        try {
            Files.write(path, "--trash comment".getBytes(), StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        result = mutate(config().withActionIfMutationChanged(s -> {
            s.execute("DROP KEYSPACE " + TEST_KEYSPACE + ";");
            Cluster cluster = s.getCluster();
            Session rawSession = cluster.connect();
            rawSession.execute("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
            return cluster.connect(TEST_KEYSPACE);
        }));
        Assert.assertTrue(result.isMutationComplete());
        Assert.assertFalse(result.isSkipped());
    }

    @Override
    public CassandraMutagenConfig config() {
        return new CassandraMutagenConfig();
    }
}
