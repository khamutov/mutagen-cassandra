package com.toddfast.mutagen.cassandra.impl;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.toddfast.mutagen.Plan;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.AbstractCassandraUnit4TestCase;
import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.yaml.ClassPathYamlDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class MutagenBaseTest {
    protected static final Logger log = LoggerFactory.getLogger(CassandraMutagenImplTest.class);

    protected static final String TEST_KEYSPACE = "mutagen_test";

    protected static Cluster cluster;
    protected static Session session;

    @BeforeClass
    public static void setUpOnce() throws InterruptedException, TTransportException, ConfigurationException, IOException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra(20000);
        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9142).build();
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    @AfterClass
    public static void tearDownClass() {
        cluster.close();
        log.info("Dropped keyspace " + TEST_KEYSPACE);
    }

    @Before
    public void setUp() throws Exception {
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9142).build();
        session = cluster.connect();
        session.execute("DROP KEYSPACE IF EXISTS " + TEST_KEYSPACE + ";");
        session.execute("CREATE KEYSPACE " + TEST_KEYSPACE + " WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.close();
        session = cluster.connect(TEST_KEYSPACE);
    }

    @After
    public void tearDown() throws Exception {
        session.close();
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    public abstract CassandraMutagenConfig config();

    protected MutationResult<Integer> mutate(CassandraMutagenConfig config)
        throws IOException {

        // Initialize the list of mutations
        String rootResourcePath = "com/toddfast/mutagen/cassandra/test/mutations";
        String premutationsPath = "com/toddfast/mutagen/cassandra/test/premutations";


        CassandraMutagenImpl mutagen = new CassandraMutagenImpl(session, config);
        mutagen.initialize(rootResourcePath, premutationsPath, rootResourcePath);

        // Mutate!

        return mutagen.mutate();
    }


}
