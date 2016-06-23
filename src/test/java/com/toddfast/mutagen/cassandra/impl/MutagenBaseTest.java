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

public abstract class MutagenBaseTest extends AbstractCassandraUnit4TestCase {
    protected static final Logger log = LoggerFactory.getLogger(CassandraMutagenImplTest.class);

    protected static final String KEYSPACE = "mutagen_test";

    protected static Cluster cluster;
    protected static Session session;

    @Override
    public DataSet getDataSet() {
        return new ClassPathYamlDataSet("keyspaceDataSet.yml");
    }

    @BeforeClass
    public static void setUpOnce() throws InterruptedException, TTransportException, ConfigurationException, IOException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9142).build();
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    @AfterClass
    public static void tearDownClass() {
        cluster.close();
        log.info("Dropped keyspace " + KEYSPACE);
    }

    @Before
    public void setUp() throws Exception {
        session = cluster.connect(KEYSPACE);
    }

    @After
    public void tearDown() throws Exception {
        session.close();
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    public abstract CassandraMutagenConfig config();

    protected Plan.Result<Integer> mutate(CassandraMutagenConfig config)
        throws IOException {

        // Initialize the list of mutations
        String rootResourcePath = "com/toddfast/mutagen/cassandra/test/mutations";
        String premutationsPath = "com/toddfast/mutagen/cassandra/test/premutations";


        CassandraMutagenImpl mutagen = new CassandraMutagenImpl(session, config);
        mutagen.initialize(rootResourcePath, premutationsPath);

        // Mutate!

        return mutagen.mutate();
    }


}
