package com.toddfast.mutagen.cassandra.impl;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.cassandra.table.SchemaConstants;
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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.junit.Assert.*;

public class CassandraMutagenImplTest extends AbstractCassandraUnit4TestCase {

    private static final Logger log = LoggerFactory.getLogger(CassandraMutagenImplTest.class);

    private static final String KEYSPACE = "mutagen_test";

    private static Cluster cluster;
    private static Session session;

    public CassandraMutagenImplTest() {
	}

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

    /**
	 * This is it!
	 *
     * @param config
     */
	private Plan.Result<Integer> mutate(CassandraMutagenConfig config)
			throws IOException {

		// Initialize the list of mutations
		String rootResourcePath="com/toddfast/mutagen/cassandra/test/mutations";


        CassandraMutagenImpl mutagen = new CassandraMutagenImpl(session, config);
		mutagen.initialize(rootResourcePath);

		// Mutate!

        return mutagen.mutate();
	}

    private State<Integer> testMutate(CassandraMutagenConfig config) throws Exception {
        Plan.Result<Integer> result = mutate(config);

        // Check the results
        State<Integer> state = result.getLastState();

        log.info("Mutation complete: {}", result.isMutationComplete());
        log.info("Exception: {}", result.getException());
        if (result.getException() != null) {
            result.getException().printStackTrace();
        }
        log.info("Completed mutations: ", result.getCompletedMutations());
        log.info("Remaining mutations: ", result.getRemainingMutations());
        log.info("Last state: " + (state != null ? state.getID() : "null"));

        assertTrue(result.isMutationComplete());
        assertNull(result.getException());
        return state;
    }

	@Test
	public void testForceMutate() throws Exception {
        testMutate(new CassandraMutagenConfig());

        session.execute(
            QueryBuilder.insertInto("Test1")
                        .value("key", "row2")
                        .value("value1", "a1")
                        .value("value2", "a2"));

        Select select = select().from("Test1");
        select.where(eq("key", "row2"));
        Row rowBefore = session.execute(select).one();

        assertEquals("a1", rowBefore.getString("value1"));
        assertEquals("a2", rowBefore.getString("value2"));

        Select selectVersion = select().from(SchemaConstants.TABLE_SCHEMA_VERSION);
        selectVersion.where(eq("key", "state"));
        selectVersion.where(eq("column1", "version"));

        Row rowVersionBefore = session.execute(selectVersion).one();
        assertEquals(5, rowVersionBefore.getBytes("value").getInt());

        State<Integer> state = testMutate(new CassandraMutagenConfig().forceMutation(3));
        assertEquals(3, (int) state.getID());

        Row rowAfter = session.execute(select).one();
        assertEquals("chicken", rowAfter.getString("value1"));
        assertEquals("sneeze", rowAfter.getString("value2"));

        Row rowVersionAfter = session.execute(selectVersion).one();
        assertEquals(5, rowVersionAfter.getBytes("value").getInt());

    }

    @Test
    public void testForceRangeMutate() throws Exception {
        testMutate(new CassandraMutagenConfig());

        Select selectVersion = select().from(SchemaConstants.TABLE_SCHEMA_VERSION);
        selectVersion.where(eq("key", "state"));
        selectVersion.where(eq("column1", "version"));

        Row rowVersionBefore = session.execute(selectVersion).one();
        assertEquals(5, rowVersionBefore.getBytes("value").getInt());

        State<Integer> state = testMutate(new CassandraMutagenConfig().forceRangeMutation(3, 4));
        assertEquals(4, (int) state.getID());
    }

    @Test
    public void testForceVersion() throws Exception {
        testMutate(new CassandraMutagenConfig());

        session.execute(
            QueryBuilder.insertInto("Test1")
                        .value("key", "row2")
                        .value("value1", "a1")
                        .value("value2", "a2"));

        Select select = select().from("Test1");
        select.where(eq("key", "row2"));
        Row rowBefore = session.execute(select).one();

        assertEquals("a1", rowBefore.getString("value1"));
        assertEquals("a2", rowBefore.getString("value2"));

        Select selectVersion = select().from(SchemaConstants.TABLE_SCHEMA_VERSION);
        selectVersion.where(eq("key", "state"));
        selectVersion.where(eq("column1", "version"));

        Row rowVersionBefore = session.execute(selectVersion).one();
        assertEquals(5, rowVersionBefore.getBytes("value").getInt());

        State<Integer> state = testMutate(new CassandraMutagenConfig().forceVersion(3));
        assertEquals(3, (int) state.getID());

        Row rowAfter = session.execute(select).one();
        assertEquals("a1", rowAfter.getString("value1"));
        assertEquals("a2", rowAfter.getString("value2"));

        Row rowVersionAfter = session.execute(selectVersion).one();
        assertEquals(3, rowVersionAfter.getBytes("value").getInt());

    }

    /**
	 *
	 *
	 */
	@Test
	public void testData() throws Exception {

        State<Integer> state = testMutate(new CassandraMutagenConfig());

        assertEquals(5, state != null ? (int) state.getID() : -1);

        Select select = QueryBuilder.select().all().from("Test1");
        select.where(eq("key", "row1"));
        Row row = session.execute(select).one();

		assertEquals("foo", row.getString("value1"));
		assertEquals("bar", row.getString("value2"));

        select = QueryBuilder.select().all().from("Test1");
        select.where(eq("key", "row2"));
        row = session.execute(select).one();

		assertEquals("chicken", row.getString("value1"));
		assertEquals("sneeze", row.getString("value2"));

        select = QueryBuilder.select().all().from("Test1");
        select.where(eq("key", "row3"));
        row = session.execute(select).one();

		assertEquals("bar", row.getString("value1"));
		assertEquals("baz", row.getString("value2"));

	}
}
