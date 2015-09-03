package com.toddfast.mutagen.cassandra.impl;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.State;
import org.cassandraunit.AbstractCassandraUnit4TestCase;
import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.yaml.ClassPathYamlDataSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.CassandraAdminOperations;
import org.springframework.data.cassandra.core.CassandraAdminTemplate;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.mapping.BasicCassandraMappingContext;

import java.io.IOException;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static org.junit.Assert.*;

public class CassandraMutagenImplTest extends AbstractCassandraUnit4TestCase {

    private static Cluster cluster;

    public CassandraMutagenImplTest() {
	}

    @Override
    public DataSet getDataSet() {
        return new ClassPathYamlDataSet("keyspaceDataSet.yml");
    }

    @BeforeClass
    public static void setUpOnce() {
        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9142).build();
    }

	@AfterClass
	public static void tearDownClass() {
        cluster.close();
		System.out.println("Dropped keyspace mutagen_test");
	}


	/**
	 * This is it!
	 *
	 */
	private Plan.Result<Integer> mutate()
			throws IOException {

		// Initialize the list of mutations
		String rootResourcePath="com/toddfast/mutagen/cassandra/test/mutations";

        Session localSession = cluster.connect("mutagen_test");
        CassandraConverter converter = new MappingCassandraConverter(new BasicCassandraMappingContext());
        CassandraAdminOperations cassandraAdminOperations = new CassandraAdminTemplate(localSession, converter);

        CassandraMutagenImpl mutagen = new CassandraMutagenImpl(cassandraAdminOperations);
		mutagen.initialize(rootResourcePath);

		// Mutate!

        Plan.Result<Integer> result = mutagen.mutate();
        localSession.close();

        return result;
	}


	private void testInitialize() throws Exception {

		Plan.Result<Integer> result = mutate();

		// Check the results
		State<Integer> state=result.getLastState();

		System.out.println("Mutation complete: "+result.isMutationComplete());
		System.out.println("Exception: "+result.getException());
		if (result.getException()!=null) {
			result.getException().printStackTrace();
		}
		System.out.println("Completed mutations: "+result.getCompletedMutations());
		System.out.println("Remining mutations: "+result.getRemainingMutations());
		System.out.println("Last state: "+(state!=null ? state.getID() : "null"));

		assertTrue(result.isMutationComplete());
		assertNull(result.getException());
		assertEquals((state!=null ? state.getID() : (Integer)(-1)),(Integer)4);
	}


	/**
	 *
	 *
	 */
	@Test
	public void testData() throws Exception {

        testInitialize();
        Session localSession = cluster.connect("mutagen_test");
        CassandraOperations cassandraOperations = new CassandraTemplate(localSession);

        Select select = QueryBuilder.select().all().from("Test1");
        select.where(eq("key", "row1"));
        Row row = cassandraOperations.getSession().execute(select).one();

		assertEquals("foo", row.getString("value1"));
		assertEquals("bar", row.getString("value2"));

        select = QueryBuilder.select().all().from("Test1");
        select.where(eq("key", "row2"));
        row = cassandraOperations.getSession().execute(select).one();

		assertEquals("chicken", row.getString("value1"));
		assertEquals("sneeze", row.getString("value2"));

        select = QueryBuilder.select().all().from("Test1");
        select.where(eq("key", "row3"));
        row = cassandraOperations.getSession().execute(select).one();

		assertEquals("bar", row.getString("value1"));
		assertEquals("baz", row.getString("value2"));

        localSession.close();
	}
}
