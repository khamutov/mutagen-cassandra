package com.toddfast.mutagen.cassandra.test.mutations;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.basic.SimpleState;
import com.toddfast.mutagen.cassandra.AbstractCassandraMutation;
import com.toddfast.mutagen.cassandra.dao.SchemaVersionDao;
import com.toddfast.mutagen.cassandra.impl.SessionHolder;
import org.apache.commons.io.IOUtils;

import java.io.IOException;

/**
 * @author Todd Fast
 */
public class V003_test_3 extends AbstractCassandraMutation {

    public V003_test_3(SessionHolder sessionHolder, SchemaVersionDao schemaVersionDao) {
        super(sessionHolder, schemaVersionDao);
        state = new SimpleState<>(3);
    }

    @Override
    public State<Integer> getResultingState() {
        return state;
    }

    @Override
    public byte[] getFootprint() {
        try {
            return IOUtils.toByteArray(this.getClass().getResourceAsStream(this.getClass().getSimpleName() + ".java"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void performMutation(Context context) {
        context.debug("Executing mutation {}", state.getID());

        Session session = getSession();

        Insert insert = QueryBuilder.insertInto("Test1")
                                    .value("key", "row2")
                                    .value("value1", "chicken")
                                    .value("value2", "sneeze");

        session.execute(insert);
    }

    private State<Integer> state;
}
