package com.toddfast.mutagen.cassandra.test.mutations;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.basic.SimpleState;
import com.toddfast.mutagen.cassandra.AbstractCassandraMutation;
import com.toddfast.mutagen.cassandra.dao.SchemaVersionDao;

/**
 * @author Todd Fast
 */
public class V003 extends AbstractCassandraMutation {

    public V003(Session session, SchemaVersionDao schemaVersionDao) {
        super(session, schemaVersionDao);
        state = new SimpleState<>(3);
    }

    @Override
    public State<Integer> getResultingState() {
        return state;
    }


    /**
     * Return a canonical representative of the change in string form
     */
    @Override
    protected String getChangeSummary() {
        return "update 'Test1' set value1='chicken', value2='sneeze' " +
            "where key='row2';";
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
