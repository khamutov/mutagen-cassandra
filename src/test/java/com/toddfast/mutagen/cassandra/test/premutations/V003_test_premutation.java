package com.toddfast.mutagen.cassandra.test.premutations;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.toddfast.mutagen.cassandra.premutation.CheckStateException;
import com.toddfast.mutagen.cassandra.premutation.Premutation;
import com.toddfast.mutagen.cassandra.premutation.Scheme;
import com.toddfast.mutagen.cassandra.premutation.Table;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class V003_test_premutation extends Premutation {

    public V003_test_premutation(Session session) {
        super(session);
    }

    @Override
    public Scheme formScheme() {
        return Scheme.instance().addTable(
            Table.instance("Test1")
                 .value("key", "test")
                 .value("value1", "val1"));
        //value2 is intentionally omitted

    }

    @Override
    public void check() {
        Session session = getSession();
        Row row = session.execute(QueryBuilder.select()
                                              .all()
                                              .from("Test1")
                                              .where(eq("key", "row2")))
                         .one();
        String val = row.getString("value2");
        if (val != null) {
            throw new CheckStateException("field [value2] has value [" +
                                              val + "] and we'll pretend that it is wrong!");
        }
    }
}
