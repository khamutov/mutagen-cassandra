package com.toddfast.mutagen.cassandra.premutation;

import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.cassandra.CassandraCoordinator;
import com.toddfast.mutagen.cassandra.CassandraSubject;
import com.toddfast.mutagen.cassandra.impl.CassandraContext;
import com.toddfast.mutagen.cassandra.impl.SessionHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class MultiPremutationProcessor implements PremutationProcessor {
    private final static Logger log = LoggerFactory.getLogger(PremutationProcessor.class);
    private SessionHolder sessionHolder;
    private List<Premutation> premutations;
    private List<Mutation<Integer>> mutations;
    private CassandraContext cassandraContext;

    public MultiPremutationProcessor(SessionHolder sessionHolder, List<Premutation> premutations,
                                     List<Mutation<Integer>> mutations, CassandraSubject subject,
                                     CassandraCoordinator cassandraCoordinator) {
        this.premutations = premutations;
        this.sessionHolder = sessionHolder;
        this.mutations = mutations;
        this.cassandraContext = new CassandraContext(subject, cassandraCoordinator);
    }

    @Override
    public void execute() {
        Objects.nonNull(mutations);
        Objects.nonNull(premutations);
        premutations.forEach((p) -> {
            Mutation<Integer> mutation = mutations.stream()
                                                  .filter(m -> m.getResultingState().getID() == p.getMutationNumber())
                                                  .findFirst()
                                                  .orElseThrow(() -> new IllegalStateException("Mutation [%s] was not found. But found corresponding premutation."));

            SinglePremutationProcessor processor = new SinglePremutationProcessor(sessionHolder, p);
            processor.execute();
            mutation.mutate(cassandraContext);
            processor.check();
            processor.truncate();
        });
    }

}
