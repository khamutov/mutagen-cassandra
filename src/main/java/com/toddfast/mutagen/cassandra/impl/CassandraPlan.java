package com.toddfast.mutagen.cassandra.impl;

import com.toddfast.mutagen.Coordinator;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;
import com.toddfast.mutagen.cassandra.premutation.Premutation;
import com.toddfast.mutagen.cassandra.premutation.PremutationProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CassandraPlan implements Plan<Integer> {
    private final static Logger log = LoggerFactory.getLogger(CassandraPlan.class);

    private Subject<Integer> subject;
    private Coordinator<Integer> coordinator;
    private List<Mutation<Integer>> mutations;
    private List<Premutation> premutations;
    private SessionHolder sessionHolder;
    private CassandraMutagenConfig config;

    public CassandraPlan(Subject<Integer> subject, Coordinator<Integer> coordinator,
                         List<Mutation<Integer>> mutations, List<Premutation> premutations,
                         SessionHolder sessionHolder, CassandraMutagenConfig config) {
        this.subject = subject;
        this.coordinator = coordinator;
        this.mutations = mutations;
        this.premutations = premutations;
        this.sessionHolder = sessionHolder;
        this.config = config;
    }

    @Override
    public Subject<Integer> getSubject() {
        return subject;
    }

    @Override
    public Coordinator<Integer> getCoordinator() {
        return coordinator;
    }

    @Override
    public List<Mutation<Integer>> getMutations() {
        return mutations;
    }

    public List<Premutation> getPremutations() {
        return premutations;
    }

    @Override
    public Result<Integer> execute() throws MutagenException {
        List<Mutation<Integer>> completedMutations = new ArrayList<>();
        List<Mutation<Integer>> remainingMutations =
            new ArrayList<>(mutations);
        MutagenException exception = null;

        Mutation.Context context = new CassandraContext(subject, coordinator);
        Map<Integer, Premutation> premutationMap = premutations.stream()
                                                               .collect(Collectors.toMap(Premutation::getMutationNumber, p -> p));
        State<Integer> lastState = null;
        for (Iterator<Mutation<Integer>>
             i = remainingMutations.iterator(); i.hasNext(); ) {

            final Mutation<Integer> mutation = i.next();
            int mutationNumber = mutation.getResultingState().getID();
            try {
                if (config.premutationsEnabled() && premutationMap.containsKey(mutationNumber)) {
                    PremutationProcessor processor = new PremutationProcessor(sessionHolder, premutationMap.get(mutationNumber));
                    processor.execute();
                    mutation.mutate(context);
                    processor.check();
                    processor.truncate();
                } else {
                    mutation.mutate(context);
                }

                lastState = mutation.getResultingState();

                // Add to the completed list, remove from remaining list
                completedMutations.add(mutation);
                i.remove();
                log.info("Successfully executed mutation [{}]", mutationNumber);
            } catch (RuntimeException e) {
                exception = new MutagenException("Exception executing " +
                                                     "mutation for state \"" + mutation.getResultingState() +
                                                     "\"", e);
                break;
            }
        }
        return new MutationResult<>(this, completedMutations,
                                    remainingMutations, lastState, exception);
    }

}
