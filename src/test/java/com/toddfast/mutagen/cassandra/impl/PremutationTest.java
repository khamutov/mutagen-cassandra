package com.toddfast.mutagen.cassandra.impl;

import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.cassandra.premutation.CheckStateException;
import org.junit.Test;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PremutationTest extends MutagenBaseTest {

    @Test
    public void testMutate() throws Exception {
        Plan.Result<Integer> result = mutate(config());

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

        assertFalse(result.isMutationComplete());
        assertTrue(result.getException().getCause() instanceof CheckStateException);
    }

    @Override
    public CassandraMutagenConfig config() {
        return new CassandraMutagenConfig().enablePremutations();
    }
}
