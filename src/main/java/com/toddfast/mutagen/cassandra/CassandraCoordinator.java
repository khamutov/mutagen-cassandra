package com.toddfast.mutagen.cassandra;

import com.toddfast.mutagen.Coordinator;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;
import com.toddfast.mutagen.cassandra.impl.CassandraMutagenConfig;
import org.springframework.cglib.core.Predicate;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.function.BiPredicate;

/**
 *
 *
 * @author Todd Fast
 */
public class CassandraCoordinator implements Coordinator<Integer> {

	private final CassandraMutagenConfig config;
    private final BiPredicate<State<Integer>, State<Integer>> predicate;

	public CassandraCoordinator(CassandraMutagenConfig config) {
		this.config = config;
        if (config.getMode() == CassandraMutagenConfig.Mode.STANDARD) {
            predicate = (current, target) -> current.getID() < target.getID();
        } else {
            predicate = (current, target) -> Objects.equals(target.getID(), config.getMutation());
        }
	}

	/**
	 * 
	 * 
	 */
	@Override
	public boolean accept(Subject<Integer> subject, State<Integer> targetState) {
        return predicate.test(subject.getCurrentState(), targetState);
	}
}
