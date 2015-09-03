package com.toddfast.mutagen.cassandra;

import com.toddfast.mutagen.Coordinator;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;
import org.springframework.stereotype.Component;

/**
 *
 *
 * @author Todd Fast
 */
public class CassandraCoordinator implements Coordinator<Integer> {

	/**
	 * 
	 * 
	 */
	public CassandraCoordinator() {
	}

	/**
	 * 
	 * 
	 */
	@Override
	public boolean accept(Subject<Integer> subject, State<Integer> targetState) {
		State<Integer> currentState = subject.getCurrentState();
		return targetState.getID() > currentState.getID();
	}
}
