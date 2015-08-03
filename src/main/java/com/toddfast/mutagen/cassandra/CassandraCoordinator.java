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
@Component
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




	////////////////////////////////////////////////////////////////////////////
	// Fields
	////////////////////////////////////////////////////////////////////////////

	/*public static final ColumnFamily<String,String> VERSION_CF=
		ColumnFamily.newColumnFamily(
			"schema_version",
			StringSerializer.get(),
			StringSerializer.get());*/
}
