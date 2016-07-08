package com.toddfast.mutagen.cassandra;

import com.toddfast.mutagen.Plan;

import java.io.IOException;

/**
 *
 *
 * @author Todd Fast
 */
public interface CassandraMutagen {

	/**
	 *
	 *
	 */
	Plan.Result<Integer> mutate();
}
