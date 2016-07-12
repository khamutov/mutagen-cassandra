package com.toddfast.mutagen.cassandra.impl;

import com.datastax.driver.core.exceptions.DriverException;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.cassandra.AbstractCassandraMutation;
import com.toddfast.mutagen.cassandra.dao.SchemaVersionDao;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Todd Fast
 */
public class CQLMutation extends AbstractCassandraMutation {

	private String source;
	private byte[] sourceBytes;
	private State<Integer> state;
	private List<String> statements = new ArrayList<>();

	public CQLMutation(SessionHolder session, SchemaVersionDao schemaVersionDao, String resourceName) {
		super(session, schemaVersionDao);
		state = super.parseVersion(resourceName);
		loadCQLStatements(resourceName);
	}

	private void loadCQLStatements(String resourceName) {

		try {
			sourceBytes = loadResource(resourceName);
			source = new String(sourceBytes);
		}
		catch (IOException e) {
			throw new MutagenException("Could not load resource \""+
				resourceName+"\"",e);
		}

		String[] lines=source.split("\n");

		StringBuilder statement=new StringBuilder();

		for (int i=0; i<lines.length; i++) {
			int index;
			String line=lines[i];
			String trimmedLine=line.trim();

			if (trimmedLine.startsWith("--") || trimmedLine.startsWith("//")) {
				// Skip
			}
			else
			if ((index=line.indexOf(";"))!=-1) {
				// Split the line at the semicolon
				statement
					.append("\n")
					.append(line.substring(0,index+1));
				statements.add(statement.toString());

				if (line.length() > index+1) {
					statement=new StringBuilder(line.substring(index+1));
				}
				else {
					statement=new StringBuilder();
				}
			}
			else {
				statement
					.append("\n")
					.append(line);
			}

		}


	}

	private byte[] loadResource(String path)
			throws IOException {

		ClassLoader loader=Thread.currentThread().getContextClassLoader();
		if (loader==null) {
			loader=getClass().getClassLoader();
		}

		InputStream input=loader.getResourceAsStream(path);

		if (input==null) {
			throw new IllegalArgumentException("Resource \""+
				path+"\" not found");
		}

		try {
			return IOUtils.toByteArray(input);
		}
		finally {
			IOUtils.closeQuietly(input);
		}
	}

	@Override
	public State<Integer> getResultingState() {
		return state;
	}

	@Override
	public byte[] getFootprint() {
		return sourceBytes;
	}

	@Override
	protected void performMutation(Context context) {
		context.debug("Executing mutation {}",state.getID());

		for (String statement: statements) {
			context.debug("Executing CQL \"{}\"",statement);

            try {
                getSession().execute(statement);
            } catch (DriverException e) {
                context.error("Exception executing CQL {}",statement,e);
                throw new MutagenException("Exception executing CQL \""+
                        statement+"\"",e);
            } catch (RuntimeException e) {
				context.error("Exception executing CQL {}",statement,e);
				throw e;
			}
			context.info("Executed statement from mutation [{}].", state.getID());
			context.debug("Successfully executed CQL {}", statement);
		}

		context.debug("Done executing mutation {}",state.getID());
	}
}
