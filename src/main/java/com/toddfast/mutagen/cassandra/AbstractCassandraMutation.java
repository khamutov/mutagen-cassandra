package com.toddfast.mutagen.cassandra;

import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.basic.SimpleState;
import com.toddfast.mutagen.cassandra.dao.SchemaVersionDao;
import org.springframework.context.ApplicationContext;
import org.springframework.data.cassandra.core.CassandraOperations;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 *
 * @author Todd Fast
 */
public abstract class AbstractCassandraMutation implements Mutation<Integer> {

    private ApplicationContext applicationContext;

    /**
	 *
	 *
	 */
	protected AbstractCassandraMutation(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
	}


	/**
	 *
	 *
	 */
	@Override
	public String toString() {
		if (getResultingState()!=null) {
			return super.toString()+"[state="+getResultingState().getID()+"]";
		}
		else {
			return super.toString();
		}
	}


	/**
	 *
	 *
	 */
	protected final State<Integer> parseVersion(String resourceName) {
		String versionString=resourceName;
		int index=versionString.lastIndexOf("/");
		if (index!=-1) {
			versionString=versionString.substring(index+1);
		}

		index=versionString.lastIndexOf(".");
		if (index!=-1) {
			versionString=versionString.substring(0,index);
		}

		StringBuilder buffer=new StringBuilder();
		for (Character c: versionString.toCharArray()) {
			// Skip all initial non-digit characters
			if (!Character.isDigit(c)) {
				if (buffer.length()==0) {
					continue;
				}
				else {
					// End when we reach the first non-digit
					break;
				}
			}
			else {
				buffer.append(c);
			}
		}

		return new SimpleState<Integer>(Integer.parseInt(buffer.toString()));
	}


	/**
	 * Perform the actual mutation
	 *
	 */
	protected abstract void performMutation(Context context);


	/**
	 *
	 *
	 */
	@Override
	public abstract State<Integer> getResultingState();


	/**
	 * Return a canonical representative of the change in string form
	 *
	 */
	protected abstract String getChangeSummary();


	/**
	 * Performs the actual mutation and then updates the recorded schema version
	 *
	 */
	@Override
	public final void mutate(Context context)
			throws MutagenException {

        SchemaVersionDao schemaVersionDao = applicationContext.getBean(SchemaVersionDao.class);

		// Perform the mutation
		performMutation(context);

		int version=getResultingState().getID();

		String change=getChangeSummary();
		if (change==null) {
			change="";
		}

		String changeHash = md5String(change);

		// The straightforward way, without locking
        ByteBuffer versionByteBuffer = ByteBuffer.allocate(4).putInt(version);
        versionByteBuffer.rewind();
        schemaVersionDao.add("state", "version", versionByteBuffer);
        schemaVersionDao.add(String.format("%08d", version), "change", ByteBuffer.wrap(change.getBytes()));
        schemaVersionDao.add(String.format("%08d", version), "hash", ByteBuffer.wrap(changeHash.getBytes()));
	}


	/**
	 *
	 *
	 * @param key
	 * @return
	 */
	public static byte[] md5(String key) {
		MessageDigest algorithm;
		try {
			algorithm=MessageDigest.getInstance("MD5");
		}
		catch (NoSuchAlgorithmException ex) {
			throw new RuntimeException(ex);
		}

		algorithm.reset();

		try {
			algorithm.update(key.getBytes("UTF-8"));
		}
		catch (UnsupportedEncodingException ex) {
			throw new RuntimeException(ex);
		}

		byte[] messageDigest=algorithm.digest();
		return messageDigest;
	}


	/**
	 *
	 *
	 * @param key
	 * @return
	 */
	public static String md5String(String key) {
		byte[] messageDigest=md5(key);
		return toHex(messageDigest);
	}


	/**
	 * Encode a byte array as a hexadecimal string
	 *
	 * @param	bytes
	 * @return
	 */
	public static String toHex(byte[] bytes) {
		StringBuilder hexString=new StringBuilder();
		for (int i=0; i<bytes.length; i++) {

			String hex=Integer.toHexString(0xFF & bytes[i]);
			if (hex.length() == 1) {
				hexString.append('0');
			}

			hexString.append(hex);
		}

		return hexString.toString();
	}

    public CassandraOperations getCassandraOperations() {
        return applicationContext.getBean(CassandraOperations.class);
    }

    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }
}
