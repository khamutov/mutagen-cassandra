package com.toddfast.mutagen.cassandra.hash;

import com.toddfast.mutagen.cassandra.AbstractCassandraMutation;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Supplier;

public class FileSystemHashSupplier implements Supplier<byte[]> {

    private String resourcePath;

    public FileSystemHashSupplier(String resourcePath) {
        this.resourcePath = resourcePath;
    }

    @Override
    public byte[] get() {
        return AbstractCassandraMutation.md5(loadResource(resourcePath));
    }

    private byte[] loadResource(String resource) {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) {
            loader = getClass().getClassLoader();
        }
        try (InputStream stream = loader.getResourceAsStream(resource)) {
            return IOUtils.toByteArray(stream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
