package com.toddfast.mutagen.cassandra.hash;

import com.datastax.driver.core.Session;
import com.toddfast.mutagen.cassandra.AbstractCassandraMutation;
import com.toddfast.mutagen.cassandra.dao.SchemaVersionDao;
import com.toddfast.mutagen.cassandra.impl.SessionHolder;
import com.toddfast.mutagen.cassandra.table.SchemaVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public class ActionHashChecker {
    private static final Logger log = LoggerFactory.getLogger(ActionHashChecker.class);
    private SchemaVersionDao versionDao;
    private List<String> resources;
    private SessionHolder sessionHolder;

    public ActionHashChecker(SessionHolder sessionHolder, SchemaVersionDao dao, List<String> resources) {
        this.resources = resources;
        this.sessionHolder = sessionHolder;
        this.versionDao = dao;
    }

    /**
     * Checks hashes of mutations and performs an action if distinction found
     *
     * @param action action to be performed. returns session
     */
    public boolean tryDrop(UnaryOperator<Session> action) {
        Map<Integer, Supplier<byte[]>> mutations;
        //get lazy loading map sorted by migration number
        try {
            mutations = new TreeMap<>(getMutations(resources));
        } catch (IOException e) {
            log.error("Can not load resources!", e);
            throw new RuntimeException(e);
        }
        //get version map with hashes from cassandra with migration number key
        Map<Integer, SchemaVersion> hashesByNumber = new TreeMap<>(versionDao.getHashes()
                                                                             .stream()
                                                                             .collect(Collectors.toMap(s -> Integer.valueOf(s.getKey()), s -> s)));
        if(hashesByNumber.size() == 0) {
            log.info("There are no mutations previously applied. Running mutations.");
            return true;
        }
        //check hashes
        Iterator<Map.Entry<Integer, Supplier<byte[]>>> resourcesIterator = mutations.entrySet().iterator();
        Iterator<Map.Entry<Integer, SchemaVersion>> versionIterator = hashesByNumber.entrySet().iterator();
        while (resourcesIterator.hasNext()) {
            Map.Entry<Integer, Supplier<byte[]>> entry = resourcesIterator.next();
            boolean mutationIsPresent = hashesByNumber.containsKey(entry.getKey());
            if (versionIterator.hasNext()){
                versionIterator.next();
            }
            if (mutationIsPresent &&
                !Arrays.equals(AbstractCassandraMutation.md5(entry.getValue().get()), hashesByNumber.get(entry.getKey())
                                                                                                    .getValue()
                                                                                                    .array())) {
                log.info("Found changed mutation [{}]. Performing action.", entry.getKey(), sessionHolder.get()
                                                                                                         .getLoggedKeyspace());
                sessionHolder.set(action.apply(sessionHolder.get()));
                return true;
            }
            //check if migration is missed in cassandra
            else if (!mutationIsPresent && versionIterator.hasNext()) {
                log.info("Mutation [{}] is in resources but was not previously applied. Performing action.", entry.getKey());
                sessionHolder.set(action.apply(sessionHolder.get()));
                return true;
            }
            if (mutationIsPresent) {
                log.info("Skipping [{}] mutation. MD5 hashes are equal.", entry.getKey());
            } else {
                log.info("All mutations were checked. Applying new mutations.", entry.getKey());
                return false;
            }
        }
        return false;
    }

    /**
     * Naive implementation of lazy loading map
     */
    private Map<Integer, Supplier<byte[]>> getMutations(List<String> resources) throws IOException {
        return resources.stream()
                        .filter(p -> p.endsWith(".cql") || p.endsWith(".java"))
                        .map(Paths::get)
                        .collect(Collectors.toMap(
                            p -> {
                                String mutationName = p.getFileName().toString();
                                try {
                                    return Integer.valueOf(mutationName.substring(1, mutationName.indexOf('_')));
                                } catch (Throwable e) {
                                    log.error("Mutation name is malformed {}. Expected: \\w\\d+_\\w*", mutationName);
                                    throw new RuntimeException(e);
                                }
                            },
                            p ->
                                () -> {
                                    try {
                                        return Files.readAllBytes(p);
                                    } catch (IOException e) {
                                        log.error("Can not read bytes of {}", p.getFileName(), e);
                                        throw new RuntimeException(e);
                                    }
                                }
                        ));
    }

}
