package com.toddfast.mutagen.cassandra.hash;

import com.toddfast.mutagen.cassandra.dao.SchemaVersionDao;
import com.toddfast.mutagen.cassandra.table.SchemaVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class HashChecker {
    private static final Logger log = LoggerFactory.getLogger(HashChecker.class);

    private List<Pair> hashes;

    private HashChecker() {
        hashes = new ArrayList<>();
    }

    /**
     * Check if hashes from db and resources are the same.
     *
     * @return true - if no changes were found,
     * false - if MD5 hashes of same mutation are unequal,
     * missed mutation found in db, or no mutations in db were found
     */
    public boolean isValid() {
        boolean valid = true;
        for (Pair pair : hashes) {
            Node resNode = pair.resNode, dbNode = pair.dbNode;
            //check if end was reached
            if (dbNode != null) {
                //check hashes
                if (resNode.mutationNumber == dbNode.mutationNumber) {
                    valid = resNode.hasSameHashAs(dbNode);
                }
                //check for skipped mutations in db
                else if (dbNode.mutationNumber > resNode.mutationNumber) {
                    valid = false;
                }
                if (!valid) {
                    break;
                }
            }
            //end reached
            else {
                //if db is empty state is considered as not valid
                Pair minResMutationPair = hashes.stream()
                                                .min(Comparator.comparing((p) -> p.resNode.mutationNumber))
                                                .orElseThrow(() -> new IllegalStateException("No resource mutations were found"));
                valid = !(resNode == minResMutationPair.resNode);
                break;
            }
        }
        return valid;
    }

    /**
     * Check if there are new mutations
     *
     * @return check result
     */
    public boolean hasNewMutations() {
        return hashes.stream().anyMatch(p -> p.resNode.mutationNumber > p.dbNode.mutationNumber);
    }

    /**
     * Build sorted array of mutation pairs.
     *
     * @param resources        list of resources for resource nodes
     * @param schemaVersionDao dao for db nodes
     * @return Built HashChecker object
     */
    public static HashChecker build(List<String> resources, SchemaVersionDao schemaVersionDao) {
        HashChecker list = new HashChecker();
        Map<Integer, String> resMap = new TreeMap<>(resources.stream()
                                                             .collect(Collectors.toMap(HashChecker::getVersion, s -> s)));
        Map<Integer, SchemaVersion> dbHashes = new TreeMap<>(schemaVersionDao.getHashes()
                                                                             .stream()
                                                                             .collect(Collectors.toMap(s -> Integer.valueOf(s.getKey()), s -> s)));
        Iterator<Map.Entry<Integer, SchemaVersion>> iterator = dbHashes.entrySet().iterator();
        for (Integer mutationNumber : resMap.keySet()) {
            Node resNode = new Node(mutationNumber, new FileSystemHashSupplier(resMap.get(mutationNumber)));
            Node dbNode = null;
            if (iterator.hasNext()) {
                Map.Entry<Integer, SchemaVersion> entry = iterator.next();
                dbNode = new Node(entry.getKey(), new SchemaVersionHashSupplier(entry.getValue()));
            }
            list.add(new Pair(resNode, dbNode));
        }
        return list;
    }

    private void add(Pair pair) {
        hashes.add(pair);
    }

    private static Integer getVersion(String mutationName) {
        try {
            return Integer.valueOf(mutationName.substring(mutationName.lastIndexOf('/') + 2, mutationName.indexOf('_')));
        } catch (Throwable e) {
            log.error("Mutation name is malformed {}. Expected: \\w\\d+_\\w*", mutationName);
            throw new RuntimeException(e);
        }
    }

    private static class Pair {
        private Node resNode;
        private Node dbNode;

        public Pair(Node resNode, Node dbNode) {
            this.resNode = resNode;
            this.dbNode = dbNode;
        }
    }

    private static class Node {
        private int mutationNumber;
        private Supplier<byte[]> hashSupplier;

        private Node(int mutationNumber, Supplier<byte[]> hashSupplier) {
            this.mutationNumber = mutationNumber;
            this.hashSupplier = hashSupplier;
        }

        private boolean hasSameHashAs(Node other) {
            return Arrays.equals(this.hashSupplier.get(), other.hashSupplier.get());
        }
    }

}
