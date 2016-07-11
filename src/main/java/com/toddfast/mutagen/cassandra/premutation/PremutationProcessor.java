package com.toddfast.mutagen.cassandra.premutation;

import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.cassandra.impl.SessionHolder;

import java.util.ArrayList;
import java.util.List;

public interface PremutationProcessor {

    void execute();

    static List<Premutation> loadPremutations(SessionHolder sessionHolder, List<String> resources) {
        List<Premutation> premutations = new ArrayList<>();
        resources.stream().filter(resource -> resource.endsWith(".class")).forEach(resource -> {
            int index = resource.indexOf(".class");
            String className = resource.substring(0, index).replace('/', '.');
            try {
                if (isPremutationResource(className)) {
                    premutations.add(loadPremutation(sessionHolder, className));
                }
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(String.format("Class [%s] was not found", className), e);
            }

        });
        return premutations;
    }

    static Premutation loadPremutation(SessionHolder sessionHolder, String className) {
        int mutationNumber;
        try {
            mutationNumber = Integer.valueOf(className.substring(className.lastIndexOf('.') + 2, className.indexOf('_')));
        } catch (NumberFormatException | IndexOutOfBoundsException e) {
            throw new RuntimeException("Malformed premutation class name. Class name must start with Vxxx_{name}", e);
        }
        try {
            Class<?> clazz = Class.forName(className);
            if (!isPremutationResource(className)) {
                throw new MutagenException("Class [" + className + "] doesn't inherit Premutation class");
            }
            Premutation premutation = (Premutation) clazz.getConstructor(SessionHolder.class)
                                                         .newInstance(sessionHolder);
            premutation.setMutationNumber(mutationNumber);
            return premutation;
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Can not instantiate premutation class", e);
        }
    }

    static boolean isPremutationResource(String className) throws ClassNotFoundException {
        return Premutation.class.isAssignableFrom(Class.forName(className));
    }
}
