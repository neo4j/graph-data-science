/*
 * Copyright (c) 2017-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStoreFactory;
import org.neo4j.graphalgo.canonization.CanonicalAdjacencyMatrix;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.loading.CypherFactory;
import org.neo4j.graphalgo.core.loading.NativeFactory;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.Orientation.NATURAL;
import static org.neo4j.graphalgo.Orientation.REVERSE;

public final class TestSupport {

    private TestSupport() {}

    @Retention(RetentionPolicy.RUNTIME)
    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.TestSupport#allTypes")
    public @interface AllGraphTypesTest {}

    public static Stream<Class<? extends GraphStoreFactory>> allTypes() {
        return Stream.of(NativeFactory.class, CypherFactory.class);
    }

    public static Stream<Orientation> allDirectedProjections() {
        return Stream.of(NATURAL, REVERSE);
    }

    public static <T> Supplier<Stream<Arguments>> toArguments(Supplier<Stream<T>> fn) {
        return () -> fn.get().map(Arguments::of);
    }

    @SafeVarargs
    public static Stream<Arguments> crossArguments(Supplier<Stream<Arguments>> firstFn, Supplier<Stream<Arguments>>... otherFns) {
        return Arrays
                .stream(otherFns)
                .reduce(firstFn, (l, r) -> () -> crossArguments(l, r))
                .get();
    }

    public static Stream<Arguments> crossArguments(Supplier<Stream<Arguments>> leftFn, Supplier<Stream<Arguments>> rightFn) {
        return leftFn.get().flatMap(leftArgs ->
                rightFn.get().map(rightArgs -> {
                    Collection<Object> leftObjects = new ArrayList<>(Arrays.asList(leftArgs.get()));
                    leftObjects.addAll(new ArrayList<>(Arrays.asList(rightArgs.get())));
                    return Arguments.of(leftObjects.toArray());
                }));
    }

    public static void assertGraphEquals(Graph expected, Graph actual) {
        Assertions.assertEquals(expected.nodeCount(), actual.nodeCount(), "Node counts do not match.");
        // TODO: we cannot check this right now, because the relationshhip counts depends on how the graph has been loaded for HugeGraph
//        Assertions.assertEquals(expected.relationshipCount(), actual.relationshipCount(), "Relationship counts to not match.");
        Assertions.assertEquals(CanonicalAdjacencyMatrix.canonicalize(expected), CanonicalAdjacencyMatrix.canonicalize(actual));
    }

    /**
     * Checks if exactly one of the given expected graphs matches the actual graph.
     */
    public static void assertGraphEquals(Collection<Graph> expectedGraphs, Graph actual) {
        List<String> expectedCanonicalized = expectedGraphs.stream().map(CanonicalAdjacencyMatrix::canonicalize).collect(Collectors.toList());
        String actualCanonicalized = CanonicalAdjacencyMatrix.canonicalize(actual);

        boolean equals = expectedCanonicalized
            .stream()
            .map(expected -> expected.equals(actualCanonicalized))
            .reduce(Boolean::logicalXor)
            .orElse(false);

        String message = String.format(
            "None of the given graphs matches the actual one.%nActual:%n%s%nExpected:%n%s",
            actualCanonicalized,
            String.join("\n\n", expectedCanonicalized)
        );

        assertTrue(equals, message);
    }

    /**
     * This method assumes that the given algorithm calls {@link Algorithm#assertRunning()} at least once.
     * When called, the algorithm will sleep for {@code sleepMillis} milliseconds before it checks the transaction state.
     * A second thread will terminate the transaction during the sleep interval.
     */
    public static void assertAlgorithmTermination(
        GraphDatabaseAPI db,
        Algorithm<?, ?> algorithm,
        Consumer<Algorithm<?, ?>> algoConsumer,
        long sleepMillis
    ) {
        assert sleepMillis >= 100 && sleepMillis <= 10_000;

        KernelTransaction kernelTx = GraphDatabaseApiProxy.newExplicitKernelTransaction(db, 10, TimeUnit.SECONDS);
        algorithm.withTerminationFlag(new TestTerminationFlag(kernelTx, sleepMillis));

        Runnable algorithmThread = () -> {
            try {
                algoConsumer.accept(algorithm);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };

        Runnable interruptingThread = () -> {
            try {
                Thread.sleep(sleepMillis / 2);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            kernelTx.markForTermination(Status.Transaction.TransactionMarkedAsFailed);
        };

        assertThrows(
            TransactionTerminatedException.class,
            () -> {
                try {
                    ParallelUtil.run(Arrays.asList(algorithmThread, interruptingThread), Pools.DEFAULT);
                } catch (RuntimeException e) {
                    throw e.getCause();
                }
            }
        );
    }

    public static void assertTransactionTermination(Executable executable) {
        TransactionTerminatedException exception = assertThrows(
            TransactionTerminatedException.class,
            executable
        );

        assertEquals(Status.Transaction.Terminated, exception.status());
    }
}
