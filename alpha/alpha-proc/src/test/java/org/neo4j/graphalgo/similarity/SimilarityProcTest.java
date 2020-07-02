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
package org.neo4j.graphalgo.similarity;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GraphCreateConfigSupport;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.compat.GraphDatabaseApiProxy;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.similarity.SimilarityAlgorithm;
import org.neo4j.graphalgo.impl.similarity.SimilarityConfig;
import org.neo4j.graphalgo.impl.similarity.SimilarityInput;
import org.neo4j.graphalgo.similarity.nil.NullGraph;
import org.neo4j.graphalgo.similarity.nil.NullGraphLoader;
import org.neo4j.graphalgo.similarity.nil.NullGraphStore;
import org.neo4j.graphalgo.utils.ExceptionUtil;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Procedure;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphalgo.ElementProjection.PROJECT_ALL;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.newKernelTransaction;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.NODE_PROJECTION_KEY;
import static org.neo4j.graphalgo.config.GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;

public abstract class SimilarityProcTest<
    ALGORITHM extends SimilarityAlgorithm<ALGORITHM, INPUT>,
    INPUT extends SimilarityInput
    > extends BaseProcTest implements GraphCreateConfigSupport {

    abstract Class<? extends SimilarityProc<ALGORITHM, ? extends SimilarityConfig>> getProcedureClazz();

    void applyOnProcedure(Consumer<? super SimilarityProc<ALGORITHM, ? extends SimilarityConfig>> func) {
        try (GraphDatabaseApiProxy.Transactions transactions = newKernelTransaction(db)) {
            SimilarityProc<ALGORITHM, ? extends SimilarityConfig> proc;
            try {
                proc = getProcedureClazz().getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Could not instantiate Procedure Class " + getProcedureClazz().getSimpleName());
            }

            proc.transaction = transactions.ktx();
            proc.api = db;
            proc.callContext = ProcedureCallContext.EMPTY;
            proc.log = new TestLog();

            func.accept(proc);
        }
    }

    private String getProcedureMethodName(Method method) {
        Procedure procedureAnnotation = method.getDeclaredAnnotation(Procedure.class);
        Objects.requireNonNull(procedureAnnotation, method + " is not annotation with " + Procedure.class);
        String name = procedureAnnotation.name();
        if (name.isEmpty()) {
            name = procedureAnnotation.value();
        }
        return name;
    }

    private Stream<Method> getProcMethods(SimilarityProc<ALGORITHM, ? extends SimilarityConfig> proc) {
        return Arrays.stream(proc.getClass().getDeclaredMethods())
            .filter(method ->
                method.getDeclaredAnnotation(Procedure.class) != null && Set
                    .of("stream", "stats", "write")
                    .stream()
                    .anyMatch(mode -> getProcedureMethodName(method).endsWith(mode))
            );
    }

    Map<String, Object> minimalViableConfig() {
        return new HashMap<>();
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void throwsOnExplicitGraph() {
        GraphStoreCatalog.set(
            emptyWithNameNative(getUsername(), "foo"), new NullGraphStore()
        );
        applyOnProcedure(proc -> {
            getProcMethods(proc).forEach(method -> {
                try {
                    method.invoke(proc, "foo", minimalViableConfig());
                } catch (InvocationTargetException e) {
                    var rootCause = ExceptionUtil.rootCause(e);
                    assertEquals(IllegalArgumentException.class, rootCause.getClass());
                    assertEquals(
                        "Similarity algorithms does not support named graphs",
                        rootCause.getMessage()
                    );
                } catch (IllegalAccessException e) {
                    fail(e);
                }
            });
        });
    }

    @Test
    void shouldAcceptNoProjections() {
        applyOnProcedure(proc -> {
            getProcMethods(proc).forEach(method -> {
                try {
                    method.invoke(proc, minimalViableConfig(), Map.of());
                } catch (IllegalAccessException | InvocationTargetException e) {
                    fail(e);
                }
            });
        });

        // does not throw
    }

    @Test
    void worksOnEmptyGraph() {
        runQuery("MATCH (n) DETACH DELETE n");
        Map<String, Object> config = minimalViableConfig();
        config.putAll(Map.of(
            NODE_PROJECTION_KEY, PROJECT_ALL,
            RELATIONSHIP_PROJECTION_KEY, PROJECT_ALL
        ));

        applyOnProcedure((proc) -> {
            getProcMethods(proc).forEach(method -> {
                try {
                    Stream<?> result = (Stream<?>) method.invoke(proc, config, Map.of());

                    if (getProcedureMethodName(method).endsWith("stream")) {
                        assertEquals(0, result.count(), "Stream result should be empty.");
                    } else {
                        assertEquals(1, result.count());
                    }
                } catch (Throwable e) {
                    fail(e);
                }
            });
        });
    }

    @Test
    void shouldNotLoadAnything() {
        applyOnProcedure(proc -> {
            GraphLoader graphLoader = proc.newLoader(GraphCreateConfig.createImplicit(
                getUsername(),
                CypherMapWrapper.create(Map.of(
                    NODE_PROJECTION_KEY, PROJECT_ALL,
                    RELATIONSHIP_PROJECTION_KEY, PROJECT_ALL
                ))
            ), AllocationTracker.EMPTY);

            assertTrue(graphLoader instanceof NullGraphLoader);
            assertTrue(graphLoader.graph() instanceof NullGraph);
            assertTrue(graphLoader.graphStore() instanceof NullGraphStore);
            assertTrue(graphLoader.graphStore().nodeLabels().isEmpty());
            assertTrue(graphLoader.graphStore().relationshipTypes().isEmpty());
            assertTrue(graphLoader.graphStore().getGraph(Set.of(), Set.of(), Optional.empty()) instanceof NullGraph);
        });
    }
}
