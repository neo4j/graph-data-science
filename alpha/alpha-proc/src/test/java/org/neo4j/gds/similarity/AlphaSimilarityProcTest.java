/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.gds.similarity;

import org.eclipse.collections.api.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GraphCreateConfigSupport;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.GraphStoreWithConfig;
import org.neo4j.gds.impl.similarity.SimilarityAlgorithm;
import org.neo4j.gds.impl.similarity.SimilarityConfig;
import org.neo4j.gds.impl.similarity.SimilarityInput;
import org.neo4j.gds.similarity.nil.NullGraph;
import org.neo4j.gds.similarity.nil.NullGraphStore;
import org.neo4j.gds.utils.ExceptionUtil;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.gds.ElementProjection.PROJECT_ALL;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.newKernelTransaction;
import static org.neo4j.gds.config.GraphCreateFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.gds.config.GraphCreateFromCypherConfig.ALL_RELATIONSHIPS_QUERY;
import static org.neo4j.gds.config.GraphCreateFromCypherConfig.NODE_QUERY_KEY;
import static org.neo4j.gds.config.GraphCreateFromCypherConfig.RELATIONSHIP_QUERY_KEY;
import static org.neo4j.gds.config.GraphCreateFromStoreConfig.NODE_PROJECTION_KEY;
import static org.neo4j.gds.config.GraphCreateFromStoreConfig.RELATIONSHIP_PROJECTION_KEY;
import static org.neo4j.gds.similarity.AlphaSimilarityProc.SIMILARITY_FAKE_GRAPH_NAME;

public abstract class AlphaSimilarityProcTest<
    ALGORITHM extends SimilarityAlgorithm<ALGORITHM, INPUT>,
    INPUT extends SimilarityInput
    > extends BaseProcTest implements GraphCreateConfigSupport {

    abstract Class<? extends AlphaSimilarityProc<ALGORITHM, ? extends SimilarityConfig>> getProcedureClazz();

    void applyOnProcedure(Consumer<AlphaSimilarityProc<ALGORITHM, ? extends SimilarityConfig>> func) {
        try (GraphDatabaseApiProxy.Transactions transactions = newKernelTransaction(db)) {
            var proc = TestProcedureRunner.instantiateProcedure(
                db,
                getProcedureClazz(),
                transactions.tx()
            );
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

    private Stream<Method> getProcMethods(AlphaSimilarityProc<ALGORITHM, ? extends SimilarityConfig> proc) {
        return Arrays.stream(proc.getClass().getDeclaredMethods())
            .filter(method ->
                method.getDeclaredAnnotation(Procedure.class) != null &&
                Stream.of("stream", "stats", "write")
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
            emptyWithNameNative(getUsername(), "foo"), new NullGraphStore(db.databaseId())
        );
        applyOnProcedure(proc -> {
            getProcMethods(proc).forEach(method -> {
                try {
                    method.invoke(proc, "foo", minimalViableConfig());
                } catch (InvocationTargetException e) {
                    var rootCause = ExceptionUtil.rootCause(e);
                    assertEquals(IllegalArgumentException.class, rootCause.getClass());
                    assertEquals(
                        "Similarity algorithms do not support named graphs",
                        rootCause.getMessage()
                    );
                } catch (IllegalAccessException e) {
                    fail(e);
                }
            });
        });
    }

    @Test
    void shouldAcceptOmittingProjections() {
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
    void shouldAcceptIncludingProjections() {
        Map<String, Object> config = minimalViableConfig();
        config.putAll(Map.of(
            NODE_PROJECTION_KEY, PROJECT_ALL,
            RELATIONSHIP_PROJECTION_KEY, PROJECT_ALL
        ));
        applyOnProcedure(proc -> {
            getProcMethods(proc).forEach(method -> {
                try {
                    method.invoke(proc, config, Map.of());
                } catch (IllegalAccessException | InvocationTargetException e) {
                    fail(e);
                }
            });
        });

        // does not throw
    }

    @Test
    void shouldAcceptIncludingQueries() {
        Map<String, Object> config = minimalViableConfig();
        config.putAll(Map.of(
            NODE_QUERY_KEY, ALL_NODES_QUERY,
            RELATIONSHIP_QUERY_KEY, ALL_RELATIONSHIPS_QUERY
        ));
        applyOnProcedure(proc -> {
            getProcMethods(proc).forEach(method -> {
                try {
                    method.invoke(proc, config, Map.of());
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

        applyOnProcedure((proc) -> {
            getProcMethods(proc).forEach(method -> {
                try {
                    Stream<?> result = (Stream<?>) method.invoke(proc, minimalViableConfig(), Map.of());

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
            Pair<? extends SimilarityConfig, Optional<String>> input = proc.configParser().processInput(
                minimalViableConfig(),
                Map.of()
            );
            assertEquals(SIMILARITY_FAKE_GRAPH_NAME, input.getTwo().get());
            assertTrue(GraphStoreCatalog.exists(getUsername(), db.databaseId(), SIMILARITY_FAKE_GRAPH_NAME));
            GraphStoreWithConfig graphStoreWithConfig = GraphStoreCatalog.get(
                getUsername(),
                db.databaseId(), SIMILARITY_FAKE_GRAPH_NAME
            );

            GraphStore graphStore = graphStoreWithConfig.graphStore();

            assertTrue(graphStore instanceof NullGraphStore);
            assertTrue(graphStore.nodeLabels().isEmpty());
            assertTrue(graphStore.relationshipTypes().isEmpty());
            assertTrue(graphStore.getGraph(Set.of(), Set.of(), Optional.empty()) instanceof NullGraph);
        });
    }

    @Test
    void leavesNoTraceInGraphCatalog() {
        applyOnProcedure((proc) -> {
            getProcMethods(proc).forEach(method -> {
                try {
                    method.invoke(proc, minimalViableConfig(), Map.of());
                } catch (Throwable e) {
                    fail(e);
                }

                assertEquals(0, GraphStoreCatalog.graphStoresCount(db.databaseId()));
            });
        });

    }

    @Test
    void leavesNoTraceInGraphCatalogOnError() {
        applyOnProcedure((proc) -> {
            getProcMethods(proc).forEach(method -> {
                Map<String, Object> config = minimalViableConfig();
                config.put("foo", 5);
                var ignored = assertThrows(
                    InvocationTargetException.class,
                    () -> method.invoke(proc, config, Map.of())
                );
                assertEquals(0, GraphStoreCatalog.getGraphStores(getUsername()).size());
            });
        });
    }
}
