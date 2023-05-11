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
package org.neo4j.gds.catalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.EmptyDependencyResolver;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.TerminationMonitor;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.executor.ImmutableExecutionContext;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@GdlExtension
class GraphListOperatorTest {
    @GdlGraph(idOffset = 42)
    private static final String DB_CYPHER ="CREATE (:A {foo: 1})-[:REL {bar: 2}]->(:A)";

    @Inject
    private GraphStore graphStore;


    @GdlGraph(idOffset = 42,orientation = Orientation.REVERSE,graphNamePrefix = "reverse")
    private static final String REVERSE_DB_CYPHER ="CREATE (a:Person), (b:Person), (a)-[:INTERACTS]->(b)";

    @Inject
    private GraphStore reverseGraphStore;


    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void listAllAvailableGraphsForUser() {
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("Alice", "aliceGraph"), graphStore);
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("Bob", "bobGraph"), graphStore);

        var executionContextAlice = executionContextBuilder("Alice")
            .build();
        var executionContextBob = executionContextBuilder("Bob")
            .build();

        var resultAlice=GraphListOperator.list(GraphListOperator.NO_VALUE,executionContextAlice);
        var resultBob=GraphListOperator.list(GraphListOperator.NO_VALUE,executionContextBob);
        assertThat(resultAlice.findFirst().get().graphName).isEqualTo("aliceGraph");
        assertThat(resultBob.findFirst().get().graphName).isEqualTo("bobGraph");

    }

    @Test
    void shouldHaveCreationTimeField(){
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("Alice", "aliceGraph"), graphStore);
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("Bob", "bobGraph"), graphStore);
        var executionContextAlice = executionContextBuilder("Alice")
            .build();
        var executionContextBob = executionContextBuilder("Bob")
            .build();
        AtomicReference<String> creationTimeAlice = new AtomicReference<>();
        AtomicReference<String> creationTimeBob = new AtomicReference<>();

        var resultAlice=GraphListOperator.list(GraphListOperator.NO_VALUE,executionContextAlice);
        var resultBob=GraphListOperator.list(GraphListOperator.NO_VALUE,executionContextBob);
        creationTimeAlice.set(formatCreationTime(resultAlice.findFirst().get().creationTime));
        creationTimeAlice.set(formatCreationTime(resultBob.findFirst().get().creationTime));

        assertNotEquals(creationTimeAlice.get(), creationTimeBob.get());

    }

    @Test
    void returnEmptyStreamWhenNoGraphsAreLoaded()
    {
        var executionContextAlice = executionContextBuilder("Alice")
            .build();
        var resultAlice=GraphListOperator.list(GraphListOperator.NO_VALUE,executionContextAlice);
        assertThat(resultAlice.count()).isEqualTo(0L);
    }

    @Test
    void reverseProjectionForListing(){
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("user", "graph"), reverseGraphStore);

        var executionContext = executionContextBuilder("user")
            .build();
        var result=GraphListOperator.list(GraphListOperator.NO_VALUE,executionContext);
        assertThat(result.findFirst().get().nodeCount).isEqualTo(2L);
    }

    private ImmutableExecutionContext.Builder executionContextBuilder(String username) {
        return ImmutableExecutionContext
            .builder()
            .databaseId(graphStore.databaseId())
            .dependencyResolver(EmptyDependencyResolver.INSTANCE)
            .returnColumns(ProcedureReturnColumns.EMPTY)
            .userLogRegistryFactory(EmptyUserLogRegistryFactory.INSTANCE)
            .taskRegistryFactory(EmptyTaskRegistryFactory.INSTANCE)
            .username(username)
            .terminationMonitor(TerminationMonitor.EMPTY)
            .closeableResourceRegistry(CloseableResourceRegistry.EMPTY)
            .algorithmMetaDataSetter(AlgorithmMetaDataSetter.EMPTY)
            .nodeLookup(NodeLookup.EMPTY)
            .log(Neo4jProxy.testLog())
            .isGdsAdmin(false);
    }
    private String formatCreationTime(ZonedDateTime zonedDateTime) {
        return ISO_LOCAL_DATE_TIME.format(zonedDateTime);
    }

}
