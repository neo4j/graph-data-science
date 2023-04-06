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
package org.neo4j.gds.beta.undirected;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.gdl.GdlFactory;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.INFO;

@GdlExtension
class ToUndirectedTest {

    @GdlGraph(graphNamePrefix = "directed", orientation = Orientation.NATURAL)
    private static final String DIRECTED =
        "  (a), (b), (c), (d)" +
        ", (a)-[:T1 {prop1: 42.0D, prop2: 84.0D, prop3: 1337.0D}]->(b)" +
        ", (b)-[:T1 {prop1: 1.0D, prop2: 2.0D, prop3: 3.0D}]->(a)" +
        ", (b)-[:T1 {prop1: 4.0D, prop2: 5.0D, prop3: 6.0D}]->(c)" +
        ", (a)-[:T1 {prop1: 4.0D, prop2: 5.0D, prop3: 6.0D}]->(a)";

    @GdlGraph(graphNamePrefix = "undirected", orientation = Orientation.UNDIRECTED)
    private static final String UNDIRECTED =
        "  (a), (b), (c), (d)" +
        ", (a)-[:T2 {prop1: 42.0D, prop2: 84.0D, prop3: 1337.0D}]->(b)" +
        ", (b)-[:T2 {prop1: 1.0D, prop2: 2.0D, prop3: 3.0D}]->(a)" +
        ", (b)-[:T2 {prop1: 4.0D, prop2: 5.0D, prop3: 6.0D}]->(c)" +
        ", (a)-[:T2 {prop1: 4.0D, prop2: 5.0D, prop3: 6.0D}]->(a)";

    @Inject
    GraphStore directedGraphStore;
    @Inject
    GraphStore undirectedGraphStore;
    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void shouldCreateUndirectedRelationships(int concurrency) {
        var config = ToUndirectedConfigImpl
            .builder()
            .concurrency(concurrency)
            .relationshipType("T1")
            .mutateRelationshipType("T2")
            .build();

        SingleTypeRelationships undirectedRelationships = new ToUndirected(
            directedGraphStore,
            config,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        ).compute();

        directedGraphStore.addRelationshipType(undirectedRelationships);

        for (String relationshipPropertyKey : undirectedGraphStore.relationshipPropertyKeys()) {
            assertGraphEquals(
                undirectedGraphStore.getGraph(RelationshipType.of("T2"), Optional.of(relationshipPropertyKey)),
                directedGraphStore.getGraph(RelationshipType.of("T2"), Optional.of(relationshipPropertyKey))
            );
        }
    }

    @GdlGraph(graphNamePrefix = "singleDirected", orientation = Orientation.NATURAL)
    private static final String SINGLE_PROPERTY_DIRECTED =
        "  (a), (b), (c), (d)" +
        ", (a)-[:T1 {prop1: 42.0D}]->(b)" +
        ", (b)-[:T1 {prop1: 1.0D}]->(a)" +
        ", (b)-[:T1 {prop1: 4.0D}]->(c)" +
        ", (a)-[:T1 {prop1: 4.0D}]->(a)";

    @GdlGraph(graphNamePrefix = "singleUndirected", orientation = Orientation.UNDIRECTED)
    private static final String SINGLE_PROPERTY_UNDIRECTED =
        "  (a), (b), (c), (d)" +
        ", (a)-[:T2 {prop1: 42.0D}]->(b)" +
        ", (b)-[:T2 {prop1: 1.0D}]->(a)" +
        ", (b)-[:T2 {prop1: 4.0D}]->(c)" +
        ", (a)-[:T2 {prop1: 4.0D}]->(a)";
    @Inject
    GraphStore singleDirectedGraphStore;
    @Inject
    GraphStore singleUndirectedGraphStore;

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void shouldCreateUndirectedRelationshipsWithSingleRelationshipProperty(int concurrency) {
        var config = ToUndirectedConfigImpl
            .builder()
            .concurrency(concurrency)
            .relationshipType("T1")
            .mutateRelationshipType("T2")
            .build();

        SingleTypeRelationships undirectedRelationships = new ToUndirected(
            singleDirectedGraphStore,
            config,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        ).compute();

        singleDirectedGraphStore.addRelationshipType(undirectedRelationships);

        assertGraphEquals(
            singleUndirectedGraphStore.getGraph(RelationshipType.of("T2"), Optional.of("prop1")),
            singleDirectedGraphStore.getGraph(RelationshipType.of("T2"), Optional.of("prop1"))
        );
    }

    @GdlGraph(graphNamePrefix = "noPropertyDirected", orientation = Orientation.NATURAL)
    private static final String NO_PROPERTY_DIRECTED =
        "  (a), (b), (c), (d)" +
        ", (a)-[:T1]->(b)" +
        ", (b)-[:T1]->(a)" +
        ", (b)-[:T1]->(c)" +
        ", (a)-[:T1]->(a)";

    @GdlGraph(graphNamePrefix = "noPropertyUndirected", orientation = Orientation.UNDIRECTED)
    private static final String NO_PROPERTY_UNDIRECTED =
        "  (a), (b), (c), (d)" +
        ", (a)-[:T2]->(b)" +
        ", (b)-[:T2]->(a)" +
        ", (b)-[:T2]->(c)" +
        ", (a)-[:T2]->(a)";

    @Inject
    GraphStore noPropertyDirectedGraphStore;
    @Inject
    GraphStore noPropertyUndirectedGraphStore;

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void shouldCreateUndirectedRelationshipsWithNoRelationshipProperty(int concurrency) {
        var config = ToUndirectedConfigImpl
            .builder()
            .concurrency(concurrency)
            .relationshipType("T1")
            .mutateRelationshipType("T2")
            .build();

        SingleTypeRelationships undirectedRelationships = new ToUndirected(
            noPropertyDirectedGraphStore,
            config,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        ).compute();

        noPropertyDirectedGraphStore.addRelationshipType(undirectedRelationships);

        assertGraphEquals(
            noPropertyUndirectedGraphStore.getGraph(RelationshipType.of("T2")),
            noPropertyDirectedGraphStore.getGraph(RelationshipType.of("T2"))
        );
    }

    @Test
    void shouldAggregateWithoutProperties() {
        var inputGraphStore = GdlFactory.of(
            "  (a), (b)" +
            ", (a)-[:T]->(b)" +
            ", (a)-[:T]->(b)" +
            ", (a)-[:T]->(a)").build();

        var config = ToUndirectedConfigImpl
            .builder()
            .concurrency(4)
            .relationshipType("T")
            .mutateRelationshipType("TU")
            .aggregation(Aggregation.SINGLE)
            .build();


        SingleTypeRelationships undirectedRels = new ToUndirected(
            inputGraphStore,
            config,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        ).compute();

        inputGraphStore.addRelationshipType(undirectedRels);

        Graph actualGraph = inputGraphStore.getGraph(RelationshipType.of("TU"));

        var expectedGraph =  GdlFactory.of("(a)-[:TU]->(b), (b)-[:TU]->(a), (a)-[:TU]->(a)").build().getUnion();

        assertGraphEquals(expectedGraph, actualGraph);
    }

    @Test
    void shouldAggregateWithPropertiesAndGlobalAggregation() {
        var input = GdlFactory.of(
            "  (a), (b)" +
            ", (a)-[:T {prop1: 42.0D, prop2: 84.0D}]->(b)" +
            ", (a)-[:T {prop1: 1.0D, prop2: 2.0D}]->(b)" +
            ", (a)-[:T {prop1: 4.0D, prop2: 5.0D}]->(a)").build();

        var config = ToUndirectedConfigImpl
            .builder()
            .concurrency(4)
            .relationshipType("T")
            .mutateRelationshipType("TU")
            .aggregation(Aggregation.MAX)
            .build();


        SingleTypeRelationships aggregatedUndirectedRelationships = new ToUndirected(
            input,
            config,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        ).compute();

        input.addRelationshipType(aggregatedUndirectedRelationships);

        var expectedGraphProp2 = GdlFactory.of(
            "(a)-[:TU {prop2: 84.0}]->(b)" +
            ", (b)-[:TU {prop2: 84.0}]->(a)" +
            ", (a)-[:TU {prop2: 5.0D}]->(a)").build().getUnion();


        var expectedGraphProp1 = GdlFactory.of(
            "(a)-[:TU {prop1: 42.0}]->(b)" +
            ", (b)-[:TU {prop1: 42.0}]->(a)" +
            ", (a)-[:TU {prop1: 4.0D}]->(a)").build().getUnion();


        assertGraphEquals(expectedGraphProp1, input.getGraph(RelationshipType.of("TU"), Optional.of("prop1")));
        assertGraphEquals(expectedGraphProp2, input.getGraph(RelationshipType.of("TU"), Optional.of("prop2")));
    }

    @Test
    void shouldAggregateWithPropertiesAndLocalAggregation() {
        var input = GdlFactory.of(
            "  (a), (b)" +
            ", (a)-[:T {prop1: 42.0D, prop2: 84.0D}]->(b)" +
            ", (a)-[:T {prop1: 1.0D, prop2: 2.0D}]->(b)" +
            ", (a)-[:T {prop1: 4.0D, prop2: 5.0D}]->(a)").build();

        var config = ToUndirectedConfigImpl
            .builder()
            .concurrency(4)
            .relationshipType("T")
            .mutateRelationshipType("TU")
            .aggregation(Map.of("prop1", "min", "prop2", "max"))
            .build();

        SingleTypeRelationships aggregatedUndirectedRelationships = new ToUndirected(
            input,
            config,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        ).compute();

        input.addRelationshipType(aggregatedUndirectedRelationships);

        var expectedGraphProp1 = GdlFactory.of(
            "(a)-[:TU {prop1: 1.0}]->(b)" +
            ", (b)-[:TU {prop1: 1.0}]->(a)" +
            ", (a)-[:TU {prop1: 4.0}]->(a)").build().getUnion();


        assertGraphEquals(expectedGraphProp1, input.getGraph(RelationshipType.of("TU"), Optional.of("prop1")));

        var expectedGraphProp2 = GdlFactory.of(
            "(a)-[:TU {prop2: 84.0}]->(b)" +
            ", (b)-[:TU {prop2: 84.0}]->(a)" +
            ", (a)-[:TU {prop2: 5.0D}]->(a)").build().getUnion();

        assertGraphEquals(expectedGraphProp2, input.getGraph(RelationshipType.of("TU"), Optional.of("prop2")));
    }

    @Test
    void shouldLogProgress() {
        var log = Neo4jProxy.testLog();

        var config = ToUndirectedConfigImpl
            .builder()
            .concurrency(4)
            .relationshipType("T1")
            .mutateRelationshipType("T2")
            .build();

        ToUndirected toUndirected = new ToUndirectedAlgorithmFactory().build(
            directedGraphStore,
            config,
            log,
            EmptyTaskRegistryFactory.INSTANCE
        );

        toUndirected.compute();

        var messagesInOrder = log.getMessages(INFO);

        assertThat(messagesInOrder)
            // avoid asserting on the thread id
            .extracting(removingThreadId())
            .hasSize(11)
            .containsSequence(
                "ToUndirected :: Create Undirected Relationships :: Start",
                "ToUndirected :: Create Undirected Relationships 25%",
                "ToUndirected :: Create Undirected Relationships 50%",
                "ToUndirected :: Create Undirected Relationships 75%",
                "ToUndirected :: Create Undirected Relationships 100%",
                "ToUndirected :: Create Undirected Relationships :: Finished"
            )
            .containsSequence(
                "ToUndirected :: Build undirected Adjacency list :: Start",
                "ToUndirected :: Build undirected Adjacency list 100%",
                "ToUndirected :: Build undirected Adjacency list :: Finished"
            );
    }

}
