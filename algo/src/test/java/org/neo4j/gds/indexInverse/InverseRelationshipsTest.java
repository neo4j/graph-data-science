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
package org.neo4j.gds.indexInverse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.logging.GdsTestLog;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.INFO;

@GdlExtension
class InverseRelationshipsTest {
    @GdlGraph(orientation = Orientation.NATURAL)
    @GdlGraph(orientation = Orientation.REVERSE, graphNamePrefix = "inverse")
    private static final String DIRECTED =
        "  (a), (b), (c), (d)" +
        ", (a)-[:T1 {prop1: 42.0D, prop2: 84.0D, prop3: 1337.0D}]->(b)" +
        ", (b)-[:T1 {prop1: 1.0D, prop2: 2.0D, prop3: 3.0D}]->(a)" +
        ", (b)-[:T2 {prop1: 4.0D, prop2: 5.0D, prop3: 6.0D}]->(c)" +
        ", (a)-[:T1 {prop1: 4.0D, prop2: 5.0D, prop3: 6.0D}]->(a)";

    @Inject
    private GraphStore graphStore;

    @Inject
    private GraphStore inverseGraphStore;

    static Stream<Object> multipleTypes() {
        return Stream.of(List.of("T1", "T2"), "*");
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void shouldCreateIndexedRelationships(int concurrency) {
        var relationshipType = RelationshipType.of("T1");

        var parameters = new InverseRelationshipsParameters(new Concurrency(concurrency), List.of(relationshipType.name));

        var inverseRelationshipsPerType = new InverseRelationships(
            graphStore,
            parameters,
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE
        ).compute();

        assertThat(inverseRelationshipsPerType).hasSize(1);

        // we need to use the same name for assertGraphEquals to work
        graphStore.deleteRelationships(relationshipType);
        graphStore.addRelationshipType(inverseRelationshipsPerType.get(relationshipType));

        for (String relationshipPropertyKey : inverseGraphStore.relationshipPropertyKeys()) {
            assertGraphEquals(
                inverseGraphStore.getGraph(relationshipType, Optional.of(relationshipPropertyKey)),
                graphStore.getGraph(relationshipType, Optional.of(relationshipPropertyKey))
                );
        }
    }

    @ParameterizedTest
    @MethodSource("multipleTypes")
    void shouldIndexMultipleTypes(Object relTypes) {
        var parameters = new InverseRelationshipsParameters(ConcurrencyConfig.TYPED_DEFAULT_CONCURRENCY, InverseRelationshipsConfig.parseRelTypes(relTypes));

        var internalTypes = List.copyOf(parameters.internalRelationshipTypes(graphStore));

        var inverseRelationshipsPerType = new InverseRelationships(
            graphStore,
            parameters,
            ProgressTracker.NULL_TRACKER,
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE
        ).compute();

        assertThat(inverseRelationshipsPerType).hasSize(internalTypes.size());

        internalTypes.forEach(internalType -> {
            // we need to use the same name for assertGraphEquals to work
            graphStore.deleteRelationships(internalType);
            graphStore.addRelationshipType(inverseRelationshipsPerType.get(internalType));
        });

        for (String relationshipPropertyKey : inverseGraphStore.relationshipPropertyKeys()) {
            assertGraphEquals(
                inverseGraphStore.getGraph(internalTypes, Optional.of(relationshipPropertyKey)),
                graphStore.getGraph(internalTypes, Optional.of(relationshipPropertyKey))
            );
        }
    }

    @Test
    void logProgress() {
        var log = new GdsTestLog();

        var parameters = new InverseRelationshipsParameters(new Concurrency(4), List.of("T1", "T2"));

        var factory = new InverseRelationshipsAlgorithmFactory();
        var task = factory.progressTask(graphStore.nodeCount(), parameters.internalRelationshipTypes(graphStore));
        var progressTracker = new TestProgressTracker(task, log, parameters.concurrency(), EmptyTaskRegistryFactory.INSTANCE);
        factory.build(graphStore, parameters, progressTracker).compute();

        assertThat(log.getMessages(INFO))
            .extracting(removingThreadId())
            .containsExactly(
                "IndexInverse :: Start",
                "IndexInverse :: Create inverse relationships of type 'T1' :: Start",
                "IndexInverse :: Create inverse relationships of type 'T1' 25%",
                "IndexInverse :: Create inverse relationships of type 'T1' 50%",
                "IndexInverse :: Create inverse relationships of type 'T1' 75%",
                "IndexInverse :: Create inverse relationships of type 'T1' 100%",
                "IndexInverse :: Create inverse relationships of type 'T1' :: Finished",
                "IndexInverse :: Build Adjacency list :: Start",
                "IndexInverse :: Build Adjacency list 100%",
                "IndexInverse :: Build Adjacency list :: Finished",
                "IndexInverse :: Create inverse relationships of type 'T2' :: Start",
                "IndexInverse :: Create inverse relationships of type 'T2' 25%",
                "IndexInverse :: Create inverse relationships of type 'T2' 50%",
                "IndexInverse :: Create inverse relationships of type 'T2' 75%",
                "IndexInverse :: Create inverse relationships of type 'T2' 100%",
                "IndexInverse :: Create inverse relationships of type 'T2' :: Finished",
                "IndexInverse :: Build Adjacency list :: Start",
                "IndexInverse :: Build Adjacency list 100%",
                "IndexInverse :: Build Adjacency list :: Finished",
                "IndexInverse :: Finished"
            );
    }
}
