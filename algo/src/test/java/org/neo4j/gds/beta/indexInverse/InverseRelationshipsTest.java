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
package org.neo4j.gds.beta.indexInverse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImportResult;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.Optional;

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
        ", (b)-[:T1 {prop1: 4.0D, prop2: 5.0D, prop3: 6.0D}]->(c)" +
        ", (a)-[:T1 {prop1: 4.0D, prop2: 5.0D, prop3: 6.0D}]->(a)";

    @Inject
    GraphStore graphStore;

    @Inject
    GraphStore inverseGraphStore;

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void shouldCreateIndexedRelationships(int concurrency) {
        var config = InverseRelationshipsConfigImpl
            .builder()
            .concurrency(concurrency)
            .relationshipType("T1")
            .build();

        SingleTypeRelationshipImportResult inverseRelationships = new InverseRelationships(
            graphStore,
            config,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        ).compute();

        var inverseType = RelationshipType.of(config.relationshipType());

        // we need to use the same name for assertGraphEquals to work
        graphStore.deleteRelationships(inverseType);
        graphStore.addRelationshipType(inverseType, inverseRelationships);

        for (String relationshipPropertyKey : inverseGraphStore.relationshipPropertyKeys()) {
            assertGraphEquals(
                inverseGraphStore.getGraph(inverseType, Optional.of(relationshipPropertyKey)),
                graphStore.getGraph(inverseType, Optional.of(relationshipPropertyKey))
                );
        }
    }

    @Test
    void logProgress() {
        var log = Neo4jProxy.testLog();

        var config = InverseRelationshipsConfigImpl
            .builder()
            .concurrency(4)
            .relationshipType("T1")
            .build();

        new InverseRelationshipsAlgorithmFactory().build(
            graphStore,
            config,
            log,
            EmptyTaskRegistryFactory.INSTANCE
        ).compute();

        assertThat(log.getMessages(INFO))
            .extracting(removingThreadId())
            .containsExactly(
                "IndexInverse :: Start",
                "IndexInverse :: Create inverse relationships :: Start",
                "IndexInverse :: Create inverse relationships 25%",
                "IndexInverse :: Create inverse relationships 50%",
                "IndexInverse :: Create inverse relationships 75%",
                "IndexInverse :: Create inverse relationships 100%",
                "IndexInverse :: Create inverse relationships :: Finished",
                "IndexInverse :: Build Adjacency list :: Start",
                "IndexInverse :: Build Adjacency list 100%",
                "IndexInverse :: Build Adjacency list :: Finished",
                "IndexInverse :: Finished"
            );
    }
}
