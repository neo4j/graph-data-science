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
import org.neo4j.gds.api.CompositeRelationshipIterator;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.CollectingMultiplePropertiesConsumer;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImportResult;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.TestLog.INFO;

@GdlExtension
class IndexInverseTest {
    @GdlGraph(orientation = Orientation.NATURAL)
    private static final String DIRECTED =
        "  (a), (b), (c), (d)" +
        ", (a)-[:T1 {prop1: 42.0D, prop2: 84.0D, prop3: 1337.0D}]->(b)" +
        ", (b)-[:T1 {prop1: 1.0D, prop2: 2.0D, prop3: 3.0D}]->(a)" +
        ", (b)-[:T1 {prop1: 4.0D, prop2: 5.0D, prop3: 6.0D}]->(c)" +
        ", (a)-[:T1 {prop1: 4.0D, prop2: 5.0D, prop3: 6.0D}]->(a)";

    @Inject
    GraphStore graphStore;

    @Inject
    IdFunction idFunction;

    @ParameterizedTest
    @ValueSource(ints = {1, 4})
    void shouldCreateIndexedRelationships(int concurrency) {
        var config = IndexInverseConfigImpl
            .builder()
            .concurrency(concurrency)
            .relationshipType("T1")
            .mutateRelationshipType("T2")
            .build();

        SingleTypeRelationshipImportResult indexedRelationships = new IndexInverse(
            graphStore,
            config,
            ProgressTracker.NULL_TRACKER,
            Pools.DEFAULT
        ).compute();

        assertThat(indexedRelationships.inverseTopology()).isPresent();

        var indexedType = RelationshipType.of(config.mutateRelationshipType());

        graphStore.addRelationshipType(indexedType, indexedRelationships);

        long aId = idFunction.of("a");
        long bId = idFunction.of("b");
        long cId = idFunction.of("c");

        var expected = Map.of(
            aId, Map.of(aId, List.of(new double[] {4.0, 5.0, 6.0}), bId, List.of(new double[] {1.0, 2.0, 3.0})),
            bId, Map.of(aId, List.of(new double[] {42.0, 84.0, 1337.0})),
            cId, Map.of(bId, List.of(new double[] {4.0, 5.0, 6.0}))
        );

        var consumer = new CollectingMultiplePropertiesConsumer();
        CompositeRelationshipIterator iterator = graphStore.getCompositeRelationshipIterator(
            indexedType,
            List.of("prop1", "prop2", "prop3")
        );

        graphStore.nodes().forEachNode(nodeId -> {
            iterator.forEachInverseRelationship(nodeId, consumer);
            return true;
        });

        assertThat(consumer.seenRelationships).usingRecursiveComparison().isEqualTo(expected);
    }

    @Test
    void logProgress() {
        var log = Neo4jProxy.testLog();

        var config = IndexInverseConfigImpl
            .builder()
            .concurrency(4)
            .relationshipType("T1")
            .mutateRelationshipType("T2")
            .build();

        new IndexInverseAlgorithmFactory().build(
            graphStore,
            config,
            log,
            EmptyTaskRegistryFactory.INSTANCE
        ).compute();

        assertThat(log.getMessages(INFO))
            .extracting(removingThreadId())
            .containsExactly(
                "IndexInverse :: Start",
                "IndexInverse :: Create inversely indexed relationships :: Start",
                "IndexInverse :: Create inversely indexed relationships 25%",
                "IndexInverse :: Create inversely indexed relationships 50%",
                "IndexInverse :: Create inversely indexed relationships 75%",
                "IndexInverse :: Create inversely indexed relationships 100%",
                "IndexInverse :: Create inversely indexed relationships :: Finished",
                "IndexInverse :: Build Adjacency list :: Start",
                "IndexInverse :: Build Adjacency list 100%",
                "IndexInverse :: Build Adjacency list :: Finished",
                "IndexInverse :: Finished"
            );
    }
}
