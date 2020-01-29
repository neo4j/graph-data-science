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
package org.neo4j.graphalgo.impl.similarity;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongHashSet;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.IdMap;
import org.neo4j.graphalgo.core.loading.IdMapBuilder;
import org.neo4j.graphalgo.core.loading.IdsAndProperties;
import org.neo4j.graphalgo.core.loading.NodeImporter;
import org.neo4j.graphalgo.core.loading.NodesBatchBuffer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.graphalgo.results.SimilarityResult;

import java.util.Collections;
import java.util.HashMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.ArrayMatching.arrayContainingInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;

class HugeRelationshipsBuilderTest {

    @Test
    void findOutgoing() {
        int numberOfNodes = 100;
        IdsAndProperties nodes = createNodes(numberOfNodes);

        HugeRelationshipsBuilder.HugeRelationshipsBuilderWithBuffer builder = new HugeRelationshipsBuilder(nodes).withBuffer();

        builder.addRelationship(0, 1);
        builder.addRelationship(0, 2);
        builder.addRelationship(0, 3);
        builder.addRelationship(0, 4);
        builder.addRelationship(0, 5);

        Graph graph = ANNUtils.createGraphsByRelationshipType(nodes, builder.build()).getUnion();

        final LongArrayList rels = new LongArrayList();
        graph.forEachRelationship(0L, (sourceNodeId, targetNodeId) -> {
            rels.add(targetNodeId);
            return true;
        });

        long[] relationships = rels.toArray();
        assertEquals(5, relationships.length);
        assertThat(ArrayUtils.toObject(relationships), arrayContainingInAnyOrder( 1L, 2L, 3L, 4L, 5L));
    }

    @Test
    void findIncoming() {
        int numberOfNodes = 100;
        IdsAndProperties nodes = createNodes(numberOfNodes);

        HugeRelationshipsBuilder.HugeRelationshipsBuilderWithBuffer builder = new HugeRelationshipsBuilder(nodes).withBuffer();

        builder.addRelationship(0, 1);
        builder.addRelationship(0, 2);
        builder.addRelationship(0, 3);
        builder.addRelationship(0, 4);
        builder.addRelationship(0, 5);

        Graph graph = ANNUtils.createGraphsByRelationshipType(nodes, builder.build()).getUnion();

        final LongArrayList rels = new LongArrayList();
        graph.forEachRelationship(1L, (sourceNodeId, targetNodeId) -> {
            rels.add(targetNodeId);
            return true;
        });

        long[] relationships = rels.toArray();
        assertEquals(1, relationships.length);
        assertThat(ArrayUtils.toObject(relationships), arrayContainingInAnyOrder( 0L));
    }

    @Test
    void ignoreUnmappedNodes() {
        IdsAndProperties nodes = buildNodes(new WeightedInput[] {
                new WeightedInput(0, new double[] { 1.0, 0.9, 0.8}),
                new WeightedInput(1, new double[] { 1.0, 0.9, 0.8}),
                new WeightedInput(2, new double[] { 1.0, 0.9, 0.8}),
        });

        HugeRelationshipsBuilder.HugeRelationshipsBuilderWithBuffer builder = new HugeRelationshipsBuilder(nodes).withBuffer();

        AnnTopKConsumer annTopKConsumer = new AnnTopKConsumer(3, SimilarityResult.DESCENDING);
        annTopKConsumer.applyAsInt(new SimilarityResult(0, 1, -1, -1, -1, 0.8));
        annTopKConsumer.applyAsInt(new SimilarityResult(0, 2, -1, -1, -1, 0.7));
        annTopKConsumer.applyAsInt(new SimilarityResult(0, 200, -1, -1, -1, 0.6));

        builder.addRelationshipsFrom(new AnnTopKConsumer[] {annTopKConsumer});

        Graph graph = ANNUtils.createGraphsByRelationshipType(nodes, builder.build()).getUnion();

        final LongArrayList rels = new LongArrayList();
        graph.forEachRelationship(0L, (sourceNodeId, targetNodeId) -> {
            rels.add(targetNodeId);
            return true;
        });

        long[] relationships = rels.toArray();
        assertEquals(2, relationships.length);
        assertThat(ArrayUtils.toObject(relationships), arrayContainingInAnyOrder( 1L, 2L));
    }

    private IdsAndProperties createNodes(int numberOfNodes) {
        HugeLongArrayBuilder idMapBuilder = HugeLongArrayBuilder.of(numberOfNodes, AllocationTracker.EMPTY);
        NodeImporter nodeImporter = new NodeImporter(idMapBuilder, null);
        NodesBatchBuffer buffer = new NodesBatchBuffer(null, new LongHashSet(), numberOfNodes, false);

        for (int i = 0; i < numberOfNodes; i++) {
            buffer.add(i, -1);
        }
        nodeImporter.importNodes(buffer, null);

        IdMap idMap = IdMapBuilder.build(idMapBuilder, numberOfNodes-1, 1, AllocationTracker.EMPTY);
        return new IdsAndProperties(idMap, new HashMap<>());
    }

    private IdsAndProperties buildNodes(final WeightedInput[] inputs) {
        HugeLongArrayBuilder idMapBuilder = HugeLongArrayBuilder.of(inputs.length, AllocationTracker.EMPTY);
        NodeImporter nodeImporter = new NodeImporter(idMapBuilder, null);
        long maxNodeId = 0L;

        NodesBatchBuffer buffer = new NodesBatchBuffer(null, new LongHashSet(), inputs.length, false);

        for (WeightedInput input : inputs) {
            if (input.getId() > maxNodeId) {
                maxNodeId = input.getId();
            }
            buffer.add(input.getId(), -1);
            if (buffer.isFull()) {
                nodeImporter.importNodes(buffer, null);
                buffer.reset();
            }
        }
        nodeImporter.importNodes(buffer, null);

        IdMap idMap = IdMapBuilder.build(idMapBuilder, maxNodeId, 1, AllocationTracker.EMPTY);
        return new IdsAndProperties(idMap, Collections.emptyMap());
    }

}
