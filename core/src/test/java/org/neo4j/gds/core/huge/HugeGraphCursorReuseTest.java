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
package org.neo4j.gds.core.huge;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.ImmutableTopology;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;

class HugeGraphCursorReuseTest {

    @Test
    void shouldReuseCursors() {
        var baseGraph = RandomGraphGenerator.builder()
            .nodeCount(100)
            .averageDegree(5)
            .relationshipDistribution(RelationshipDistribution.RANDOM)
            .orientation(Orientation.NATURAL)
            .build()
            .generate();

        var adjacencyList = new TestAdjacencyList();

        var mockTopology = ImmutableTopology
            .builder()
            .adjacencyList(adjacencyList)
            .elementCount(baseGraph.relationshipTopology().elementCount())
            .isMultiGraph(false)
            .build();

        var mockGraph = HugeGraph.create(
            baseGraph.idMap,
            baseGraph.schema(),
            Map.of(),
            mockTopology,
            Optional.empty()
        );

        mockGraph.forEachNode(nodeId -> {
            mockGraph.forEachRelationship(nodeId, (source, target) -> true);
            return true;
        });

        assertThat(adjacencyList.cursorInstanceCount()).isEqualTo(1L);
    }


    private static class TestAdjacencyList implements AdjacencyList {
        private LongAdder cursorInstanceCounter;

        private TestAdjacencyList() {this.cursorInstanceCounter = new LongAdder();}

        public long cursorInstanceCount() {
            return cursorInstanceCounter.longValue();
        }

        @Override
        public int degree(long node) {
            return 0;
        }

        @Override
        public AdjacencyCursor adjacencyCursor(long node, double fallbackValue) {
            return rawAdjacencyCursor();
        }

        @Override
        public AdjacencyCursor adjacencyCursor(@Nullable AdjacencyCursor reuse, long node, double fallbackValue) {
            return reuse instanceof TestAdjacencyCursor ? reuse : rawAdjacencyCursor();
        }

        @Override
        public AdjacencyCursor rawAdjacencyCursor() {
            cursorInstanceCounter.increment();
            return new TestAdjacencyCursor();
        }

        @Override
        public void close() {

        }
    }

    private static class TestAdjacencyCursor implements AdjacencyCursor {

        @Override
        public void init(long index, int degree) {

        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean hasNextVLong() {
            return false;
        }

        @Override
        public long nextVLong() {
            return 0;
        }

        @Override
        public long peekVLong() {
            return 0;
        }

        @Override
        public int remaining() {
            return 0;
        }

        @Override
        public long skipUntil(long nodeId) {
            return 0;
        }

        @Override
        public long advance(long nodeId) {
            return 0;
        }

        @Override
        public long advanceBy(int n) {
            return NOT_FOUND;
        }

        @Override
        public @NotNull AdjacencyCursor shallowCopy(@Nullable AdjacencyCursor destination) {
            return new TestAdjacencyCursor();
        }

        @Override
        public void close() {

        }
    }
}
