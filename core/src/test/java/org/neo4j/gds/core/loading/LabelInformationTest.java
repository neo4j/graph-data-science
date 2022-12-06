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
package org.neo4j.gds.core.loading;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.NodeLabel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class LabelInformationTest {

    @Test
    void singleLabelAssignment() {
        var nodeIds = LongStream.range(0, 10).boxed().collect(Collectors.toList());
        testLabelAssignment(nodeIds, node -> node);
    }

    @Test
    void singleLabelAssignmentWithNonDirectMapping() {
        var idMap = LongStream
            .range(0, 10)
            .boxed()
            .collect(Collectors.toMap(nodeId -> 42 * (nodeId + 1337), nodeId -> nodeId));
            testLabelAssignment(idMap.keySet(), idMap::get);
    }

    @ParameterizedTest
    @MethodSource("nodeIteratorTestCombinations")
    void testNodeIterator(LabelProducer labelProducer, Collection<NodeLabel> selectedLabels, Iterable<Long> expectedIds) {

        var nodeIds = LongStream.range(0, 10).boxed().collect(Collectors.toList());
        var labelInformation = buildLabelInformation(nodeIds, node -> node, labelProducer);
        var iter = labelInformation.nodeIterator(selectedLabels, 10);

        var actual = new ArrayList<Long>();
        while (iter.hasNext()) {
            actual.add(iter.nextLong());
        }

        assertThat(actual).containsExactlyElementsOf(expectedIds);
    }

    static Stream<Arguments> nodeIteratorTestCombinations() {
        var labelA = NodeLabel.listOf("A");
        var labelB = NodeLabel.listOf("B");
        var labelAB = NodeLabel.listOf("A", "B");
        var anyLabel = List.of(NodeLabel.ALL_NODES);
        var all = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        var even = List.of(0L, 2L, 4L, 6L, 8L);
        return Stream.of(
            arguments((LabelProducer) (id) -> labelA, labelA, all),
            arguments((LabelProducer) (id) -> id % 2 == 0 ? labelA : labelB, labelAB, all),
            arguments((LabelProducer) (id) -> id % 2 == 0 ? labelA : labelB, labelA, even),
            arguments((LabelProducer) (id) -> id % 2 == 0 ? labelA : labelB, anyLabel, all)
        );
    }


    private void testLabelAssignment(Collection<Long> nodeIds, LongUnaryOperator nodeIdMap) {
        var label = NodeLabel.of("A");
        var labelInformation = buildLabelInformation(nodeIds, nodeIdMap, (__) -> Set.of(label));

        for (var nodeId : nodeIds) {
            assertThat(labelInformation.hasLabel(nodeIdMap.applyAsLong(nodeId), label)).isTrue();
        }
    }

    private LabelInformation buildLabelInformation(Collection<Long> nodeIds, LongUnaryOperator nodeIdMap, LabelProducer labelProducer) {
        var nodeCount = nodeIds.size();
        var builder = LabelInformationBuilders.multiLabelWithCapacity(nodeCount);

        for (var nodeId : nodeIds) {
            for (NodeLabel nodeLabel : labelProducer.get(nodeId)) {
                builder.addNodeIdToLabel(nodeLabel, nodeId);
            }
        }

        return builder.build(nodeCount, nodeIdMap);
    }

    interface LabelProducer {
        Collection<NodeLabel> get(long nodeId);
    }
}
