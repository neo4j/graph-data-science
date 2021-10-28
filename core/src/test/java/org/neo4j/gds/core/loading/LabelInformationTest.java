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

import com.carrotsearch.hppc.IntObjectHashMap;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.core.utils.mem.AllocationTracker;

import java.util.Collection;
import java.util.List;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

class LabelInformationTest {

    @Test
    void singleLabelAssignment() {
        var nodeIds = LongStream.range(0, 10).boxed().collect(Collectors.toList());
        testLabelAssignment(nodeIds, node -> node);
    }

    @Test
    void singleLabelAssignmentWithNonDirectMapping() {
        var nodeMapping = LongStream
            .range(0, 10)
            .boxed()
            .collect(Collectors.toMap(nodeId -> 42 * (nodeId + 1337), nodeId -> nodeId));
            testLabelAssignment(nodeMapping.keySet(), nodeMapping::get);
    }

    private void testLabelAssignment(Collection<Long> nodeIds, LongUnaryOperator nodeIdMapping) {
        var label = NodeLabel.of("A");
        var nodeCount = nodeIds.size();
        var tokenLabelsMap = new IntObjectHashMap<List<NodeLabel>>();

        tokenLabelsMap.put(0, List.of(label));

        var builder = LabelInformation.builder(nodeCount, tokenLabelsMap, AllocationTracker.empty());

        for (var nodeId : nodeIds) {
            builder.addNodeIdToLabel(label, nodeId);
        }

        var labelInformation = builder.build(nodeCount, nodeIdMapping);

        for (var nodeId : nodeIds) {
            assertThat(labelInformation.hasLabel(nodeIdMapping.applyAsLong(nodeId), label)).isTrue();
        }
    }
}
