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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.core.loading.construction.NodeLabelTokens;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class HighLimitIdMapTest {

    private HighLimitIdMap idMap;

    @BeforeEach
    void setup() {
        var builder = new LazyIdMapBuilder(4, true, false);
        builder.addNode(1000, NodeLabelTokens.ofStrings("A"));
        builder.addNode(2000, NodeLabelTokens.ofStrings("B"));
        builder.addNode(3000, NodeLabelTokens.ofStrings("C"));

        this.idMap = builder.build().idMap();
    }

    @Test
    void testToRootNodeId() {
        idMap.forEachNode(nodeId -> {
            assertThat(idMap.toRootNodeId(nodeId)).isEqualTo(nodeId);
            return true;
        });
    }

    @Nested
    class FilteredHighLimitIdMap {

        @Test
        void testToRootNodeId() {
            var filteredIdMapA = idMap.withFilteredLabels(List.of(NodeLabel.of("A")), 1).get();
            long expectedA = idMap.toMappedNodeId(1000);
            assertThat(filteredIdMapA.toRootNodeId(0)).isEqualTo(expectedA);

            var filteredIdMapB = idMap.withFilteredLabels(List.of(NodeLabel.of("B")), 1).get();
            long expectedB = idMap.toMappedNodeId(2000);
            assertThat(filteredIdMapB.toRootNodeId(0)).isEqualTo(expectedB);


            var filteredIdMapC = idMap.withFilteredLabels(List.of(NodeLabel.of("C")), 1).get();
            long expectedC = idMap.toMappedNodeId(3000);
            assertThat(filteredIdMapC.toRootNodeId(0)).isEqualTo(expectedC);
        }
    }

}
