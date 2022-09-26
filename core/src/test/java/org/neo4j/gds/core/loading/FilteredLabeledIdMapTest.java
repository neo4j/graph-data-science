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
import org.neo4j.gds.NodeLabel;

import java.util.ArrayList;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.api.IdMap.NOT_FOUND;

class FilteredLabeledIdMapTest {

    @Test
    void nodeCount() {
        var idMap = new FilteredLabeledIdMap(
            idMap(42, 0, 43, 1, 1337, 2),
            idMap(0, 0, 2, 1)
        );
        assertThat(idMap.nodeCount()).isEqualTo(2);
    }

    @Test
    void rootNodeCount() {
        var idMap = new FilteredLabeledIdMap(
            idMap(42, 0, 43, 1, 1337, 2),
            idMap(0, 0, 2, 1)
        );
        assertThat(idMap.rootNodeCount()).isEqualTo(OptionalLong.of(3));
    }

    @Test
    void toOriginalNodeId() {
        var idMap = new FilteredLabeledIdMap(
            idMap(42, 0, 43, 1, 1337, 2),
            idMap(0, 0, 2, 1)
        );
        assertThat(idMap.toOriginalNodeId(0)).isEqualTo(42);
        assertThat(idMap.toOriginalNodeId(1)).isEqualTo(1337);
        assertThat(idMap.toOriginalNodeId(2)).isEqualTo(NOT_FOUND);
    }

    @Test
    void toMappedNodeId() {
        var idMap = new FilteredLabeledIdMap(
            idMap(42, 0, 43, 1, 1337, 2),
            idMap(0, 0, 2, 1)
        );
        assertThat(idMap.toMappedNodeId(42)).isEqualTo(0);
        assertThat(idMap.toMappedNodeId(1337)).isEqualTo(1);
        assertThat(idMap.toMappedNodeId(1338)).isEqualTo(NOT_FOUND);
    }

    @Test
    void toFilteredNodeId() {
        var idMap = new FilteredLabeledIdMap(
            idMap(42, 0, 43, 1, 1337, 2),
            idMap(0, 0, 2, 1)
        );
        assertThat(idMap.toFilteredNodeId(0)).isEqualTo(0);
        assertThat(idMap.toFilteredNodeId(1)).isEqualTo(NOT_FOUND);
        assertThat(idMap.toFilteredNodeId(2)).isEqualTo(1);
    }

    @Test
    void toRootNodeId() {
        var idMap = new FilteredLabeledIdMap(
            idMap(42, 0, 43, 1, 1337, 2),
            idMap(0, 0, 2, 1)
        );
        assertThat(idMap.toRootNodeId(0)).isEqualTo(0);
        assertThat(idMap.toRootNodeId(1)).isEqualTo(2);
        assertThat(idMap.toRootNodeId(3)).isEqualTo(NOT_FOUND);
    }

    @Test
    void toRootIdMap() {
        var originalToRootIdMap = idMap(42, 0, 43, 1, 1337, 2);
        var rootToFilteredIdMap = idMap(0, 0, 2, 1);
        var idMap = new FilteredLabeledIdMap(originalToRootIdMap, rootToFilteredIdMap);

        assertThat(idMap.rootIdMap()).isEqualTo(originalToRootIdMap);
    }

    @Test
    void contains() {
        var idMap = new FilteredLabeledIdMap(
            idMap(42, 0, 43, 1, 1337, 2),
            idMap(0, 0, 2, 1)
        );
        assertThat(idMap.contains(42)).isTrue();
        assertThat(idMap.contains(43)).isFalse();
        assertThat(idMap.contains(1337)).isTrue();
        assertThat(idMap.contains(1338)).isFalse();
    }

    @Test
    void containsRootNodeId() {
        var idMap = new FilteredLabeledIdMap(
            idMap(42, 0, 43, 1, 1337, 2),
            idMap(0, 0, 2, 1)
        );
        assertThat(idMap.containsRootNodeId(0)).isTrue();
        assertThat(idMap.containsRootNodeId(1)).isFalse();
        assertThat(idMap.containsRootNodeId(2)).isTrue();
        assertThat(idMap.containsRootNodeId(3)).isFalse();
    }

    @Test
    void highestOriginalId() {
        var idMap = new FilteredLabeledIdMap(
            idMap(42, 0, 43, 1, 1337, 2),
            idMap(0, 0, 2, 1)
        );
        assertThat(idMap.highestOriginalId()).isEqualTo(1337);
    }

    @Test
    void hasLabel() {
        var aLabel = NodeLabel.of("A");
        var bLabel = NodeLabel.of("B");
        var cLabel = NodeLabel.of("C");

        var originalToRootIdMap = TestIdMap.builder()
            .add(42, 0, aLabel)
            .add(43, 1, bLabel)
            .add(1337, 2, cLabel)
            .build();
        var rootToFilteredIdMap = idMap(0, 0, 2, 1);

        var idMap = new FilteredLabeledIdMap(originalToRootIdMap, rootToFilteredIdMap);

        assertThat(idMap.hasLabel(0, aLabel)).isTrue();
        assertThat(idMap.hasLabel(0, bLabel)).isFalse();
        assertThat(idMap.hasLabel(1, bLabel)).isFalse();
        assertThat(idMap.hasLabel(1, cLabel)).isTrue();
    }

    @Test
    void forEachNodeLabel() {
        var aLabel = NodeLabel.of("A");
        var bLabel = NodeLabel.of("B");
        var cLabel = NodeLabel.of("C");

        var originalToRootIdMap = TestIdMap.builder()
            .add(42, 0, aLabel)
            .add(43, 1, bLabel)
            .add(1337, 2, aLabel, bLabel, cLabel)
            .build();
        var rootToFilteredIdMap = idMap(0, 0, 2, 1);

        var idMap = new FilteredLabeledIdMap(originalToRootIdMap, rootToFilteredIdMap);

        var labels_0 = new ArrayList<>();
        idMap.forEachNodeLabel(0, labels_0::add);
        var labels_1 = new ArrayList<>();
        idMap.forEachNodeLabel(1, labels_1::add);

        assertThat(labels_0).containsExactly(aLabel);
        assertThat(labels_1).containsExactly(aLabel, bLabel, cLabel);
    }

    @Test
    void nodeLabels() {
        var aLabel = NodeLabel.of("A");
        var bLabel = NodeLabel.of("B");
        var cLabel = NodeLabel.of("C");

        var originalToRootIdMap = TestIdMap.builder()
            .add(42, 0, aLabel)
            .add(43, 1, bLabel)
            .add(1337, 2, aLabel, bLabel, cLabel)
            .build();
        var rootToFilteredIdMap = idMap(0, 0, 2, 1);

        var idMap = new FilteredLabeledIdMap(originalToRootIdMap, rootToFilteredIdMap);

        assertThat(idMap.nodeLabels(0)).containsExactly(aLabel);
        assertThat(idMap.nodeLabels(1)).containsExactly(aLabel, bLabel, cLabel);
    }

    private static TestIdMap idMap(long... mappings) {
        return TestIdMap.builder().addAll(mappings).build();
    }
}
