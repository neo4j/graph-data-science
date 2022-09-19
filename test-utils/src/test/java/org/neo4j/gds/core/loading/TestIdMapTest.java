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

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.api.IdMap.NOT_FOUND;

class TestIdMapTest {

    @Test
    void empty() {
        var idMap = TestIdMap.builder().build();

        assertThat(idMap.nodeCount()).isEqualTo(0);
        assertThat(idMap.toMappedNodeId(42)).isEqualTo(NOT_FOUND);
        assertThat(idMap.toOriginalNodeId(42)).isEqualTo(NOT_FOUND);
        assertThat(idMap.highestNeoId()).isEqualTo(Long.MIN_VALUE);
    }

    @Test
    void unlabeled() {
        var idMap = TestIdMap.builder()
            .add(42, 0)
            .add(43, 1)
            .add(52, 2)
            .add(53, 3)
            .build();

        assertThat(idMap.nodeCount()).isEqualTo(4);

        assertThat(idMap.toOriginalNodeId(0)).isEqualTo(42);
        assertThat(idMap.toOriginalNodeId(1)).isEqualTo(43);
        assertThat(idMap.toOriginalNodeId(2)).isEqualTo(52);
        assertThat(idMap.toOriginalNodeId(3)).isEqualTo(53);
        assertThat(idMap.toOriginalNodeId(4)).isEqualTo(NOT_FOUND);

        assertThat(idMap.toMappedNodeId(42)).isEqualTo(0);
        assertThat(idMap.toMappedNodeId(43)).isEqualTo(1);
        assertThat(idMap.toMappedNodeId(52)).isEqualTo(2);
        assertThat(idMap.toMappedNodeId(53)).isEqualTo(3);
        assertThat(idMap.toMappedNodeId(54)).isEqualTo(NOT_FOUND);
    }

    @Test
    void labeled() {
        var labelA = NodeLabel.of("A");
        var labelB = NodeLabel.of("B");

        var idMap = TestIdMap.builder()
            .add(42, 0, labelA)
            .add(43, 1, labelB)
            .add(52, 2, labelA, labelB)
            .add(53, 3)
            .build();

        assertThat(idMap.nodeLabels(0)).containsExactly(labelA);
        assertThat(idMap.nodeLabels(1)).containsExactly(labelB);
        assertThat(idMap.nodeLabels(2)).containsExactly(labelA, labelB);
        assertThat(idMap.nodeLabels(3)).isEmpty();
    }

    @Test
    void highestOriginalId() {
        var idMap = TestIdMap.builder()
            .add(42, 0)
            .add(43, 1)
            .add(52, 2)
            .add(53, 3)
            .build();

        assertThat(idMap.highestNeoId()).isEqualTo(53);
    }

    @Test
    void rootIdMap() {
        var idMap = TestIdMap.builder().add(42, 0).build();
        assertThat(idMap.rootIdMap()).isEqualTo(idMap);
        assertThat(idMap.rootNodeCount()).isEqualTo(OptionalLong.of(1));
        assertThat(idMap.toRootNodeId(0)).isEqualTo(0);
        assertThat(idMap.toRootNodeId(1)).isEqualTo(NOT_FOUND);
    }

    @Test
    void addExisting() {
        var builder = TestIdMap.builder();

        builder.add(42, 1337);

        assertThatThrownBy(() -> builder.add(42, 1338))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("original id `42` is already mapped");

        assertThatThrownBy(() -> builder.add(43, 1337))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("internal id `1337` is already mapped");
    }

    @Test
    void addAll() {
        var builder = TestIdMap.builder();
        
        var idMap = builder.addAll(42, 0, 43, 1, 44, 2, 1337, 3).build();

        assertThat(idMap.nodeCount()).isEqualTo(4);
        assertThat(idMap.toMappedNodeId(42)).isEqualTo(0);
        assertThat(idMap.toMappedNodeId(43)).isEqualTo(1);
        assertThat(idMap.toMappedNodeId(44)).isEqualTo(2);
        assertThat(idMap.toMappedNodeId(1337)).isEqualTo(3);
    }
}
