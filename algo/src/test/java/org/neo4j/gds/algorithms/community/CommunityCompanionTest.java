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
package org.neo4j.gds.algorithms.community;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.properties.nodes.NodeProperty;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CommunityCompanionTest {

    @Test
    void shouldAcceptLongPropertyTypes() {

        var nodePropertyValues = NodePropertyValuesAdapter.adapt(HugeLongArray.of(1, 2, 3, 4));

        var graph = mock(Graph.class);
        when(graph.nodeProperties("foo")).thenReturn(nodePropertyValues);

        var extractedNodeProperties = CommunityCompanion.extractSeedingNodePropertyValues(graph, "foo");

        for (long i = 0; i < 4; ++i) {
            assertThat(extractedNodeProperties.longValue(i)).isEqualTo(i + 1);
        }

    }

    @Test
    void shouldThrowForNonLongValues() {
        var nodePropertyValues = NodePropertyValuesAdapter.adapt(HugeDoubleArray.of(1.5, 2.5, 3.5, 4.5));

        var graph = mock(Graph.class);
        when(graph.nodeProperties("foo")).thenReturn(nodePropertyValues);

        assertThatThrownBy(() -> CommunityCompanion.extractSeedingNodePropertyValues(graph, "foo"))
            .hasMessageContaining(
                "Provided seeding property `foo` does not comprise exclusively of long values");
    }


    @Test
    void shouldReturnConsecutiveIds() {
        var array = HugeLongArray.newArray(10);
        array.setAll(id -> id % 3 + 10);
        var nodePropertyValues = NodePropertyValuesAdapter.adapt(array);

        var result = CommunityCompanion.nodePropertyValues(
            true,
            nodePropertyValues
        );

        assertThat(result).isInstanceOf(ConsecutiveLongNodePropertyValues.class);
        for (int i = 0; i < 10; i++) {
            assertThat(result.longValue(i)).isEqualTo(i % 3);
        }
    }

    @Test
    void shouldReturnOnlyChangedProperties() {
        var array = HugeLongArray.newArray(10);
        array.setAll(id -> id);
        var inputProperties = NodePropertyValuesAdapter.adapt(array);

        var seedProperty = NodeProperty.of("seed", PropertyState.PERSISTENT, inputProperties);

        var result = CommunityCompanion.nodePropertyValues(
            true,
            "seed",
            "seed",
            false,
            inputProperties,
            () -> seedProperty
        );

        assertThat(result).isInstanceOf(LongIfChangedNodePropertyValues.class);
        // properties that have not changed signalled by Long.MIN_VALUE
        for (long i = 0; i < result.nodeCount(); i++) {
            assertThat(result.longValue(i)).isEqualTo(Long.MIN_VALUE);
        }
    }

    @Test
    void shouldRestrictCommunitySize() {
        var array = HugeLongArray.newArray(10);
        array.setAll(id -> id < 5 ? id : 5);
        var inputProperties = NodePropertyValuesAdapter.adapt(array);

        var result = CommunityCompanion.nodePropertyValues(
            false,
            inputProperties,
            Optional.of(2L),
            new Concurrency(4)
        );

        for (long i = 0L; i < result.nodeCount(); i++) {

            if (i < 5) {
                // properties that have not changed signalled by Long.MIN_VALUE
                assertThat(result.longValue(i)).isEqualTo(Long.MIN_VALUE);
            } else {
                assertThat(result.longValue(i)).isEqualTo(inputProperties.longValue(i));
            }
        }
    }


    @Test
    void shouldWorkWithMinComponentAndConsecutive() {
        var array = HugeLongArray.of(20, 20, 200, 10, 10, 50, 90, 10, 50, 50, 50);
        var inputProperties = NodePropertyValuesAdapter.adapt(array);

        Long[] returnedValues = new Long[]{null, null, null, 0l, 0L, 1l, null, 0l, 1l, 1l, 1l};

        var result = CommunityCompanion.nodePropertyValues(
            true,
            inputProperties,
            Optional.of(3L),
            new Concurrency(4)
        );

        for (long i = 0L; i < result.nodeCount(); i++) {
            int ii = (int) i;
            if (returnedValues[ii] == null) {
                assertThat(result.hasValue(i)).isFalse();
            } else {
                assertThat(result.hasValue(i)).isTrue();
            }

        }
    }
}
