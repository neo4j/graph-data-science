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
package org.neo4j.gds.similarity.knn.metrics;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.nodeproperties.DoubleArrayTestProperties;
import org.neo4j.gds.nodeproperties.FloatArrayTestProperties;
import org.neo4j.gds.nodeproperties.LongArrayTestProperties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@GdlExtension
class NullCheckingNodePropertiesTest {

    @GdlGraph
    private static final String TEST_GRAPH = "CREATE (), ()";

    @Inject
    Graph graph;

    @Test
    void shouldWrapDoubleArrayPropertyAccess() {
        var props = new DoubleArrayTestProperties(nodeId -> new double[]{nodeId});
        var wrappedProps = new NullCheckingNodeProperties(props, "propertyName", graph);
        graph.forEachNode(nodeId -> {
                assertThat(wrappedProps.doubleArrayValue(nodeId))
                    .isEqualTo(props.doubleArrayValue(nodeId));
                return true;
            }
        );
    }

    @Test
    void shouldWrapFloatArrayPropertyAccess() {
        var props = new FloatArrayTestProperties(nodeId -> new float[]{nodeId});
        var wrappedProps = new NullCheckingNodeProperties(props, "propertyName", graph);
        graph.forEachNode(nodeId -> {
                assertThat(wrappedProps.floatArrayValue(nodeId))
                    .isEqualTo(props.floatArrayValue(nodeId));
                return true;
            }
        );
    }

    @Test
    void shouldWrapLongArrayPropertyAccess() {
        var props = new LongArrayTestProperties(nodeId -> new long[]{nodeId});
        var wrappedProps = new NullCheckingNodeProperties(props, "propertyName", graph);
        graph.forEachNode(nodeId -> {
                assertThat(wrappedProps.longArrayValue(nodeId))
                    .isEqualTo(props.longArrayValue(nodeId));
                return true;
            }
        );
    }

    @Test
    void shouldThrowUsefullyForNullDoubleArrayValues() {
        var nullProps = new DoubleArrayTestProperties(nodeId -> null);
        var nonNullProps = new NullCheckingNodeProperties(nullProps, "propertyName", graph);
        assertThatThrownBy(() -> nonNullProps.doubleArrayValue(1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Missing node property `propertyName` for node with id `1`.");
    }

    @Test
    void shouldThrowUsefullyForNullFloatArrayValues() {
        var nullProps = new FloatArrayTestProperties(nodeId -> null);
        var nonNullProps = new NullCheckingNodeProperties(nullProps, "propertyName", graph);
        assertThatThrownBy(() -> nonNullProps.floatArrayValue(1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Missing node property `propertyName` for node with id `1`.");
    }

    @Test
    void shouldThrowUsefullyForNullLongArrayValues() {
        var nullProps = new LongArrayTestProperties(nodeId -> null);
        var nonNullProps = new NullCheckingNodeProperties(nullProps, "propertyName", graph);
        assertThatThrownBy(() -> nonNullProps.longArrayValue(1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Missing node property `propertyName` for node with id `1`.");
    }
}
