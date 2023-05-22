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
import org.neo4j.gds.nodeproperties.DoubleArrayTestPropertyValues;
import org.neo4j.gds.nodeproperties.FloatArrayTestPropertyValues;
import org.neo4j.gds.nodeproperties.LongArrayTestPropertyValues;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@GdlExtension
class NullCheckingNodePropertyValuesTest {

    @GdlGraph
    private static final String TEST_GRAPH = "CREATE (), ()";

    @Inject
    Graph graph;

    @Test
    void shouldWrapDoubleArrayPropertyAccess() {
        var props = new DoubleArrayTestPropertyValues(nodeId -> new double[]{nodeId});
        var wrappedProps = NullCheckingNodePropertyValues.create(props, "propertyName", graph);
        graph.forEachNode(nodeId -> {
                assertThat(wrappedProps.doubleArrayValue(nodeId))
                    .isEqualTo(props.doubleArrayValue(nodeId));
                return true;
            }
        );
    }

    @Test
    void shouldWrapFloatArrayPropertyAccess() {
        var props = new FloatArrayTestPropertyValues(nodeId -> new float[]{nodeId});
        var wrappedProps = NullCheckingNodePropertyValues.create(props, "propertyName", graph);
        graph.forEachNode(nodeId -> {
                assertThat(wrappedProps.floatArrayValue(nodeId))
                    .isEqualTo(props.floatArrayValue(nodeId));
                return true;
            }
        );
    }

    @Test
    void shouldWrapLongArrayPropertyAccess() {
        var props = new LongArrayTestPropertyValues(nodeId -> new long[]{nodeId});
        var wrappedProps = NullCheckingNodePropertyValues.create(props, "propertyName", graph);
        graph.forEachNode(nodeId -> {
                assertThat(wrappedProps.longArrayValue(nodeId))
                    .isEqualTo(props.longArrayValue(nodeId));
                return true;
            }
        );
    }

    @Test
    void shouldThrowUsefullyForNullDoubleArrayValues() {
        var nullProps = new DoubleArrayTestPropertyValues(nodeId -> null);
        var nonNullProps = NullCheckingNodePropertyValues.create(nullProps, "propertyName", graph);
        assertThatThrownBy(() -> nonNullProps.doubleArrayValue(1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(String.format(Locale.US, "Missing `List of Float` node property `propertyName` for node with id `%d`.", graph.toOriginalNodeId(1)));
    }

    @Test
    void shouldThrowUsefullyForNullFloatArrayValues() {
        var nullProps = new FloatArrayTestPropertyValues(nodeId -> null);
        var nonNullProps = NullCheckingNodePropertyValues.create(nullProps, "propertyName", graph);
        assertThatThrownBy(() -> nonNullProps.floatArrayValue(1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(String.format(
                Locale.US,
                "Missing `List of Float` node property `propertyName` for node with id `%d`.",
                graph.toOriginalNodeId(1)
            ));
    }

    @Test
    void shouldThrowUsefullyForNullLongArrayValues() {
        var nullProps = new LongArrayTestPropertyValues(nodeId -> null);
        var nonNullProps = NullCheckingNodePropertyValues.create(nullProps, "propertyName", graph);
        assertThatThrownBy(() -> nonNullProps.longArrayValue(1))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(String.format(
                Locale.US,
                "Missing `List of Integer` node property `propertyName` for node with id `%d`.",
                graph.toOriginalNodeId(1)
            ));
    }

    @Test
    void shouldThrowNormallyIfTheWrongPropertyIsAccessed() {
        var longProps = new LongArrayTestPropertyValues(i -> new long[]{i});
        var wrappedProps = NullCheckingNodePropertyValues.create(longProps, "propertyName", graph);
        assertThatThrownBy(() -> wrappedProps.doubleArrayValue(0))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Tried to retrieve a value of type DOUBLE_ARRAY value from properties of type LONG_ARRAY");
    }
}
