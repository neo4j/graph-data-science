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
package org.neo4j.gds.beta.pregel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.beta.pregel.NodeValue;
import org.neo4j.graphalgo.beta.pregel.PregelSchema;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class NodeValueTest {

    @Test
    void validSingleNodeValue() {
        var key1 = "KEY1";
        var schema = new PregelSchema.Builder()
            .add(key1, ValueType.DOUBLE)
            .build();
        var nodeValues = NodeValue.of(schema, 10, 4, AllocationTracker.empty());
        assertThat(nodeValues).isInstanceOf(NodeValue.SingleNodeValue.class);
        assertEquals(nodeValues.doubleProperties(key1).size(), 10);
    }

    @Test
    void validCompositeNodeValue() {
        var key1 = "KEY1";
        var key2 = "KEY2";
        var schema = new PregelSchema.Builder()
            .add(key1, ValueType.DOUBLE)
            .add(key2, ValueType.LONG)
            .build();
        var nodeValues = NodeValue.of(schema, 10, 4, AllocationTracker.empty());
        assertThat(nodeValues).isInstanceOf(NodeValue.CompositeNodeValue.class);
        assertEquals(nodeValues.doubleProperties(key1).size(), 10);
        assertEquals(nodeValues.longProperties(key2).size(), 10);
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.beta.pregel.NodeValueTest#validPropertyTypeAndGetters")
    void testThrowWhenAccessingUnknownProperty(ValueType valueType, BiConsumer<NodeValue, String> valueConsumer) {
        var schema = new PregelSchema.Builder().add("KEY", valueType).build();
        var nodeValues = NodeValue.of(schema, 10, 4, AllocationTracker.empty());
        assertThat(nodeValues).isInstanceOf(NodeValue.SingleNodeValue.class);

        assertThatThrownBy(() -> valueConsumer.accept(nodeValues, "DOES_NOT_EXIST"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Property with key DOES_NOT_EXIST does not exist. Available properties are: [KEY]");
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.graphalgo.beta.pregel.NodeValueTest#invalidPropertyTypeAndGetters")
    void testThrowWhenAccessingPropertyOfWrongType(ValueType valueType, BiConsumer<NodeValue, String> valueConsumer) {
        var schema = new PregelSchema.Builder().add("KEY", valueType).build();
        var nodeValues = NodeValue.of(schema, 10, 4, AllocationTracker.empty());
        assertThat(nodeValues).isInstanceOf(NodeValue.SingleNodeValue.class);

        assertThatThrownBy(() -> valueConsumer.accept(nodeValues, "KEY"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("is not compatible with available property type");
    }

    static Stream<Arguments> validPropertyTypeAndGetters() {
        BiConsumer<NodeValue, String> longGetter = NodeValue::longProperties;
        BiConsumer<NodeValue, String> doubleGetter = NodeValue::doubleProperties;
        BiConsumer<NodeValue, String> longArrayGetter = NodeValue::longArrayProperties;
        BiConsumer<NodeValue, String> doubleArrayGetter = NodeValue::doubleArrayProperties;
        return Stream.of(
            arguments(ValueType.LONG, longGetter),
            arguments(ValueType.DOUBLE, doubleGetter),
            arguments(ValueType.LONG_ARRAY, longArrayGetter),
            arguments(ValueType.DOUBLE_ARRAY, doubleArrayGetter)
        );
    }

    static Stream<Arguments> invalidPropertyTypeAndGetters() {
        BiConsumer<NodeValue, String> longGetter = NodeValue::longProperties;
        BiConsumer<NodeValue, String> doubleGetter = NodeValue::doubleProperties;

        return Stream.of(
            arguments(ValueType.LONG, doubleGetter),
            arguments(ValueType.DOUBLE, longGetter),
            arguments(ValueType.LONG_ARRAY, longGetter),
            arguments(ValueType.DOUBLE_ARRAY, doubleGetter)
        );
    }
}
