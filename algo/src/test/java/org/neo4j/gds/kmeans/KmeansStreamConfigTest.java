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
package org.neo4j.gds.kmeans;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodeProperty;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KmeansStreamConfigTest {

    @Test
    void shouldThrowOnMissingNodeProperty() {
        var userInput = CypherMapWrapper.create(Map.of());
        assertThatIllegalArgumentException()
            .isThrownBy(() -> KmeansStreamConfig.of(userInput))
            .withMessageContaining("No value specified for the mandatory configuration parameter `nodeProperty`");
    }

    @Test
    void shouldNotThrowOnGoodInput() {
        var userInput = CypherMapWrapper.create(Map.of("nodeProperty", "foo"));
        assertThat(KmeansStreamConfig.of(userInput))
            .isNotNull()
            .satisfies(config -> assertThat(config.nodeProperty()).isEqualTo("foo"));
    }

    @ParameterizedTest
    @MethodSource("invalidNodePropertyValueTypes")
    void shouldFailOnInvalidPropertyValueTypes(ValueType valueType) {

        GraphStore graphStoreMock = mock(GraphStore.class);
        NodeProperty nodePropertyMock = mock(NodeProperty.class);
        when(nodePropertyMock.valueType()).thenReturn(valueType);
        when(graphStoreMock.nodeProperty("foo")).thenReturn(nodePropertyMock);

        var userInput = CypherMapWrapper.create(Map.of("nodeProperty", "foo"));
        var streamConfig = KmeansStreamConfig.of(userInput);
        assertThatIllegalArgumentException()
            .isThrownBy(() -> streamConfig.graphStoreValidation(graphStoreMock, List.of(), List.of()))
            .withMessageContaining("Unsupported node property value type")
            .withMessageContaining("Value type required: [DOUBLE_ARRAY] or [FLOAT_ARRAY].");
    }

    @ParameterizedTest
    @MethodSource("validNodePropertyValueTypes")
    void shouldAcceptValidPropertyValueTypes(ValueType valueType) {

        GraphStore graphStoreMock = mock(GraphStore.class);
        NodeProperty nodePropertyMock = mock(NodeProperty.class);
        when(nodePropertyMock.valueType()).thenReturn(valueType);
        when(graphStoreMock.nodeProperty("foo")).thenReturn(nodePropertyMock);

        var userInput = CypherMapWrapper.create(Map.of("nodeProperty", "foo"));
        var streamConfig = KmeansStreamConfig.of(userInput);
        assertThatNoException()
            .isThrownBy(() -> streamConfig.graphStoreValidation(graphStoreMock, List.of(), List.of()));
    }

    static Stream<Arguments> validNodePropertyValueTypes() {
        return Stream.of(
            Arguments.of(ValueType.DOUBLE_ARRAY),
            Arguments.of(ValueType.FLOAT_ARRAY)
        );
    }

    static Stream<Arguments> invalidNodePropertyValueTypes() {
        return Arrays.stream(ValueType.values())
            .filter(t -> t != ValueType.DOUBLE_ARRAY && t != ValueType.FLOAT_ARRAY)
            .map(Arguments::of);
    }

    @Test
    void shouldFailOnInvalidSampler() {
        var userInput = CypherMapWrapper.create(Map.of("nodeProperty", "foo", "initialSampler", "foo"));
        assertThatThrownBy(() -> KmeansStreamConfig.of(userInput)).hasMessageContaining("is not supported");
    }

    @Test
    void shouldBeCaseIgnorant() {
        var userInput = CypherMapWrapper.create(Map.of("nodeProperty", "foo", "initialSampler", "kmEanS++"));
        assertThatNoException().isThrownBy(() -> KmeansStreamConfig.of(userInput));
    }
    
}
