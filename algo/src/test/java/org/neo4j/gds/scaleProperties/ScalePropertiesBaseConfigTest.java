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
package org.neo4j.gds.scaleProperties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.gdl.GdlFactory;
import org.neo4j.gds.scaling.ScalerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ScalePropertiesBaseConfigTest {

    static Stream<Arguments> scalers() {
        return ScalerFactory.SUPPORTED_SCALERS.keySet().stream().map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("scalers")
    void parseValidScalers(String scaler) {
        ScalePropertiesMutateConfigImpl config = new ScalePropertiesMutateConfigImpl(
            CypherMapWrapper.create(
                Map.of(
                    "mutateProperty", "test",
                    "scaler", scaler,
                    "nodeProperties", List.of("a", "b")
                )
            )
        );

        assertThat(config.scaler().type()).isEqualTo(scaler);
    }

    @Test
    void supportNodePropertyMap() {
        var config = new ScalePropertiesMutateConfigImpl(
            CypherMapWrapper.create(
                Map.of(
                    "mutateProperty", "test",
                    "scaler", "Mean",
                    "nodeProperties", Map.of("a", Map.of("neoProperty", "noeA", "defaultValue", 0))
                )
            )
        );

        assertEquals(config.nodeProperties(), List.of("a"));
    }

    @Test
    void failOnNonExistentScalar() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new ScalePropertiesMutateConfigImpl(
                CypherMapWrapper.create(
                    Map.of(
                        "mutateProperty", "test",
                        "scaler", "nonExistent",
                        "nodeProperties", "test"
                    )
                )
            )
        );

        assertThat(ex.getMessage()).contains("Unrecognised scaler type specified: `nonExistent`");
    }

    @Test
    void failOnSpecifyingSamePropertyMultipleTimes() {
        assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> new ScalePropertiesMutateConfigImpl(
            CypherMapWrapper.create(
                Map.of(
                    "mutateProperty", "test",
                    "scaler", "mean",
                    "nodeProperties", List.of("a", "b", "b", "a", "a")
                )
            )
        )).withMessageContaining("Duplicate property key `b`");
    }

    @Test
    void failOnNonStringNodeProperties() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new ScalePropertiesMutateConfigImpl(
                CypherMapWrapper.create(
                    Map.of(
                        "mutateProperty", "test",
                        "scaler", "mean",
                        "nodeProperties", List.of(1)
                    )
                )
            )
        );

        assertThat(ex.getMessage()).contains("Expected String or Map for property mappings. Got Integer.");
    }

    @Test
    void failsOnMissingPropertyDimensions() {
        var config = ScalePropertiesStreamConfigImpl.builder().scaler("mean").nodeProperties(List.of("a")).build();

        var graphStore = GdlFactory.of("(), ({a: [1.0, 2.2]})").build();

        assertThatThrownBy(() -> config.graphStoreValidation(
            graphStore,
            config.nodeLabelIdentifiers(graphStore),
            config.internalRelationshipTypes(graphStore)
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Node property `a` contains a `null` value");
    }

    @Test
    void failsOnMissingProperty() {
        var config = ScalePropertiesStreamConfigImpl.builder().scaler("mean").nodeProperties(List.of("b")).build();

        var graphStore = GdlFactory.of("(), ({a: [1.0, 2.2]})").build();

        assertThatThrownBy(() -> config.graphStoreValidation(
            graphStore,
            config.nodeLabelIdentifiers(graphStore),
            config.internalRelationshipTypes(graphStore)
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(
                "The node properties `['b']` are not present for all requested labels. Requested labels: `['__ALL__']`. Properties available on all requested labels: `['a']`");
    }

    @Test
    void failsOnEmptyNodeProperties() {
        var config = ScalePropertiesStreamConfigImpl.builder().scaler("mean").nodeProperties(List.of()).build();

        var graphStore = GdlFactory.of("(), ({a: [1.0, 2.2]})").build();

        assertThatThrownBy(() -> config.graphStoreValidation(
            graphStore,
            config.nodeLabelIdentifiers(graphStore),
            config.internalRelationshipTypes(graphStore)
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("`nodeProperties` must not be empty");
    }

}
