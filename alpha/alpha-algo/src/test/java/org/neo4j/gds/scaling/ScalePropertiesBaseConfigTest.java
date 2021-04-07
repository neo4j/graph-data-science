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
package org.neo4j.gds.scaling;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.utils.StringFormatting.toLowerCaseWithLocale;
import static org.neo4j.graphalgo.utils.StringFormatting.toUpperCaseWithLocale;

class ScalePropertiesBaseConfigTest {

    @ParameterizedTest
    @EnumSource(Scaler.Variant.class)
    void parseValidScalers(Scaler.Variant scaler) {
        ScalePropertiesMutateConfigImpl config = new ScalePropertiesMutateConfigImpl(
            Optional.of("graph"),
            Optional.empty(),
            "",
            CypherMapWrapper.create(
                Map.of(
                    "mutateProperty", "test",
                    "scalers", List.of(toLowerCaseWithLocale(scaler.name()), toUpperCaseWithLocale(scaler.name())),
                    "nodeProperties", List.of("a", "b")
                )
            )
        );

        assertThat(config.scalers().size()).isEqualTo(2);
        assertThat(config.scalers().get(0)).isEqualTo(scaler);
        assertThat(config.scalers().get(1)).isEqualTo(scaler);
    }

    @Test
    void unequalNodePropertiesScalerSizes() {
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new ScalePropertiesMutateConfigImpl(
                Optional.of("graph"),
                Optional.empty(),
                "",
                CypherMapWrapper.create(
                    Map.of(
                        "mutateProperty", "test",
                        "scalers", List.of("Mean", "Minmax"),
                        "nodeProperties", List.of("a", "b", "c")
                    )
                )
            )
        );

        assertThat(ex.getMessage()).contains("Specify a scaler for each node property. Found 2 scalers for 3 node properties");
    }

    @Test
    void supportNodePropertyMap() {
        var config = new ScalePropertiesMutateConfigImpl(
            Optional.of("graph"),
            Optional.empty(),
            "",
            CypherMapWrapper.create(
                Map.of(
                    "mutateProperty", "test",
                    "scalers", List.of("Mean"),
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
                Optional.of("graph"),
                Optional.empty(),
                "",
                CypherMapWrapper.create(
                    Map.of(
                        "mutateProperty", "test",
                        "scalers", List.of("nonExistent"),
                        "nodeProperties", "test"
                    )
                )
            )
        );

        assertThat(ex.getMessage()).contains("Scaler `nonExistent` is not supported.");
    }

    private static Stream<Arguments> scalers() {
        return Stream.of(Arguments.of("mean"), Arguments.of(List.of("mean")));
    }

    @ParameterizedTest
    @MethodSource("scalers")
    void syntacticSugarForScalers() {
        var config = new ScalePropertiesMutateConfigImpl(
            Optional.of("graph"),
            Optional.empty(),
            "",
            CypherMapWrapper.create(
                Map.of(
                    "mutateProperty", "test",
                    "scalers", "mean",
                    "nodeProperties", List.of("a", "b")
                )
            )
        );

        assertThat(config.scalers()).isEqualTo(List.of(Scaler.Variant.MEAN));
    }

    @Test
    void failOnSpecifyingSamePropertyMultipleTimes() {
        assertThatExceptionOfType(IllegalStateException.class).isThrownBy(() -> new ScalePropertiesMutateConfigImpl(
            Optional.of("graph"),
            Optional.empty(),
            "",
            CypherMapWrapper.create(
                Map.of(
                    "mutateProperty", "test",
                    "scalers", "mean",
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
                Optional.of("graph"),
                Optional.empty(),
                "",
                CypherMapWrapper.create(
                    Map.of(
                        "mutateProperty", "test",
                        "scalers", "mean",
                        "nodeProperties", List.of(1)
                    )
                )
            )
        );

        assertThat(ex.getMessage()).contains("Expected String or Map for property mappings. Got Integer.");
    }
}
