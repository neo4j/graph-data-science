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
package org.neo4j.gds.embeddings.hashgnn;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class HashGNNConfigTest {
    @Test
    void failsWithTooHighDensityLevel() {
        assertThatThrownBy(() -> {
            HashGNNConfigImpl
                .builder()
                .featureProperties(List.of("x"))
                .embeddingDensity(4)
                .binarizeFeatures(Map.of("dimension", 4, "densityLevel", 3))
                .iterations(100)
                .build();
        }).isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(formatWithLocale("The value %d of `densityLevel` may not exceed half of the value %d of `dimension`.", 3, 4));
    }

    @Test
    void binarizationConfigCorrectType() {
        var config = HashGNNConfigImpl
            .builder()
            .featureProperties(List.of("x"))
            .binarizeFeatures(Map.of("dimension", 100, "densityLevel", 3))
            .embeddingDensity(4)
            .iterations(100)
            .build();
        var map = config.toMap();
        assertThat(map.get("binarizeFeatures")).isInstanceOf(Map.class);
    }
}
