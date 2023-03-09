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
package org.neo4j.gds.impl.approxmaxkcut.config;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class ApproxMaxKCutStreamConfigTest {

    @Test
    void testK() {
        CypherMapWrapper config = CypherMapWrapper.create(Map.of("k", 18));

        var streamConfig = ApproxMaxKCutStreamConfig.of(config);
        byte expectedK = 18;
        assertThat(streamConfig.k()).isEqualTo(expectedK);
    }

    @Test
    void testIterations() {
        CypherMapWrapper config = CypherMapWrapper.create(Map.of("iterations", 87));

        var streamConfig = ApproxMaxKCutStreamConfig.of(config);
        assertThat(streamConfig.iterations()).isEqualTo(87);
    }

    @Test
    void testVnsMaxNeighborhoodOrder() {
        CypherMapWrapper config = CypherMapWrapper.create(Map.of("vnsMaxNeighborhoodOrder", 31));

        var streamConfig = ApproxMaxKCutStreamConfig.of(config);
        assertThat(streamConfig.vnsMaxNeighborhoodOrder()).isEqualTo(31);
    }

    @Test
    void testRandomSeed() {
        CypherMapWrapper config = CypherMapWrapper.create(
            Map.of(
                "randomSeed", 42L,
                "concurrency", 1
            )
        );

        var streamConfig = ApproxMaxKCutStreamConfig.of(config);
        assertThat(streamConfig.randomSeed())
            .isPresent()
            .hasValue(42L);
    }

    @Test
    void shouldFailWhenTryingToSetRandomSeedWithConcurrencyGreaterThatOne() {
        CypherMapWrapper config = CypherMapWrapper.create(
            Map.of(
                "randomSeed", 42L
            )
        );

        assertThatIllegalArgumentException()
            .isThrownBy(() -> ApproxMaxKCutStreamConfig.of(config))
            .withMessage("Configuration parameter 'randomSeed' may only be set if parameter 'concurrency' is equal to 1, but got 4.");
    }
}
