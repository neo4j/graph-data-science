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
package org.neo4j.gds.wcc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WccBaseConfigTest {

    @Test
    void testThreshold() {
        CypherMapWrapper map = CypherMapWrapper.create(Map.of(
            "threshold", 3.14,
            "relationshipWeightProperty", "threshold"
        ));

        var config = TestWccBaseConfig.of(map);
        assertThat(config.threshold()).isEqualTo(3.14);
    }

    @Test
    void testIntegerThreshold() {
        CypherMapWrapper map = CypherMapWrapper.create(Map.of(
            "threshold", 3,
            "relationshipWeightProperty", "threshold"
        ));

        var config = TestWccBaseConfig.of(map);
        assertThat(config.threshold()).isEqualTo(3);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testConsecutiveIds(boolean consecutiveIds) {
        CypherMapWrapper map = CypherMapWrapper.create(Map.of(
            "consecutiveIds", consecutiveIds
        ));

        var config = TestWccBaseConfig.of(map);
        assertThat(config.consecutiveIds()).isEqualTo(consecutiveIds);
    }

    @Test
    void testFailSeedingAndConsecutiveIds() {
        CypherMapWrapper map = CypherMapWrapper.create(Map.of(
            "consecutiveIds", true,
            "seedProperty", "seed"
        ));

        assertThatThrownBy(() -> TestWccBaseConfig.of(map))
            .hasMessageContaining("Seeding and the `consecutiveIds` option cannot be used at the same time.")
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testFailThresholdWithoutRelationshipWeight() {
        CypherMapWrapper map = CypherMapWrapper.create(Map.of(
            "threshold", 3.14
        ));

        assertThatThrownBy(() -> TestWccBaseConfig.of(map))
            .hasMessageContaining("Specifying a threshold requires `relationshipWeightProperty` to be set")
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Configuration
    interface TestWccBaseConfig extends WccBaseConfig {
        static TestWccBaseConfig of(CypherMapWrapper map) {
            return new TestWccBaseConfigImpl(map);
        }
    }

}
