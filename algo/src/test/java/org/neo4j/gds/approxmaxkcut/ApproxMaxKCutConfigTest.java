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
package org.neo4j.gds.approxmaxkcut;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.approxmaxkcut.config.ImmutableApproxMaxKCutBaseConfig;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

final class ApproxMaxKCutConfigTest {

    @Test
    void invalidRandomParameters() {
        var configBuilder = ImmutableApproxMaxKCutBaseConfig.builder()
            .concurrency(4)
            .randomSeed(1337L);
        assertThrows(IllegalArgumentException.class, configBuilder::build);
    }

    @Test
    void invalidMinCommunitySizes() {
        var minConfigBuilder = ImmutableApproxMaxKCutBaseConfig.builder()
            .minimize(true)
            .minCommunitySizes(List.of(0L, 1L));
        assertThrows(IllegalArgumentException.class, minConfigBuilder::build);

        var maxConfigBuilder = ImmutableApproxMaxKCutBaseConfig.builder()
            .minimize(false)
            .minCommunitySizes(List.of(-1L, 1L));
        assertThrows(IllegalArgumentException.class, maxConfigBuilder::build);
    }
}
