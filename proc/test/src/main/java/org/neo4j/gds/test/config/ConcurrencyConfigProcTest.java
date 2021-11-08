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
package org.neo4j.gds.test.config;

import org.junit.jupiter.api.DynamicTest;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.config.WriteConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.test.config.ConfigProcTestHelpers.GRAPH_NAME;

public final class ConcurrencyConfigProcTest {

    private ConcurrencyConfigProcTest() {}

    public static <C extends AlgoBaseConfig & ConcurrencyConfig> List<DynamicTest> test(
        AlgoBaseProc<?, ?, C> proc,
        CypherMapWrapper config
    ) {
        return List.of(
            lowConcurrency(proc, config),
            tooHighConcurrency(proc, config)
        );
    }

    public static <C extends AlgoBaseConfig & ConcurrencyConfig & WriteConfig> List<DynamicTest> writeTest(
        AlgoBaseProc<?, ?, C> proc,
        CypherMapWrapper config
    ) {
        return List.of(
            lowWriteConcurrency(proc, config),
            tooHighWriteConcurrency(proc, config)
        );
    }

    private static <C extends AlgoBaseConfig & ConcurrencyConfig> DynamicTest lowConcurrency(
        AlgoBaseProc<?, ?, C> proc,
        CypherMapWrapper config
    ) {
        return DynamicTest.dynamicTest("lowConcurrency", () -> {
            var concurrencyConfig = config.withNumber("concurrency", 2);
            var algoConfig = proc.newConfig(GRAPH_NAME, concurrencyConfig);
            assertThat(algoConfig.concurrency()).isEqualTo(2);
        });
    }

    private static <C extends AlgoBaseConfig & ConcurrencyConfig & WriteConfig> DynamicTest lowWriteConcurrency(
        AlgoBaseProc<?, ?, C> proc,
        CypherMapWrapper config
    ) {
        return DynamicTest.dynamicTest("lowWriteConcurrency", () -> {
            var concurrencyConfig = config.withNumber("writeConcurrency", 2);
            var algoConfig = proc.newConfig(GRAPH_NAME, concurrencyConfig);
            assertThat(algoConfig.writeConcurrency()).isEqualTo(2);
        });
    }

    private static <C extends AlgoBaseConfig & ConcurrencyConfig> DynamicTest tooHighConcurrency(
        AlgoBaseProc<?, ?, C> proc,
        CypherMapWrapper config
    ) {
        return DynamicTest.dynamicTest("tooHighConcurrency", () -> {
            var concurrencyConfig = config.withNumber("concurrency", 1337);
            assertThatThrownBy(() -> proc.newConfig(GRAPH_NAME, concurrencyConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("concurrency")
                .hasMessageContaining("1337");
        });
    }

    private static <C extends AlgoBaseConfig & ConcurrencyConfig> DynamicTest tooHighWriteConcurrency(
        AlgoBaseProc<?, ?, C> proc,
        CypherMapWrapper config
    ) {
        return DynamicTest.dynamicTest("tooHighWriteConcurrency", () -> {
            var concurrencyConfig = config.withNumber("writeConcurrency", 1337);
            assertThatThrownBy(() -> proc.newConfig(GRAPH_NAME, concurrencyConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("writeConcurrency")
                .hasMessageContaining("1337");
        });
    }

}
