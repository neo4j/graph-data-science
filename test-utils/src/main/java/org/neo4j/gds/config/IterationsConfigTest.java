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
package org.neo4j.gds.config;

import org.junit.jupiter.api.DynamicTest;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.List;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public final class IterationsConfigTest {

    public static List<DynamicTest> test(
        Function<CypherMapWrapper, IterationsConfig> configurationCreator,
        CypherMapWrapper config
    ) {
        return List.of(
            invalidMaxIterations(
                configurationCreator,
                config
            ),
            validMaxIterations(
                configurationCreator,
                config
            )
        );
    }

    private IterationsConfigTest() {}

    private static DynamicTest invalidMaxIterations(
        Function<CypherMapWrapper, IterationsConfig> configurationCreator,
        CypherMapWrapper config
    ) {
        return DynamicTest.dynamicTest("invalidMaxIterations", () -> {
            assertThatThrownBy(() -> configurationCreator.apply(config.withNumber("maxIterations", 0)))
                .hasMessageContaining("maxIterations")
                .hasMessageContaining("0");
            assertThatThrownBy(() -> configurationCreator.apply(config.withNumber("maxIterations", -10)))
                .hasMessageContaining("maxIterations")
                .hasMessageContaining("-10");
        });
    }

    private static DynamicTest validMaxIterations(
        Function<CypherMapWrapper, IterationsConfig> configurationCreator,
        CypherMapWrapper config
    ) {
        return DynamicTest.dynamicTest("validMaxIterations", () -> {
            var iterationsConfig = config.withNumber("maxIterations", 3);
            var algoConfig = configurationCreator.apply(iterationsConfig);
            assertThat(algoConfig.maxIterations()).isEqualTo(3);
        });
    }
}
