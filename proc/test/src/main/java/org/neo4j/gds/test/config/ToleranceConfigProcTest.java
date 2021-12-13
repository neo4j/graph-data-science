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
import org.neo4j.gds.config.ToleranceConfig;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public final class ToleranceConfigProcTest {

    public static List<DynamicTest> test(
        AlgoBaseProc<?, ?, ?, ?> proc,
        CypherMapWrapper config
    ) {
        return List.of(
            invalidTolerance(proc, config),
            validTolerance(proc, config)
        );
    }

    private ToleranceConfigProcTest() {}

    private static DynamicTest invalidTolerance(
        AlgoBaseProc<?, ?, ?, ?> proc,
        CypherMapWrapper config
    ) {
        return DynamicTest.dynamicTest("invalidTolerance", () -> {
            assertThatThrownBy(() -> proc.configParser().processInput(config.withNumber("tolerance", -0.1).toMap()))
                .hasMessageContaining("tolerance")
                .hasMessageContaining("-0.1");
        });
    }

    private static DynamicTest validTolerance(
        AlgoBaseProc<?, ?, ?, ?> proc,
        CypherMapWrapper config
    ) {
        return DynamicTest.dynamicTest("validTolerance", () -> {
            var toleranceConfig = config.withNumber("tolerance", 42.42);
            var algoConfig = ((ToleranceConfig) proc.configParser().processInput(toleranceConfig.toMap()));
            assertThat(algoConfig.tolerance()).isEqualTo(42.42);
        });
    }
}
