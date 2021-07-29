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
package org.neo4j.gds;

import org.junit.jupiter.api.DynamicTest;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public final class WritePropertyConfigProcTest {

    public static List<DynamicTest> test(
        AlgoBaseProc<?, ?, ?> proc,
        CypherMapWrapper config
    ) {
        return List.of(
            unspecifiedWriteProperty(proc, config)
        );
    }

    private WritePropertyConfigProcTest() {}

    private static DynamicTest unspecifiedWriteProperty(
        AlgoBaseProc<?, ?, ?> proc,
        CypherMapWrapper config
    ) {
        return DynamicTest.dynamicTest("unspecifiedWriteProperty", () -> {
            assertThatThrownBy(() -> proc.newConfig(Optional.of("ignored"), config.withoutEntry("writeProperty")))
                .hasMessageContaining("writeProperty")
                .hasMessageContaining("mandatory");
        });
    }

}
