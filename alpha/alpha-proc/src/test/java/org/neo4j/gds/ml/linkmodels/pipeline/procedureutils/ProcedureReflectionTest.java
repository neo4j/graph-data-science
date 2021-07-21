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
package org.neo4j.gds.ml.linkmodels.pipeline.procedureutils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.BaseProcTest;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class ProcedureReflectionTest extends BaseProcTest {

    @ParameterizedTest
    @ValueSource(strings = {"boogieewoogie", "gds.graph.create", "ageRank", ".pageRank"})
    void shouldFailOnInvalidProc(String proc) {
        assertThatThrownBy(() -> ProcedureReflection.INSTANCE.findProcedureMethod(proc))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage(formatWithLocale("Invalid procedure name `%s` for pipelining.", proc));
    }

    @Test
    void shouldFailOnNonUniqueMatch() {
        assertThatThrownBy(() -> ProcedureReflection.INSTANCE.findProcedureMethod("dijkstra"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageStartingWith(
                "Ambiguous procedure name `dijkstra`. Found matching procedures gds.shortestPath.dijkstra.mutate, gds.allShortestPaths.dijkstra.mutate.");
    }

    @ParameterizedTest
    @ValueSource(strings = {"pageRank", "pageRank.mutate", "gds.pageRank", "gds.pageRank.mutate"})
    void shouldNotFailOnValidName(String name) {
        ProcedureReflection.INSTANCE.findProcedureMethod(name);
    }
}
