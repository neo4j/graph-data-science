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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.config.AlgoBaseConfig;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.QueryRunner.runQuery;
import static org.neo4j.gds.utils.ExceptionUtil.rootCause;

public interface OnlyUndirectedTest<ALGORITHM extends Algorithm<RESULT>, CONFIG extends AlgoBaseConfig, RESULT> extends AlgoBaseProcTest<ALGORITHM, CONFIG, RESULT> {

    @Test
    default void validateUndirected() {
        runQuery(graphDb(), "CALL gds.graph.project('directed', '*', '*')");

        CypherMapWrapper config = createMinimalConfig(CypherMapWrapper.empty());

        applyOnProcedure(proc -> {
            getProcedureMethods(proc)
                .filter(procMethod -> !getProcedureMethodName(procMethod).endsWith(".estimate"))
                .forEach(noneEstimateMethod -> {
                    InvocationTargetException ex = assertThrows(
                        InvocationTargetException.class,
                        () -> {
                            noneEstimateMethod.invoke(proc, "directed", config.toMap());
                        }
                    );
                    assertThat(
                        rootCause(ex).getMessage(),
                        containsString(expectedValidationMessage())
                    );
                });
        });

    }

    @MethodSource("filtered")
    @ParameterizedTest(name = "Orientation(s): {1}")
    default void validateUndirectedFiltering(List<String> filter, String ignoredModeName) {
        runQuery(graphDb(), "CALL gds.graph.project('directedMultiRels', '*', {" +
                            "  R: { type: '*', orientation: 'REVERSE' }, " +
                            "  U: { type: '*', orientation: 'UNDIRECTED' }, " +
                            "  N: { type: '*', orientation: 'NATURAL' } " +
                            "})");

        CypherMapWrapper config = createMinimalConfig(CypherMapWrapper
            .empty()
            .withEntry("relationshipTypes", filter));

        applyOnProcedure(proc -> {
            getProcedureMethods(proc)
                .filter(procMethod -> !getProcedureMethodName(procMethod).endsWith(".estimate"))
                .forEach(noneEstimateMethod -> {
                    InvocationTargetException ex = assertThrows(
                        InvocationTargetException.class,
                        () -> {
                            noneEstimateMethod.invoke(
                                proc,
                                "directedMultiRels",
                                config.toMap()
                            );
                        }
                    );
                    assertThat(
                        rootCause(ex).getMessage(),
                        containsString(expectedValidationMessage())
                    );
                });
        });
    }

    default String expectedValidationMessage() {
        return "Procedure requires relationship projections to be UNDIRECTED.";
    }

    static Stream<Arguments> filtered() {
        return Stream.of(
            Arguments.of(
                List.of("N"),
                "Natural"
            ),
            Arguments.of(
                List.of("R"),
                "Reverse"
            ),
            Arguments.of(
                List.of("U", "R"),
                "Undirected and Reverse"
            ),
            Arguments.of(
                List.of("U", "N"),
                "Undirected and Natural"
            ),
            Arguments.of(
                List.of("R", "N"),
                "Reverse and Natural"
            ),
            Arguments.of(
                List.of("*"),
                "All"
            )
        );
    }

}
