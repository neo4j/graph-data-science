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
package org.neo4j.graphalgo.doc.syntax.auto;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProcedureNameExtractorTest {

    @ParameterizedTest(name = "Parses `{1}`")
    @MethodSource("validCodeSnippets")
    void findsProcedureName(String codeSnippet, String expectedProcedureName) {
        var procedureName = ProcedureNameExtractor.findProcedureName(codeSnippet);

        assertThat(procedureName).isEqualTo(expectedProcedureName);
    }

    @ParameterizedTest(name = "Fails for `{0}`")
    @MethodSource("invalidCodeSnippets")
    void failsOnMissingProcedureName(String input) {
        assertThatThrownBy(() -> ProcedureNameExtractor.findProcedureName(input))
            .hasMessageContaining("No procedure names found");

    }

    private static Stream<Arguments> validCodeSnippets() {
        return Stream.of(
            // Leaving the first one complete with `YIELD` section but this is not necessary for the rest.
            Arguments.of(
                "CALL gds.labelPropagation.stream(\n" +
                "  graphName: String,\n" +
                "  configuration: Map\n" +
                ")\n" +
                "YIELD\n" +
                "    nodeId: Integer,\n" +
                "    communityId: Integer",
                "gds.labelPropagation.stream"
            ),
            Arguments.of(
                "CALL gds.labelPropagation.stats(\n" +
                "  graphName: String,\n" +
                "  configuration: Map\n" +
                ")",
                "gds.labelPropagation.stats"
            ),
            Arguments.of(
                "CALL gds.degreeCentrality.mutate(\n" +
                "  graphName: String,\n" +
                "  configuration: Map\n" +
                ")",
                "gds.degreeCentrality.mutate"
            ),
            Arguments.of(
                "CALL gds.louvain.write(\n" +
                "  graphName: String,\n" +
                "  configuration: Map\n" +
                ")",
                "gds.louvain.write"
            ),
            Arguments.of(
                "CALL gds.beta.k1coloring.write(\n" +
                "  graphName: String,\n" +
                "  configuration: Map\n" +
                ")",
                "gds.beta.k1coloring.write"
            ),
            Arguments.of(
                "CALL gds.beta.graphSage.train(\n" +
                "  graphName: String,\n" +
                "  configuration: Map\n" +
                ")",
                "gds.beta.graphSage.train"
            ),
            Arguments.of(
                "CALL gds.alpha.scc.write(\n" +
                "  graphName: String,\n" +
                "  configuration: Map\n" +
                ")",
                "gds.alpha.scc.write"
            ),
            Arguments.of(
                "CALL gds.alpha.ml.nodeClassification.train(\n" +
                "  graphName: String,\n" +
                "  configuration: Map\n" +
                ")",
                "gds.alpha.ml.nodeClassification.train"
            )
        );
    }

    private static Stream<Arguments> invalidCodeSnippets() {
        return Stream.of(
            Arguments.of("CALL not-a-gds.procedure.stream()"),
            Arguments.of("CALL GDS.procedure.stream()"),
            Arguments.of("What is Lorem Ipsum?")
        );
    }

}
