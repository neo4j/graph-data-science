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
package org.neo4j.graphalgo.beta.filter.expression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.extension.GdlExtension;
import org.neo4j.graphalgo.extension.GdlGraph;
import org.neo4j.graphalgo.extension.IdFunction;
import org.neo4j.graphalgo.extension.Inject;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@GdlExtension
public class EvaluationContextTest {

    @GdlGraph
    public static final String GDL = "(a:A:B:C { foo: 42 })-[:REL { baz: 84 }]->(b:B { bar: 1337 })";

    @Inject
    private GraphStore graphStore;

    @Inject
    private IdFunction idFunction;

    private static Stream<Arguments> nodesPositive() {
        return Stream.of(
            Arguments.of("a", "foo", 42.0, List.of()),
            Arguments.of("a", "foo", 42.0, List.of("A")),
            Arguments.of("a", "foo", 42.0, List.of("A", "B")),
            Arguments.of("a", "foo", 42.0, List.of("A", "B", "C")),
            Arguments.of("b", "bar", 1337.0, List.of()),
            Arguments.of("b", "bar", 1337.0, List.of("B"))
        );
    }

    @ParameterizedTest
    @MethodSource("nodesPositive")
    void nodeEvaluationContextPositive(
        String variable,
        String propertyKey,
        Object expectedValue,
        List<String> expectedLabels
    ) {
        var context = new EvaluationContext.NodeEvaluationContext(graphStore);
        context.init(idFunction.of(variable));
        assertThat(context.getProperty(propertyKey)).isEqualTo(expectedValue);
        assertThat(context.hasLabelsOrTypes(expectedLabels)).isTrue();
    }

    private static Stream<Arguments> nodesNegative() {
        return Stream.of(
            Arguments.of("a", List.of("D")),
            Arguments.of("a", List.of("D", "E"))
        );
    }

    @ParameterizedTest
    @MethodSource("nodesNegative")
    void nodeEvaluationContextNegative(String variable, List<String> unExpectedLabels) {
        var context = new EvaluationContext.NodeEvaluationContext(graphStore);
        context.init(idFunction.of(variable));
        assertThat(context.hasLabelsOrTypes(unExpectedLabels)).isFalse();
    }

    @Test
    void relationshipEvaluationContextPositive() {
        var context = new EvaluationContext.RelationshipEvaluationContext(Map.of("baz", 0));
        context.init("REL", new double[]{84});
        assertThat(context.getProperty("baz")).isEqualTo(84);
        assertThat(context.hasLabelsOrTypes(List.of("REL"))).isTrue();
    }

    @Test
    void relationshipEvaluationContextNegative() {
        var context = new EvaluationContext.RelationshipEvaluationContext(Map.of());
        context.init("REL");
        assertThat(context.hasLabelsOrTypes(List.of("BAR"))).isFalse();
    }
}
