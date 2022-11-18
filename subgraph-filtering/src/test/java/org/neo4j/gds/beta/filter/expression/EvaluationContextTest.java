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
package org.neo4j.gds.beta.filter.expression;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@GdlExtension
public class EvaluationContextTest {

    @GdlGraph
    public static final String GDL = "(a:A:B:C { p1: 42.0, p2: 42 })-[:REL { baz: 84.0 }]->(b:B { p1: 1337.0, p2: 1337 })";

    @Inject
    private GraphStore graphStore;

    @Inject
    private IdFunction idFunction;

    private static Stream<Arguments> nodesPositive() {
        return Stream.of(
            Arguments.of("a", "p1", 42.0, ValueType.DOUBLE, List.of()),
            Arguments.of("a", "p1", 42.0, ValueType.DOUBLE, List.of("A")),
            Arguments.of("a", "p1", 42.0, ValueType.DOUBLE, List.of("A", "B")),
            Arguments.of("a", "p1", 42.0, ValueType.DOUBLE, List.of("A", "B", "C")),

            Arguments.of("a", "p2", Double.longBitsToDouble(42), ValueType.LONG, List.of()),
            Arguments.of("a", "p2", Double.longBitsToDouble(42), ValueType.LONG, List.of("A")),
            Arguments.of("a", "p2", Double.longBitsToDouble(42), ValueType.LONG, List.of("A", "B")),
            Arguments.of("a", "p2", Double.longBitsToDouble(42), ValueType.LONG, List.of("A", "B", "C")),

            Arguments.of("b", "p1", 1337.0, ValueType.DOUBLE, List.of()),
            Arguments.of("b", "p1", 1337.0, ValueType.DOUBLE, List.of("B")),

            Arguments.of("b", "p2", Double.longBitsToDouble(1337), ValueType.LONG, List.of()),
            Arguments.of("b", "p2", Double.longBitsToDouble(1337), ValueType.LONG, List.of("B"))
        );
    }

    @ParameterizedTest
    @MethodSource("nodesPositive")
    void nodeEvaluationContextPositive(
        String variable,
        String propertyKey,
        Object expectedValue,
        ValueType valueType,
        List<String> expectedLabels
    ) {
        var context = new EvaluationContext.NodeEvaluationContext(graphStore, Map.of());
        context.init(idFunction.of(variable));
        assertThat(context.getProperty(propertyKey, valueType)).isEqualTo(expectedValue);
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
        var context = new EvaluationContext.NodeEvaluationContext(graphStore, Map.of());
        context.init(idFunction.of(variable));
        assertThat(context.hasLabelsOrTypes(unExpectedLabels)).isFalse();
    }

    @Test
    void relationshipEvaluationContextPositive() {
        var context = new EvaluationContext.RelationshipEvaluationContext(Map.of("baz", 0), Map.of());
        context.init(RelationshipType.of("REL"), new double[]{84});
        assertThat(context.getProperty("baz", ValueType.DOUBLE)).isEqualTo(84);
        assertThat(context.hasLabelsOrTypes(List.of("REL"))).isTrue();
    }

    @Test
    void relationshipEvaluationContextNegative() {
        var context = new EvaluationContext.RelationshipEvaluationContext(Map.of(), Map.of());
        context.init(RelationshipType.of("REL"));
        assertThat(context.hasLabelsOrTypes(List.of("BAR"))).isFalse();
    }
}
