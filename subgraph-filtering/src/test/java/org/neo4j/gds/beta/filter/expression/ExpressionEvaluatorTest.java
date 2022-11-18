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

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.DoubleRange;
import net.jqwik.api.constraints.LongRange;
import org.immutables.value.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.beta.filter.expression.Expression.FALSE;
import static org.neo4j.gds.beta.filter.expression.Expression.TRUE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class ExpressionEvaluatorTest {

    @Test
    void trueLiteral() throws ParseException {
        var expr = ExpressionParser.parse("TRUE", Map.of());
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT)).isEqualTo(TRUE);
    }

    @Test
    void falseLiteral() throws ParseException {
        var expr = ExpressionParser.parse("FALSE", Map.of());
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT)).isEqualTo(FALSE);
    }

    @Property
    void longLiteral(@ForAll long input) throws ParseException {
        var doubleInput = Double.longBitsToDouble(input);
        if (!Double.isNaN(doubleInput)) {
            var expr = ExpressionParser.parse(Long.toString(input), Map.of());
            assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT)).isEqualTo(doubleInput);
        }
    }

    @Property
    void doubleLiteral(@ForAll double input) throws ParseException {
        var expr = ExpressionParser.parse(Double.toString(input), Map.of());
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT)).isEqualTo(input);
    }

    @Property
    void doubleComparison(@ForAll double left, @ForAll double right) throws ParseException {
        var compare = Double.compare(left, right);
        var expression = compare < 0
            ? "%f < %f"
            : compare > 0
                ? "%f > %f"
                : "%f = %f";
        var expr = ExpressionParser.parse(
            formatWithLocale(expression, left, right),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isTrue();
    }

    @Property
    void longComparison(@ForAll long left, @ForAll long right) throws ParseException {
        var compare = Long.compare(left, right);
        var expression = compare < 0
            ? "%d < %d"
            : compare > 0
                ? "%d > %d"
                : "%d = %d";
        var expr = ExpressionParser.parse(
            formatWithLocale(expression, left, right),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isTrue();
    }

    @Property
    void doubleLessThan(
        @ForAll @DoubleRange(max = 1E10, maxIncluded = false) double left,
        @ForAll @DoubleRange(min = 1E10, minIncluded = false) double right
    ) throws ParseException {
        var expr = ExpressionParser.parse(
            formatWithLocale("%f < %f", left, right),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isTrue();
    }

    @Property
    void longLessThan(
        @ForAll @LongRange(max = 10_000_000_000L) long left,
        @ForAll @LongRange(min = 10_000_000_001L) long right
    ) throws ParseException {
        var expr = ExpressionParser.parse(
            formatWithLocale("%d < %d", left, right),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isTrue();
    }

    @Property
    void doubleLessThanOrEqual(
        @ForAll @DoubleRange(max = 1E10) double left,
        @ForAll @DoubleRange(min = 1E10) double right
    ) throws ParseException {
        var expr = ExpressionParser.parse(
            formatWithLocale("%f <= %f", left, right),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isTrue();
    }

    @Property
    void longLessThanOrEqual(
        @ForAll @LongRange(max = 10_000_000_000L) long left,
        @ForAll @LongRange(min = 10_000_000_000L) long right
    ) throws ParseException {
        var expr = ExpressionParser.parse(
            formatWithLocale("%d <= %d", left, right),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isTrue();
    }

    @Property
    void doubleGreaterThan(
        @ForAll @DoubleRange(min = 1E10, minIncluded = false) double left,
        @ForAll @DoubleRange(max = 1E10, maxIncluded = false) double right
    ) throws ParseException {
        var expr = ExpressionParser.parse(
            formatWithLocale("%f > %f", left, right),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isTrue();
    }

    @Property
    void longGreaterThan(
        @ForAll @LongRange(min = 10_000_000_001L) long left,
        @ForAll @LongRange(max = 10_000_000_000L) long right
    ) throws ParseException {
        var expr = ExpressionParser.parse(
            formatWithLocale("%d > %d", left, right),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isTrue();
    }

    @Property
    void doubleGreaterThanOrEqual(
        @ForAll @DoubleRange(min = 1E10) double left,
        @ForAll @DoubleRange(max = 1E10) double right
    ) throws ParseException {
        var expr = ExpressionParser.parse(
            formatWithLocale("%f >= %f", left, right),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isTrue();
    }

    @Property
    void longGreaterThanOrEqual(
        @ForAll @LongRange(min = 10_000_000_000L) long left,
        @ForAll @LongRange(max = 10_000_000_000L) long right
    ) throws ParseException {
        var expr = ExpressionParser.parse(
            formatWithLocale("%d >= %d", left, right),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isTrue();
    }


    @Property
    void doubleEqual(@ForAll double value) throws ParseException {
        var expr = ExpressionParser.parse(
            formatWithLocale("%f = %f", value, value),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isTrue();
    }

    @Property
    void doubleNotEqual(
        @ForAll @DoubleRange(min = 1E10, minIncluded = false) double left,
        @ForAll @DoubleRange(max = 1E10, maxIncluded = false) double right
    ) throws ParseException {
        var expr = ExpressionParser.parse(
            formatWithLocale("%f <> %f", left, right),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isTrue();

        expr = ExpressionParser.parse(
            formatWithLocale("%f != %f", left, right),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isTrue();
    }

    @Property
    void longEqual(@ForAll long value) throws ParseException {
        var expr = ExpressionParser.parse(
            formatWithLocale("%d = %d", value, value),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isTrue();
    }

    @Property
    void longNotEqual(
        @ForAll @LongRange(min = 10_000_000_001L) long left,
        @ForAll @LongRange(max = 10_000_000_000L) long right
    ) throws ParseException {
        var expr = ExpressionParser.parse(
            formatWithLocale("%d <> %d", left, right),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isTrue();

        expr = ExpressionParser.parse(
            formatWithLocale("%d != %d", left, right),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isTrue();
    }

    @Test
    void longEqualHandleNaN() throws ParseException {
        long baseValue = Double.doubleToRawLongBits(Double.NaN);
        long value1 = baseValue + 42;
        long value2 = baseValue + 1337;
        var expr = ExpressionParser.parse(
            formatWithLocale("%d = %d", value1, value2),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isFalse();
    }

    @Test
    void longNotEqualHandleNaN() throws ParseException {
        long baseValue = Double.doubleToRawLongBits(Double.NaN);
        long value1 = baseValue + 42;
        long value2 = baseValue + 1337;
        var expr = ExpressionParser.parse(
            formatWithLocale("%d <> %d", value1, value2),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"NOT TRUE", "NOT (1337 > 42)"})
    void notTrue(String cypher) throws ParseException {
        var expr = ExpressionParser.parse(cypher, Map.of());
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"NOT FALSE", "NOT (1337 < 42)"})
    void notFalse(String cypher) throws ParseException {
        var expr = ExpressionParser.parse(cypher, Map.of());
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isTrue();
    }

    @ParameterizedTest
    @CsvSource(value = {
        "false,false,false",
        "true,false,false",
        "false,true,false",
        "true,true,true"
    })
    void and(boolean left, boolean right, boolean expected) throws ParseException {
        var expr = ExpressionParser.parse(
            formatWithLocale("%s AND %s", left, right),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "false,false,false",
        "true,false,true",
        "false,true,true",
        "true,true,true"
    })
    void or(boolean left, boolean right, boolean expected) throws ParseException {
        var expr = ExpressionParser.parse(
            formatWithLocale("%s OR %s", left, right),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "false,false,false",
        "true,false,true",
        "false,true,true",
        "true,true,false"
    })
    void xor(boolean left, boolean right, boolean expected) throws ParseException {
        var expr = ExpressionParser.parse(
            formatWithLocale("%s XOR %s", left, right),
            Map.of()
        );
        assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT) == TRUE).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "foo,42,LONG",
        "bar,1.337,DOUBLE"
    })
    void property(String propertyKey, double propertyValue, ValueType propertyType) throws ParseException {
        var validationContext = ImmutableValidationContext.builder()
            .context(ValidationContext.Context.NODE)
            .putAvailableProperty(propertyKey, propertyType)
            .build();

        var expr = ExpressionParser.parse("n." + propertyKey, validationContext.availableProperties());
        var context = ImmutableTestContext.builder().propertyKey(propertyKey).propertyValue(propertyValue).build();
        assertThat(expr.evaluate(context) == propertyValue).isTrue();
    }

    static Stream<Arguments> hasLabelInput() {
        return Stream.of(
            Arguments.arguments(List.of("A"), List.of("A"), true),
            Arguments.arguments(List.of("A", "B"), List.of("A"), true),
            Arguments.arguments(List.of("A", "B"), List.of("A", "B"), true),
            Arguments.arguments(List.of("A"), List.of("B"), false),
            Arguments.arguments(List.of("A", "B"), List.of("B", "C"), false)
        );
    }

    @ParameterizedTest()
    @MethodSource("org.neo4j.gds.beta.filter.expression.ExpressionEvaluatorTest#hasLabelInput")
    void hasNodeLabels(Iterable<String> actual, Collection<String> requested, boolean expected) throws ParseException {
        var labelExpression = requested.stream().map(label -> ":" + label).collect(Collectors.joining());

        var validationContext = ImmutableValidationContext.builder()
            .context(ValidationContext.Context.NODE)
            .addAllAvailableNodeLabels(StreamSupport.stream(actual.spliterator(), false).map(NodeLabel::of).collect(Collectors.toList()))
            .build();

        var expr = ExpressionParser.parse("n" + labelExpression, validationContext.availableProperties());

        var evaluationContext = ImmutableTestContext.builder().addAllLabelsOrTypes(actual).build();
        assertThat(expr.evaluate(evaluationContext) == TRUE).isEqualTo(expected);
    }

    @ParameterizedTest()
    @MethodSource("org.neo4j.gds.beta.filter.expression.ExpressionEvaluatorTest#hasLabelInput")
    void hasRelationshipTypes(Iterable<String> actual, Collection<String> requested, boolean expected) throws ParseException {
        var labelExpression = requested.stream().map(label -> ":" + label).collect(Collectors.joining());

        var validationContext = ImmutableValidationContext.builder()
            .context(ValidationContext.Context.RELATIONSHIP)
            .addAllAvailableRelationshipTypes(StreamSupport.stream(actual.spliterator(), false).map(RelationshipType::of).collect(Collectors.toList()))
            .build();

        var expr = ExpressionParser.parse("r" + labelExpression, validationContext.availableProperties());

        var evaluationContext = ImmutableTestContext.builder().addAllLabelsOrTypes(actual).build();
        assertThat(expr.evaluate(evaluationContext) == TRUE).isEqualTo(expected);
    }

    private static final EvaluationContext EMPTY_EVALUATION_CONTEXT = new EvaluationContext(Map.of()) {
        @Override
        double getProperty(String propertyKey, ValueType valueType) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNodeLabels(List<NodeLabel> labels) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasRelationshipTypes(List<RelationshipType> types) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasLabelsOrTypes(List<String> labelsOrTypes) {
            throw new UnsupportedOperationException();
        }
    };

    @ValueClass
    static class TestContext extends EvaluationContext {

        TestContext() {
            super(Map.of());
        }

        @Value.Default
        public String propertyKey() {
            return "";
        }

        @Value.Default
        public double propertyValue() {
            return Double.NaN;
        }

        @Value.Default
        public List<String> labelsOrTypes() {
            return List.of();
        }

        // TODO: do we need some test that actually use the valueType?
        @Override
        @Value.Derived
        double getProperty(String propertyKey, ValueType valueType) {
            assertThat(propertyKey).isEqualTo(propertyKey());
            return propertyValue();
        }

        @Override
        public boolean hasNodeLabels(List<NodeLabel> labels) {
            return hasLabelsOrTypes(labels.stream().map(NodeLabel::name).collect(Collectors.toList()));
        }

        @Override
        public boolean hasRelationshipTypes(List<RelationshipType> types) {
            return hasLabelsOrTypes(types.stream().map(RelationshipType::name).collect(Collectors.toList()));
        }

        @Override
        @Value.Derived
        public boolean hasLabelsOrTypes(List<String> labelsOrTypes) {
            return labelsOrTypes().containsAll(labelsOrTypes);
        }
    }

}
