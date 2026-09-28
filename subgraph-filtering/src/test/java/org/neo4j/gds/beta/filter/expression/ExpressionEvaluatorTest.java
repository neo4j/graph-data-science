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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        var expr = ExpressionParser.parse("n." + propertyKey, Map.of(propertyKey, propertyType));
        var context = new TestContext(propertyKey);
        context.withProperty(propertyValue);
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
    void hasNodeLabels(List<String> actual, List<String> requested, boolean expected) throws ParseException {
        var labelExpression = requested.stream().map(label -> ":" + label).collect(Collectors.joining());
        var expr = ExpressionParser.parse("n" + labelExpression, Map.of());
        var evaluationContext = new TestContext(actual);
        assertThat(expr.evaluate(evaluationContext) == TRUE).isEqualTo(expected);
    }

    @ParameterizedTest()
    @MethodSource("org.neo4j.gds.beta.filter.expression.ExpressionEvaluatorTest#hasLabelInput")
    void hasRelationshipTypes(List<String> actual, List<String> requested, boolean expected) throws ParseException {
        var labelExpression = requested.stream().map(label -> ":" + label).collect(Collectors.joining());
        var expr = ExpressionParser.parse("r" + labelExpression, Map.of());
        var evaluationContext = new TestContext(actual);
        assertThat(expr.evaluate(evaluationContext) == TRUE).isEqualTo(expected);
    }

    @Test
    void degree() throws ParseException {
        var expr = ExpressionParser.parse("degree('REL')", Map.of());
        var evaluationContext = new TestContext(List.of("REL"));
        assertThat(expr.evaluate(evaluationContext)).isEqualTo(Double.longBitsToDouble(42));
    }

    @Nested
    class ListContains {
        @Test
        void longListLiteral() throws ParseException {
            var expr = ExpressionParser.parse("1 IN [0, 1, 2]", Map.of());
            assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT)).isEqualTo(TRUE);

            expr = ExpressionParser.parse("4 IN [0, 1, 2]", Map.of());
            assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT)).isEqualTo(FALSE);
        }

        @Test
        void doubleListLiteral() throws ParseException {
            var expr = ExpressionParser.parse("0.1 IN [0.0, 0.1, 0.2]", Map.of());
            assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT)).isEqualTo(TRUE);

            expr = ExpressionParser.parse("0.4 IN [0.0, 0.1, 0.2]", Map.of());
            assertThat(expr.evaluate(EMPTY_EVALUATION_CONTEXT)).isEqualTo(FALSE);
        }

        @Test
        void longListProperty() throws ParseException {
            var expr = ExpressionParser.parse("1 IN n.longListProp", Map.of("longListProp", ValueType.LONG_ARRAY));
            var context = new TestContext("longListProp");
            context.withLongArrayProperty(new long[]{0, 1, 2});
            assertThat(expr.evaluate(context)).isEqualTo(TRUE);

            expr = ExpressionParser.parse("4 IN n.longListProp", Map.of("longListProp", ValueType.LONG_ARRAY));
            context.withLongArrayProperty(new long[]{0, 1, 2});
            assertThat(expr.evaluate(context)).isEqualTo(FALSE);
        }

        @Test
        void doubleListProperty() throws ParseException {
            var expr = ExpressionParser.parse("0.1 IN n.doubleListProp", Map.of("doubleListProp", ValueType.DOUBLE_ARRAY));
            var context = new TestContext("doubleListProp");
            context.withDoubleArrayProperty(new double[]{0.0, 0.1, 0.2});
            assertThat(expr.evaluate(context)).isEqualTo(TRUE);

            expr = ExpressionParser.parse("0.4 IN n.doubleListProp", Map.of("doubleListProp", ValueType.DOUBLE_ARRAY));
            context.withDoubleArrayProperty(new double[]{0.0, 0.1, 0.2});
            assertThat(expr.evaluate(context)).isEqualTo(FALSE);
        }

        @Test
        void floatListProperty() throws ParseException {
            var expr = ExpressionParser.parse("0.1 IN n.floatListProp", Map.of("floatListProp", ValueType.FLOAT_ARRAY));
            var context = new TestContext("floatListProp");
            context.withFloatArrayProperty(new float[]{0.0f, 0.1f, 0.2f});
            assertThat(expr.evaluate(context)).isEqualTo(TRUE);

            expr = ExpressionParser.parse("0.4 IN n.floatListProp", Map.of("floatListProp", ValueType.FLOAT_ARRAY));
            context.withFloatArrayProperty(new float[]{0.0f, 0.1f, 0.2f});
            assertThat(expr.evaluate(context)).isEqualTo(FALSE);
        }
    }

    private static final EvaluationContext EMPTY_EVALUATION_CONTEXT = new EvaluationContext(Map.of()) {
        @Override
        double getProperty(String propertyKey, ValueType valueType) {
            throw new UnsupportedOperationException();
        }

        @Override
        long[] longArrayProperty(Expression.UnaryExpression.Property property) {
            throw new UnsupportedOperationException();
        }

        @Override
        double[] doubleArrayProperty(Expression.UnaryExpression.Property property) {
            throw new UnsupportedOperationException();
        }

        @Override
        float[] floatArrayProperty(Expression.UnaryExpression.Property property) {
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

        @Override
        public int degree(Collection<RelationshipType> typeSelection) {
            throw new UnsupportedOperationException();
        }
    };

    static class TestContext extends EvaluationContext {
        private String propertyKey = "";
        private double propertyValue = Double.NaN;
        private List<String> labelsOrTypes = new ArrayList<>();

        private double[] doubleListPropertyValue;
        private long[] longListPropertyValue;
        private float[] floatListPropertyValue;

        TestContext(String propertyKey) {
            super(Map.of());
            this.propertyKey = propertyKey;
        }

        TestContext(List<String> labelsOrTypes) {
            super(Map.of());
            this.labelsOrTypes = labelsOrTypes;
        }

        void withProperty(double value) {
            this.propertyValue = value;
        }

        void withDoubleArrayProperty(double[] doubleArray) {
            this.doubleListPropertyValue = doubleArray;
        }

        void withLongArrayProperty(long[] longArray) {
            this.longListPropertyValue = longArray;
        }

        void withFloatArrayProperty(float[] floatArray) {
            this.floatListPropertyValue = floatArray;
        }

        String propertyKey() {
            return propertyKey;
        }

        List<String> labelsOrTypes() {
            return labelsOrTypes;
        }

        // TODO: do we need some test that actually use the valueType?
        @Override
        double getProperty(String propertyKey, ValueType valueType) {
            assertThat(propertyKey).isEqualTo(propertyKey());
            return propertyValue;
        }

        @Override
        long[] longArrayProperty(Expression.UnaryExpression.Property property) {
            assertThat(property.propertyKey()).isEqualTo(propertyKey());
            return longListPropertyValue;
        }

        @Override
        double[] doubleArrayProperty(Expression.UnaryExpression.Property property) {
            assertThat(property.propertyKey()).isEqualTo(propertyKey());
            return doubleListPropertyValue;
        }

        @Override
        float[] floatArrayProperty(Expression.UnaryExpression.Property property) {
            assertThat(property.propertyKey()).isEqualTo(propertyKey());
            return floatListPropertyValue;
        }

        @Override
        public boolean hasNodeLabels(List<NodeLabel> labels) {
            return hasLabelsOrTypes(labels.stream().map(NodeLabel::name).toList());
        }

        @Override
        public boolean hasRelationshipTypes(List<RelationshipType> types) {
            return hasLabelsOrTypes(types.stream().map(RelationshipType::name).toList());
        }

        @Override
        public boolean hasLabelsOrTypes(List<String> labelsOrTypes) {
            return labelsOrTypes().containsAll(labelsOrTypes);
        }

        @Override
        public int degree(Collection<RelationshipType> typeSelection) {
            assertThat(labelsOrTypes()).containsExactlyElementsOf(
                typeSelection.stream()
                    .map(RelationshipType::name)
                    .collect(Collectors.toList())
            );
            return 42;
        }
    }

}
