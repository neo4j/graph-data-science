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
package org.neo4j.graphalgo.beta.filter.expr;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.DoubleRange;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.graphalgo.beta.filter.expr.Expression.FALSE;
import static org.neo4j.graphalgo.beta.filter.expr.Expression.TRUE;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class ExpressionEvaluatorTest {

    @Test
    void trueLiteral() throws ParseException {
        var expr = ExpressionParser.parse("TRUE");
        assertThat(expr.evaluate(EMPTY_CONTEXT)).isEqualTo(TRUE);
    }

    @Test
    void falseLiteral() throws ParseException {
        var expr = ExpressionParser.parse("FALSE");
        assertThat(expr.evaluate(EMPTY_CONTEXT)).isEqualTo(FALSE);
    }

    @Property
    void longLiteral(@ForAll long input) throws ParseException {
        var expr = ExpressionParser.parse(Long.toString(input));
        assertThat(expr.evaluate(EMPTY_CONTEXT)).isEqualTo(input);
    }

    @Property
    void doubleLiteral(@ForAll double input) throws ParseException {
        var expr = ExpressionParser.parse(Double.toString(input));
        assertThat(expr.evaluate(EMPTY_CONTEXT)).isEqualTo(input);
    }

    @Property
    void comparison(@ForAll double left, @ForAll double right) throws ParseException {
        var compare = Double.compare(left, right);
        var expression = compare < 0
            ? "%f < %f"
            : compare > 0
                ? "%f > %f"
                : "%f = %f";
        var expr = ExpressionParser.parse(formatWithLocale(expression, left, right));
        assertThat(expr.evaluate(EMPTY_CONTEXT) == TRUE).isTrue();
    }

    @Property
    void lessThan(
        @ForAll @DoubleRange(max = 1E10, maxIncluded = false) double left,
        @ForAll @DoubleRange(min = 1E10, minIncluded = false) double right
    ) throws ParseException {
        var expr = ExpressionParser.parse(formatWithLocale("%f < %f", left, right));
        assertThat(expr.evaluate(EMPTY_CONTEXT) == TRUE).isTrue();
    }

    @Property
    void lessThanOrEqual(
        @ForAll @DoubleRange(max = 1E10) double left,
        @ForAll @DoubleRange(min = 1E10) double right
    ) throws ParseException {
        var expr = ExpressionParser.parse(formatWithLocale("%f <= %f", left, right));
        assertThat(expr.evaluate(EMPTY_CONTEXT) == TRUE).isTrue();
    }

    @Property
    void greaterThan(
        @ForAll @DoubleRange(min = 1E10, minIncluded = false) double left,
        @ForAll @DoubleRange(max = 1E10, maxIncluded = false) double right
    ) throws ParseException {
        var expr = ExpressionParser.parse(formatWithLocale("%f > %f", left, right));
        assertThat(expr.evaluate(EMPTY_CONTEXT) == TRUE).isTrue();
    }

    @Property
    void greaterThanOrEqual(
        @ForAll @DoubleRange(min = 1E10) double left,
        @ForAll @DoubleRange(max = 1E10) double right
    ) throws ParseException {
        var expr = ExpressionParser.parse(formatWithLocale("%f >= %f", left, right));
        assertThat(expr.evaluate(EMPTY_CONTEXT) == TRUE).isTrue();
    }

    @Property
    void equal(@ForAll double value) throws ParseException {
        var expr = ExpressionParser.parse(formatWithLocale("%f = %f", value, value));
        assertThat(expr.evaluate(EMPTY_CONTEXT) == TRUE).isTrue();
    }

    @Property
    void notEqual(
        @ForAll @DoubleRange(min = 1E10, minIncluded = false) double left,
        @ForAll @DoubleRange(max = 1E10, maxIncluded = false) double right
    ) throws ParseException {
        var expr = ExpressionParser.parse(formatWithLocale("%f <> %f", left, right));
        assertThat(expr.evaluate(EMPTY_CONTEXT) == TRUE).isTrue();

        expr = ExpressionParser.parse(formatWithLocale("%f != %f", left, right));
        assertThat(expr.evaluate(EMPTY_CONTEXT) == TRUE).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"NOT TRUE", "NOT (1337 > 42)"})
    void notTrue(String cypher) throws ParseException {
        var expr = ExpressionParser.parse(cypher);
        assertThat(expr.evaluate(EMPTY_CONTEXT) == TRUE).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"NOT FALSE", "NOT (1337 < 42)"})
    void notFalse(String cypher) throws ParseException {
        var expr = ExpressionParser.parse(cypher);
        assertThat(expr.evaluate(EMPTY_CONTEXT) == TRUE).isTrue();
    }

    @ParameterizedTest
    @CsvSource(value = {
        "false,false,false",
        "true,false,false",
        "false,true,false",
        "true,true,true"
    })
    void and(boolean left, boolean right, boolean expected) throws ParseException {
        var expr = ExpressionParser.parse(formatWithLocale("%s AND %s", left, right));
        assertThat(expr.evaluate(EMPTY_CONTEXT) == TRUE).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "false,false,false",
        "true,false,true",
        "false,true,true",
        "true,true,true"
    })
    void or(boolean left, boolean right, boolean expected) throws ParseException {
        var expr = ExpressionParser.parse(formatWithLocale("%s OR %s", left, right));
        assertThat(expr.evaluate(EMPTY_CONTEXT) == TRUE).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource(value = {
        "false,false,false",
        "true,false,true",
        "false,true,true",
        "true,true,false"
    })
    void xor(boolean left, boolean right, boolean expected) throws ParseException {
        var expr = ExpressionParser.parse(formatWithLocale("%s XOR %s", left, right));
        assertThat(expr.evaluate(EMPTY_CONTEXT) == TRUE).isEqualTo(expected);
    }

    @Test
    void property() throws ParseException {
        var expr = ExpressionParser.parse("n.foo");
        var context = TestEvaluationContext.of("foo", 42, true);
        assertThat(expr.evaluate(context) == 42).isTrue();
    }

    private static final EvaluationContext EMPTY_CONTEXT = new EvaluationContext() {
        @Override
        double getProperty(String propertyKey) {
            throw new UnsupportedOperationException();
        }

        @Override
        boolean hasLabelsOrTypes(List<String> labelsOrTypes) {
            throw new UnsupportedOperationException();
        }
    };

    static class TestEvaluationContext extends EvaluationContext {

        private final String propertyKey;
        private final double value;

        private final boolean hasLabelOrRelType;

        TestEvaluationContext(String propertyKey, double value, boolean hasLabelOrRelType) {
            this.propertyKey = propertyKey;
            this.value = value;
            this.hasLabelOrRelType = hasLabelOrRelType;
        }

        static EvaluationContext of(String propertyKey, double value, boolean hasLabelOrRelType) {
            return new TestEvaluationContext(propertyKey, value, hasLabelOrRelType);
        }

        @Override
        double getProperty(String propertyKey) {
            assertThat(propertyKey).isEqualTo(this.propertyKey);
            return value;
        }

        @Override
        boolean hasLabelsOrTypes(List<String> labelsOrTypes) {
            return hasLabelOrRelType;
        }
    }

}
