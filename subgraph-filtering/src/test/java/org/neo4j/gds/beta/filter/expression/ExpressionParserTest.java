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
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.utils.StringJoining;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class ExpressionParserTest {

    // unary

    static Stream<Arguments> nots() {
        return Stream.of(
            Arguments.of("NOT TRUE", ImmutableNot.builder().in(ImmutableTrueLiteral.INSTANCE).build()),
            Arguments.of("NOT FALSE", ImmutableNot.builder().in(ImmutableFalseLiteral.INSTANCE).build()),
            Arguments.of(
                "NOT (TRUE OR FALSE)",
                ImmutableNot
                    .builder()
                    .in(ImmutableOr
                        .builder()
                        .lhs(ImmutableTrueLiteral.INSTANCE)
                        .rhs(ImmutableFalseLiteral.INSTANCE)
                        .build())
                    .build()
            )
        );
    }


    @ParameterizedTest
    @MethodSource("nots")
    void nots(String cypher, Expression.UnaryExpression.Not expected) throws ParseException {
        var actual = ExpressionParser.parse(cypher, Map.of());
        assertThat(actual).isEqualTo(expected);
    }

    // literal

    @Test
    void trueLiteral() throws ParseException {
        var actual = ExpressionParser.parse("TRUE", Map.of());
        assertThat(actual).isEqualTo(Expression.Literal.TrueLiteral.INSTANCE);
    }

    @Test
    void falseLiteral() throws ParseException {
        var actual = ExpressionParser.parse("FALSE", Map.of());
        assertThat(actual).isEqualTo(Expression.Literal.FalseLiteral.INSTANCE);
    }

    static Stream<Arguments> longs() {
        return Stream.of(
            Arguments.of("42", ImmutableLongLiteral.of(42)),
            Arguments.of("-42", ImmutableLongLiteral.of(-42)),
            Arguments.of("0", ImmutableLongLiteral.of(0)),
            Arguments.of("1337", ImmutableLongLiteral.of(1337))
        );
    }

    @ParameterizedTest
    @MethodSource("longs")
    void longLiteral(String cypher, Expression.Literal.LongLiteral expected) throws ParseException {
        var actual = ExpressionParser.parse(cypher, Map.of());
        assertThat(actual).isEqualTo(expected);
    }

    static Stream<Arguments> doubles() {
        return Stream.of(
            Arguments.of("42.0", ImmutableDoubleLiteral.of(42.0)),
            Arguments.of("-42.0", ImmutableDoubleLiteral.of(-42.0)),
            Arguments.of("0.0", ImmutableDoubleLiteral.of(0.0)),
            Arguments.of("13.37", ImmutableDoubleLiteral.of(13.37)),
            Arguments.of("-13.37", ImmutableDoubleLiteral.of(-13.37))
        );
    }

    @ParameterizedTest
    @MethodSource("doubles")
    void doubleLiteral(String cypher, Expression.Literal.DoubleLiteral expected) throws ParseException {
        var actual = ExpressionParser.parse(cypher, Map.of());
        assertThat(actual).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"Foo", "bar", "BAZ", "42"})
    void stringLiteral(String string) throws ParseException {
        var actual = ExpressionParser.parse("'" + string + "'", Map.of());
        assertThat(actual).isEqualTo(ImmutableStringLiteral.builder().value(string).build());
    }

    // binary

    static Stream<Arguments> ands() {
        return Stream.of(
            Arguments.of(
                "TRUE AND FALSE",
                ImmutableAnd.builder().lhs(ImmutableTrueLiteral.INSTANCE).rhs(ImmutableFalseLiteral.INSTANCE).build()
            ),
            Arguments.of(
                "TRUE AND TRUE",
                ImmutableAnd.builder().lhs(ImmutableTrueLiteral.INSTANCE).rhs(ImmutableTrueLiteral.INSTANCE).build()
            ),
            Arguments.of(
                "TRUE AND TRUE AND FALSE",
                ImmutableAnd.builder()
                    .lhs(
                        ImmutableAnd
                            .builder()
                            .lhs(ImmutableTrueLiteral.INSTANCE)
                            .rhs(ImmutableTrueLiteral.INSTANCE)
                            .build()
                    )
                    .rhs(ImmutableFalseLiteral.INSTANCE)
                    .build()
            )
        );
    }

    @ParameterizedTest
    @MethodSource("ands")
    void and(String cypher, Expression.BinaryExpression.And expected) throws ParseException {
        var actual = ExpressionParser.parse(cypher, Map.of());
        assertThat(actual).isEqualTo(expected);
    }

    static Stream<Arguments> ors() {
        return Stream.of(
            Arguments.of(
                "TRUE OR FALSE",
                ImmutableOr.builder().lhs(ImmutableTrueLiteral.INSTANCE).rhs(ImmutableFalseLiteral.INSTANCE).build()
            ),
            Arguments.of(
                "TRUE OR TRUE",
                ImmutableOr.builder().lhs(ImmutableTrueLiteral.INSTANCE).rhs(ImmutableTrueLiteral.INSTANCE).build()
            ),
            Arguments.of(
                "TRUE OR TRUE OR FALSE",
                ImmutableOr.builder()
                    .lhs(
                        ImmutableOr.builder()
                            .lhs(ImmutableTrueLiteral.INSTANCE)
                            .rhs(ImmutableTrueLiteral.INSTANCE)
                            .build()
                    ).rhs(ImmutableFalseLiteral.INSTANCE)
                    .build()
            )
        );
    }

    @ParameterizedTest
    @MethodSource("ors")
    void or(String cypher, Expression.BinaryExpression.Or expected) throws ParseException {
        var actual = ExpressionParser.parse(cypher, Map.of());
        assertThat(actual).isEqualTo(expected);
    }

    static Stream<Arguments> xors() {
        return Stream.of(
            Arguments.of(
                "TRUE XOR FALSE",
                ImmutableXor.builder().lhs(ImmutableTrueLiteral.INSTANCE).rhs(ImmutableFalseLiteral.INSTANCE).build()
            ),
            Arguments.of(
                "TRUE XOR TRUE",
                ImmutableXor.builder().lhs(ImmutableTrueLiteral.INSTANCE).rhs(ImmutableTrueLiteral.INSTANCE).build()
            ),
            Arguments.of(
                "TRUE XOR TRUE XOR FALSE",
                ImmutableXor.builder()
                    .lhs(ImmutableXor
                        .builder()
                        .lhs(ImmutableTrueLiteral.INSTANCE)
                        .rhs(ImmutableTrueLiteral.INSTANCE)
                        .build())
                    .rhs(ImmutableFalseLiteral.INSTANCE)
                    .build()
            )
        );
    }

    @ParameterizedTest
    @MethodSource("xors")
    void xor(String cypher, Expression.BinaryExpression.Xor expected) throws ParseException {
        var actual = ExpressionParser.parse(cypher, Map.of());
        assertThat(actual).isEqualTo(expected);
    }

    static Stream<Arguments> equals() {
        return Stream.of(
            Arguments.of(
                "TRUE = FALSE",
                ImmutableEqual.builder().lhs(ImmutableTrueLiteral.INSTANCE).rhs(ImmutableFalseLiteral.INSTANCE).build()
            ),
            Arguments.of(
                "TRUE = TRUE",
                ImmutableEqual.builder().lhs(ImmutableTrueLiteral.INSTANCE).rhs(ImmutableTrueLiteral.INSTANCE).build()
            ),
            Arguments.of(
                "TRUE = (TRUE = FALSE)",
                ImmutableEqual.builder()
                    .lhs(ImmutableTrueLiteral.INSTANCE)
                    .rhs(
                        ImmutableEqual.builder()
                            .lhs(ImmutableTrueLiteral.INSTANCE)
                            .rhs(ImmutableFalseLiteral.INSTANCE)
                            .build())
                    .build()
            )
        );
    }

    @ParameterizedTest
    @MethodSource("equals")
    void equal(String cypher, Expression.BinaryExpression.Equal expected) throws ParseException {
        var actual = ExpressionParser.parse(cypher, Map.of());
        assertThat(actual).isEqualTo(expected);
    }

    static Stream<Arguments> notEquals() {
        return Stream.of(
            Arguments.of(
                "TRUE <> FALSE",
                ImmutableNotEqual
                    .builder()
                    .lhs(ImmutableTrueLiteral.INSTANCE)
                    .rhs(ImmutableFalseLiteral.INSTANCE)
                    .build()
            ),
            Arguments.of(
                "TRUE != FALSE",
                ImmutableNotEqual
                    .builder()
                    .lhs(ImmutableTrueLiteral.INSTANCE)
                    .rhs(ImmutableFalseLiteral.INSTANCE)
                    .build()
            ),
            Arguments.of(
                "TRUE <> TRUE",
                ImmutableNotEqual
                    .builder()
                    .lhs(ImmutableTrueLiteral.INSTANCE)
                    .rhs(ImmutableTrueLiteral.INSTANCE)
                    .build()
            ),
            Arguments.of(
                "TRUE <> (TRUE <> FALSE)",
                ImmutableNotEqual.builder()
                    .lhs(ImmutableTrueLiteral.INSTANCE)
                    .rhs(ImmutableNotEqual.builder()
                        .lhs(ImmutableTrueLiteral.INSTANCE)
                        .rhs(ImmutableFalseLiteral.INSTANCE)
                        .build()
                    ).build()

            )
        );
    }

    @ParameterizedTest
    @MethodSource("notEquals")
    void notEqual(String cypher, Expression.BinaryExpression.NotEqual expected) throws ParseException {
        var actual = ExpressionParser.parse(cypher, Map.of());
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void greaterThan() throws ParseException {
        var actual = ExpressionParser.parse("1337 > 42", Map.of());
        assertThat(actual).isEqualTo(ImmutableGreaterThan.builder()
            .lhs(ImmutableLongLiteral.of(1337))
            .rhs(ImmutableLongLiteral.of(42))
            .build()
        );
    }

    @Test
    void greaterThanEquals() throws ParseException {
        var actual = ExpressionParser.parse("1337 >= 42", Map.of());
        assertThat(actual).isEqualTo(ImmutableGreaterThanOrEquals.builder()
            .lhs(ImmutableLongLiteral.of(1337))
            .rhs(ImmutableLongLiteral.of(42))
            .build()
        );
    }

    @Test
    void lessThan() throws ParseException {
        var actual = ExpressionParser.parse("1337 < 42", Map.of());

        assertThat(actual).isEqualTo(ImmutableLessThan.builder()
            .lhs(ImmutableLongLiteral.of(1337))
            .rhs(ImmutableLongLiteral.of(42))
            .build()
        );
    }

    @Test
    void lessThanEquals() throws ParseException {
        var actual = ExpressionParser.parse("1337 <= 42", Map.of());
        assertThat(actual).isEqualTo(ImmutableLessThanOrEquals.builder()
            .lhs(ImmutableLongLiteral.of(1337))
            .rhs(ImmutableLongLiteral.of(42))
            .build()
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "foo AND TRUE bar",
        "TRUE FALSE",
        "TRUE, FALSE",
    })
    void shouldThrowOnMultipleExpressions(String input) {
        assertThatThrownBy(() -> ExpressionParser
            .parse(input, Map.of()))
            .isInstanceOf(ParseException.class)
            .hasMessageContaining(formatWithLocale("Expected a single filter expression, got '%s'", input));
    }

    static Stream<Arguments> properties() {
        return Stream.of(
            Arguments.of("n.foo", Map.of("foo", ValueType.LONG), ValueType.LONG),
            Arguments.of("n.foo", Map.of("foo", ValueType.DOUBLE), ValueType.DOUBLE),
            Arguments.of("n.foo", Map.of(), ValueType.UNKNOWN)
        );
    }

    @ParameterizedTest
    @MethodSource("properties")
    void property(String exprString, Map<String, ValueType> properties, ValueType expectedValueType) throws ParseException {
        var expr = ExpressionParser.parse(exprString, properties);

        assertThat(expr).isEqualTo(ImmutableProperty
            .builder()
            .in(ImmutableVariable.builder().name("n").build())
            .propertyKey("foo")
            .valueType(expectedValueType)
            .build());
    }

    static Stream<List<String>> types() {
        return Stream.of(
            List.of(),
            List.of("Foo"),
            List.of("Foo", "Bar")
        );
    }

    @ParameterizedTest
    @MethodSource("types")
    void degree(Collection<String> types) throws ParseException {
        var exprString = StringJoining.join(types.stream().map(type -> "'" + type + "'"), ", ", "degree(", ")");
        var expr = ExpressionParser.parse(exprString, Map.of());
        assertThat(expr).isEqualTo(ImmutableDegree.builder()
            .addAllTypeSelection(types.stream().map(RelationshipType::of).collect(Collectors.toSet()))
            .build());
    }
}
