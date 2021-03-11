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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opencypher.v9_0.parser.javacc.ParseException;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class ExpressionParserTest {

    // unary

    static Stream<Arguments> nots() {
        return Stream.of(
            Arguments.of("NOT TRUE", ImmutableNot.of(ImmutableTrueLiteral.INSTANCE)),
            Arguments.of("NOT FALSE", ImmutableNot.of(ImmutableFalseLiteral.INSTANCE)),
            Arguments.of("NOT (TRUE OR FALSE)", ImmutableNot.of(ImmutableOr.of(
                ImmutableTrueLiteral.INSTANCE,
                ImmutableFalseLiteral.INSTANCE
            )))
        );
    }

    @ParameterizedTest
    @MethodSource("nots")
    void nots(String cypher, Expression.UnaryExpression.Not expected) throws ParseException {
        var actual = new ExpressionParser().parse(cypher);
        assertThat(actual).isEqualTo(expected);
    }

    // literal

    @Test
    void trueLiteral() throws ParseException {
        var actual = new ExpressionParser().parse("TRUE");
        assertThat(actual).isEqualTo(Expression.Literal.TrueLiteral.INSTANCE);
    }

    @Test
    void falseLiteral() throws ParseException {
        var actual = new ExpressionParser().parse("FALSE");
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
        var actual = new ExpressionParser().parse(cypher);
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
        var actual = new ExpressionParser().parse(cypher);
        assertThat(actual).isEqualTo(expected);
    }

    // binary

    static Stream<Arguments> ands() {
        return Stream.of(
            Arguments.of(
                "TRUE AND FALSE",
                ImmutableAnd.of(ImmutableTrueLiteral.INSTANCE, ImmutableFalseLiteral.INSTANCE)
            ),
            Arguments.of(
                "TRUE AND TRUE",
                ImmutableAnd.of(ImmutableTrueLiteral.INSTANCE, ImmutableTrueLiteral.INSTANCE)
            ),
            Arguments.of(
                "TRUE AND TRUE AND FALSE",
                ImmutableAnd.of(
                    ImmutableAnd.of(ImmutableTrueLiteral.INSTANCE, ImmutableTrueLiteral.INSTANCE),
                    ImmutableFalseLiteral.INSTANCE
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("ands")
    void and(String cypher, Expression.BinaryExpression.And expected) throws ParseException {
        var actual = new ExpressionParser().parse(cypher);
        assertThat(actual).isEqualTo(expected);
    }

    static Stream<Arguments> ors() {
        return Stream.of(
            Arguments.of(
                "TRUE OR FALSE",
                ImmutableOr.of(ImmutableTrueLiteral.INSTANCE, ImmutableFalseLiteral.INSTANCE)
            ),
            Arguments.of(
                "TRUE OR TRUE",
                ImmutableOr.of(ImmutableTrueLiteral.INSTANCE, ImmutableTrueLiteral.INSTANCE)
            ),
            Arguments.of(
                "TRUE OR TRUE OR FALSE",
                ImmutableOr.of(
                    ImmutableOr.of(ImmutableTrueLiteral.INSTANCE, ImmutableTrueLiteral.INSTANCE),
                    ImmutableFalseLiteral.INSTANCE
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("ors")
    void or(String cypher, Expression.BinaryExpression.Or expected) throws ParseException {
        var actual = new ExpressionParser().parse(cypher);
        assertThat(actual).isEqualTo(expected);
    }

    static Stream<Arguments> xors() {
        return Stream.of(
            Arguments.of(
                "TRUE XOR FALSE",
                ImmutableXor.of(ImmutableTrueLiteral.INSTANCE, ImmutableFalseLiteral.INSTANCE)
            ),
            Arguments.of(
                "TRUE XOR TRUE",
                ImmutableXor.of(ImmutableTrueLiteral.INSTANCE, ImmutableTrueLiteral.INSTANCE)
            ),
            Arguments.of(
                "TRUE XOR TRUE XOR FALSE",
                ImmutableXor.of(
                    ImmutableXor.of(ImmutableTrueLiteral.INSTANCE, ImmutableTrueLiteral.INSTANCE),
                    ImmutableFalseLiteral.INSTANCE
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("xors")
    void xor(String cypher, Expression.BinaryExpression.Xor expected) throws ParseException {
        var actual = new ExpressionParser().parse(cypher);
        assertThat(actual).isEqualTo(expected);
    }

    static Stream<Arguments> equals() {
        return Stream.of(
            Arguments.of(
                "TRUE = FALSE",
                ImmutableEqual.of(ImmutableTrueLiteral.INSTANCE, ImmutableFalseLiteral.INSTANCE)
            ),
            Arguments.of(
                "TRUE = TRUE",
                ImmutableEqual.of(ImmutableTrueLiteral.INSTANCE, ImmutableTrueLiteral.INSTANCE)
            ),
            Arguments.of(
                "TRUE = (TRUE = FALSE)",
                ImmutableEqual.of(
                    ImmutableTrueLiteral.INSTANCE,
                    ImmutableEqual.of(
                        ImmutableTrueLiteral.INSTANCE,
                        ImmutableFalseLiteral.INSTANCE
                    )
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("equals")
    void equal(String cypher, Expression.BinaryExpression.Equal expected) throws ParseException {
        var actual = new ExpressionParser().parse(cypher);
        assertThat(actual).isEqualTo(expected);
    }

    static Stream<Arguments> notEquals() {
        return Stream.of(
            Arguments.of(
                "TRUE <> FALSE",
                ImmutableNotEqual.of(ImmutableTrueLiteral.INSTANCE, ImmutableFalseLiteral.INSTANCE)
            ),
            Arguments.of(
                "TRUE != FALSE",
                ImmutableNotEqual.of(ImmutableTrueLiteral.INSTANCE, ImmutableFalseLiteral.INSTANCE)
            ),
            Arguments.of(
                "TRUE <> TRUE",
                ImmutableNotEqual.of(ImmutableTrueLiteral.INSTANCE, ImmutableTrueLiteral.INSTANCE)
            ),
            Arguments.of(
                "TRUE <> (TRUE <> FALSE)",
                ImmutableNotEqual.of(
                    ImmutableTrueLiteral.INSTANCE,
                    ImmutableNotEqual.of(
                        ImmutableTrueLiteral.INSTANCE,
                        ImmutableFalseLiteral.INSTANCE
                    )
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("notEquals")
    void notEqual(String cypher, Expression.BinaryExpression.NotEqual expected) throws ParseException {
        var actual = new ExpressionParser().parse(cypher);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void greaterThan() throws ParseException {
        var actual = new ExpressionParser().parse("1337 > 42");
        assertThat(actual).isEqualTo(ImmutableGreaterThan.of(
            ImmutableLongLiteral.of(1337),
            ImmutableLongLiteral.of(42)
        ));
    }

    @Test
    void greaterThanEquals() throws ParseException {
        var actual = new ExpressionParser().parse("1337 >= 42");
        assertThat(actual).isEqualTo(ImmutableGreaterThanEquals.of(
            ImmutableLongLiteral.of(1337),
            ImmutableLongLiteral.of(42)
        ));
    }

    @Test
    void lessThan() throws ParseException {
        var actual = new ExpressionParser().parse("1337 < 42");
        assertThat(actual).isEqualTo(ImmutableLessThan.of(
            ImmutableLongLiteral.of(1337),
            ImmutableLongLiteral.of(42)
        ));
    }

    @Test
    void lessThanEquals() throws ParseException {
        var actual = new ExpressionParser().parse("1337 <= 42");
        assertThat(actual).isEqualTo(ImmutableLessThanEquals.of(
            ImmutableLongLiteral.of(1337),
            ImmutableLongLiteral.of(42)
        ));
    }

}