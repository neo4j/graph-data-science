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

import org.neo4j.cypher.internal.ast.factory.ASTExpressionFactory;
import org.neo4j.cypher.internal.ast.factory.ASTFactory;

import java.util.List;
import java.util.stream.Collectors;

class GDSExpressionFactory implements ASTExpressionFactory<Expression, Object, Expression.Variable, Expression.Property, Object, InputPosition> {

    private static final String LONG_MIN_VALUE_DECIMAL_STRING = Long.toString(Long.MIN_VALUE).substring(1);

    @Override
    public Expression.Variable newVariable(InputPosition p, String name) {
        return new Expression.Variable(name);
    }

    @Override
    public Expression newParameter(InputPosition p, Expression.Variable v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression newParameter(InputPosition p, String offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression oldParameter(InputPosition p, Expression.Variable v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression.Literal.DoubleLiteral newDouble(InputPosition p, String image) {
        return new Expression.Literal.DoubleLiteral(Double.parseDouble(image));
    }

    @Override
    public Expression.Literal.LongLiteral newDecimalInteger(InputPosition p, String image, boolean negated) {
        try {
            long x = Long.parseLong(image);
            return new Expression.Literal.LongLiteral(negated ? -x : x);
        } catch (NumberFormatException e) {
            if (negated && LONG_MIN_VALUE_DECIMAL_STRING.equals(image)) {
                return new Expression.Literal.LongLiteral(Long.MIN_VALUE);
            } else {
                throw e;
            }
        }
    }

    @Override
    public Expression newHexInteger(InputPosition p, String image, boolean negated) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression newOctalInteger(InputPosition p, String image, boolean negated) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression newString(InputPosition p, String image) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression newTrueLiteral(InputPosition p) {
        return new Expression.Literal.TrueLiteral();
    }

    @Override
    public Expression newFalseLiteral(InputPosition p) {
        return new Expression.Literal.FalseLiteral();
    }

    @Override
    public Expression newNullLiteral(InputPosition p) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression listLiteral(InputPosition p, List<Expression> values) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression mapLiteral(
        InputPosition p, List<ASTFactory.StringPos<InputPosition>> keys, List<Expression> values
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression hasLabelsOrTypes(
        Expression subject, List<ASTFactory.StringPos<InputPosition>> labels
    ) {
        if (labels.size() > 1) {
            throw new UnsupportedOperationException("Currently only a single type predicate is supported");
        }

        return new Expression.HasLabelsOrTypes(subject, labels.stream().map(l -> l.string).collect(Collectors.toList()));
    }

    @Override
    public Expression.Property property(Expression subject, ASTFactory.StringPos<InputPosition> propertyKeyName) {
        return new Expression.Property(subject, propertyKeyName.string);
    }

    @Override
    public Expression or(InputPosition p, Expression lhs, Expression rhs) {
        return new Expression.BinaryExpression.Or(lhs, rhs);
    }

    @Override
    public Expression xor(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression and(InputPosition p, Expression lhs, Expression rhs) {
        return new Expression.BinaryExpression.And(lhs, rhs);
    }

    @Override
    public Expression ands(List<Expression> exprs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression not(Expression e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression plus(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression minus(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression multiply(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression divide(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression modulo(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression pow(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression unaryPlus(Expression e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression unaryMinus(Expression e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression eq(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression neq(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression neq2(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression lte(InputPosition p, Expression lhs, Expression rhs) {
        return new Expression.BinaryExpression.LessThanEquals(lhs, rhs);
    }

    @Override
    public Expression gte(InputPosition p, Expression lhs, Expression rhs) {
        return new Expression.BinaryExpression.GreaterThanEquals(lhs, rhs);
    }

    @Override
    public Expression lt(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression gt(InputPosition p, Expression lhs, Expression rhs) {
        return new Expression.BinaryExpression.GreaterThan(lhs, rhs);
    }

    @Override
    public Expression regeq(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression startsWith(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression endsWith(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression contains(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression in(InputPosition p, Expression lhs, Expression rhs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression isNull(Expression e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression listLookup(Expression list, Expression index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression listSlice(InputPosition p, Expression list, Expression start, Expression end) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression newCountStar(InputPosition p) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression functionInvocation(
        InputPosition p, List<String> namespace, String name, boolean distinct, List<Expression> arguments
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression listComprehension(
        InputPosition p, Expression.Variable v, Expression list, Expression where, Expression projection
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression patternComprehension(
        InputPosition p, Expression.Variable v, Object o, Expression where, Expression projection
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression filterExpression(
        InputPosition p, Expression.Variable v, Expression list, Expression where
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression extractExpression(
        InputPosition p, Expression.Variable v, Expression list, Expression where, Expression projection
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression reduceExpression(
        InputPosition p,
        Expression.Variable acc,
        Expression accExpr,
        Expression.Variable v,
        Expression list,
        Expression innerExpr
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression allExpression(
        InputPosition p, Expression.Variable v, Expression list, Expression where
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression anyExpression(
        InputPosition p, Expression.Variable v, Expression list, Expression where
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression noneExpression(
        InputPosition p, Expression.Variable v, Expression list, Expression where
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression singleExpression(
        InputPosition p, Expression.Variable v, Expression list, Expression where
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression patternExpression(InputPosition p, Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression existsSubQuery(InputPosition p, List<Object> objects, Expression where) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression mapProjection(InputPosition p, Expression.Variable v, List<Object> objects) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object mapProjectionLiteralEntry(
        ASTFactory.StringPos<InputPosition> property, Expression value
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object mapProjectionProperty(ASTFactory.StringPos<InputPosition> property) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object mapProjectionVariable(Expression.Variable v) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object mapProjectionAll(InputPosition p) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression caseExpression(
        InputPosition p, Expression e, List<Expression> whens, List<Expression> thens, Expression elze
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputPosition inputPosition(int offset, int line, int column) {
        return ImmutableInputPosition.of(offset, line, column);
    }
}
