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


import org.opencypher.v9_0.ast.factory.ASTFactory;

import java.util.List;
import java.util.stream.Collectors;

class GdsASTFactory extends ASTFactoryAdapter {

    private static final String LONG_MIN_VALUE_DECIMAL_STRING = Long.toString(Long.MIN_VALUE).substring(1);

    @Override
    public Expression.Variable newVariable(InputPosition p, String name) {
        return new Expression.Variable(name);
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
    public Expression newTrueLiteral(InputPosition p) {
        return new Expression.Literal.TrueLiteral();
    }

    @Override
    public Expression newFalseLiteral(InputPosition p) {
        return new Expression.Literal.FalseLiteral();
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
    public Expression not(Expression e) {
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
    public InputPosition inputPosition(int offset, int line, int column) {
        return ImmutableInputPosition.of(offset, line, column);
    }
}
