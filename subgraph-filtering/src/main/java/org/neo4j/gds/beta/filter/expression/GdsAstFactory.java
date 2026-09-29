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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.beta.filter.expression.Expression.BinaryExpression.And;
import org.neo4j.gds.beta.filter.expression.Expression.BinaryExpression.Equal;
import org.neo4j.gds.beta.filter.expression.Expression.BinaryExpression.GreaterThan;
import org.neo4j.gds.beta.filter.expression.Expression.BinaryExpression.GreaterThanOrEquals;
import org.neo4j.gds.beta.filter.expression.Expression.BinaryExpression.LessThan;
import org.neo4j.gds.beta.filter.expression.Expression.BinaryExpression.LessThanOrEquals;
import org.neo4j.gds.beta.filter.expression.Expression.BinaryExpression.NotEqual;
import org.neo4j.gds.beta.filter.expression.Expression.BinaryExpression.Or;
import org.neo4j.gds.beta.filter.expression.Expression.BinaryExpression.Xor;
import org.neo4j.gds.beta.filter.expression.Expression.Function.Degree;
import org.neo4j.gds.beta.filter.expression.Expression.LeafExpression.Variable;
import org.neo4j.gds.beta.filter.expression.Expression.Literal.DoubleLiteral;
import org.neo4j.gds.beta.filter.expression.Expression.Literal.FalseLiteral;
import org.neo4j.gds.beta.filter.expression.Expression.Literal.LongLiteral;
import org.neo4j.gds.beta.filter.expression.Expression.Literal.StringLiteral;
import org.neo4j.gds.beta.filter.expression.Expression.Literal.TrueLiteral;
import org.neo4j.gds.beta.filter.expression.Expression.UnaryExpression.HasNodeLabels;
import org.neo4j.gds.beta.filter.expression.Expression.UnaryExpression.HasRelationshipTypes;
import org.neo4j.gds.beta.filter.expression.Expression.UnaryExpression.NewParameter;
import org.neo4j.gds.beta.filter.expression.Expression.UnaryExpression.Not;
import org.neo4j.gds.beta.filter.expression.Expression.UnaryExpression.Property;
import org.opencypher.v9_0.ast.factory.ASTFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.StringSimilarity.prettySuggestions;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.utils.StringFormatting.toLowerCaseWithLocale;

class GdsAstFactory extends AstFactoryAdapter {

    private static final String LONG_MIN_VALUE_DECIMAL_STRING = Long.toString(Long.MIN_VALUE).substring(1);

    private final Map<String, ValueType> properties;

    GdsAstFactory(Map<String, ValueType> properties) {
        this.properties = properties;
    }

    @Override
    public Variable newVariable(InputPosition p, String name) {
        return new Variable(name);
    }


    @Override
    public Expression newParameter(
        InputPosition p, Variable v
    ) {
        return new NewParameter(v);
    }

    @Override
    public DoubleLiteral newDouble(InputPosition p, String image) {
        return new DoubleLiteral(Double.parseDouble(image));
    }

    @Override
    public LongLiteral newDecimalInteger(InputPosition p, String image, boolean negated) {
        try {
            long value = Long.parseLong(image);
            return new LongLiteral(negated ? -value : value);
        } catch (NumberFormatException e) {
            if (negated && LONG_MIN_VALUE_DECIMAL_STRING.equals(image)) {
                return new LongLiteral(Long.MIN_VALUE);
            } else {
                throw e;
            }
        }
    }

    @Override
    public Expression newTrueLiteral(InputPosition p) {
        return TrueLiteral.INSTANCE;
    }

    @Override
    public Expression newFalseLiteral(InputPosition p) {
        return FalseLiteral.INSTANCE;
    }

    @Override
    public Expression newString(InputPosition p, String image) {
        return new StringLiteral(image);
    }

    @Override
    public Expression listLiteral(InputPosition p, List<Expression> values) {
        return new Expression.Literal.ListLiteral(values);
    }

    @Override
    public Expression hasLabelsOrTypes(Expression subject, List<ASTFactory.StringPos<InputPosition>> labels) {
        if (subject instanceof Variable variable) {
            if (variable.name().equals("n")) {
                var nodeLabels = labels.stream().map(l -> l.string).map(NodeLabel::of).toList();
                return new HasNodeLabels(subject, nodeLabels);
            } else if (variable.name().equals("r")) {
                var relationshipTypes = labels
                    .stream()
                    .map(l -> l.string)
                    .map(RelationshipType::of)
                    .toList();
                return new HasRelationshipTypes(subject, relationshipTypes);
            }
            throw new IllegalArgumentException(formatWithLocale(
                "Invalid variable `%s`. Use `n` for nodes and `r` for relationships.",
                variable.name()
            ));
        }
        throw new UnsupportedOperationException(
            "Label / Type expression can only be combined with variable expressions.");
    }

    @Override
    public Property property(Expression subject, ASTFactory.StringPos<InputPosition> propertyKeyName) {
        var propertyKey = propertyKeyName.string;
        var propertyType = properties.getOrDefault(propertyKey, ValueType.UNKNOWN);

        return new Property(subject, propertyKey, propertyType);
    }

    @Override
    public Expression functionInvocation(
        InputPosition p,
        List<String> namespace,
        String name,
        boolean distinct,
        List<Expression> arguments
    ) {
        if (toLowerCaseWithLocale(name).equals(Degree.NAME)) {
            var relationshipTypes = arguments
                .stream()
                .filter(expr -> {
                    if (expr instanceof StringLiteral) {
                        return true;
                    }
                    throw new IllegalArgumentException(formatWithLocale(
                        "Invalid argument for `%s`. Only strings are allowed. Got `%s`.",
                        Degree.NAME,
                        expr.prettyString()
                    ));

                })
                .map(expr -> (StringLiteral) expr)
                .map(StringLiteral::value)
                .map(RelationshipType::of)
                .collect(Collectors.toSet());

            return new Degree(relationshipTypes);
        }
        throw new UnsupportedOperationException(
            prettySuggestions(
                formatWithLocale("Unknown function `%s`.", name),
                name,
                Set.of(Degree.NAME)
            ));
    }

    @Override
    public Expression or(InputPosition p, Expression lhs, Expression rhs) {
        return new Or(lhs, rhs);
    }

    @Override
    public Expression xor(InputPosition p, Expression lhs, Expression rhs) {
        return new Xor(lhs, rhs);
    }

    @Override
    public Expression and(InputPosition p, Expression lhs, Expression rhs) {
        return new And(lhs, rhs);
    }

    @Override
    public Expression not(Expression e) {
        return new Not(e);
    }

    @Override
    public Expression eq(InputPosition p, Expression lhs, Expression rhs) {
        return new Equal(lhs, rhs);
    }

    @Override
    public Expression neq(InputPosition p, Expression lhs, Expression rhs) {
        return new NotEqual(lhs, rhs);
    }

    @Override
    public Expression neq2(InputPosition p, Expression lhs, Expression rhs) {
        return new NotEqual(lhs, rhs);
    }

    @Override
    public Expression lte(InputPosition p, Expression lhs, Expression rhs) {
        return new LessThanOrEquals(lhs, rhs);
    }

    @Override
    public Expression gte(InputPosition p, Expression lhs, Expression rhs) {
        return new GreaterThanOrEquals(lhs, rhs);
    }

    @Override
    public Expression lt(InputPosition p, Expression lhs, Expression rhs) {
        return new LessThan(lhs, rhs);
    }

    @Override
    public Expression gt(InputPosition p, Expression lhs, Expression rhs) {
        return new GreaterThan(lhs, rhs);
    }

    @Override
    public InputPosition inputPosition(int offset, int line, int column) {
        return new InputPosition(offset, line, column);
    }

    @Override
    public Expression in(InputPosition p, Expression lhs, Expression rhs) {
        return new Expression.BinaryExpression.ListContains(lhs, rhs);
    }
}
