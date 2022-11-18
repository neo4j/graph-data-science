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
import org.opencypher.v9_0.ast.factory.ASTFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class GdsAstFactory extends AstFactoryAdapter {

    private static final String LONG_MIN_VALUE_DECIMAL_STRING = Long.toString(Long.MIN_VALUE).substring(1);

    private final Map<String, ValueType> properties;

    GdsAstFactory(Map<String, ValueType> properties) {
        this.properties = properties;
    }

    @Override
    public Expression.LeafExpression.Variable newVariable(InputPosition p, String name) {
        return ImmutableVariable.builder().name(name).build();
    }


    @Override
    public Expression newParameter(
        InputPosition p, Expression.LeafExpression.Variable v
    ) {
        return ImmutableNewParameter.of(v);
    }

    @Override
    public Expression.Literal.DoubleLiteral newDouble(InputPosition p, String image) {
        return ImmutableDoubleLiteral.builder().value(Double.parseDouble(image)).build();
    }

    @Override
    public Expression.Literal.LongLiteral newDecimalInteger(InputPosition p, String image, boolean negated) {
        try {
            long value = Long.parseLong(image);
            return ImmutableLongLiteral.builder().value(negated ? -value : value).build();
        } catch (NumberFormatException e) {
            if (negated && LONG_MIN_VALUE_DECIMAL_STRING.equals(image)) {
                return ImmutableLongLiteral.builder().value(Long.MIN_VALUE).build();
            } else {
                throw e;
            }
        }
    }

    @Override
    public Expression newTrueLiteral(InputPosition p) {
        return Expression.Literal.TrueLiteral.INSTANCE;
    }

    @Override
    public Expression newFalseLiteral(InputPosition p) {
        return Expression.Literal.FalseLiteral.INSTANCE;
    }

    @Override
    public Expression hasLabelsOrTypes(Expression subject, List<ASTFactory.StringPos<InputPosition>> labels) {
        if (subject instanceof Expression.LeafExpression.Variable) {
            var variable = (Expression.LeafExpression.Variable) subject;
            if (variable.name().equals("n")) {
                var nodeLabels = labels.stream().map(l -> l.string).map(NodeLabel::of).collect(Collectors.toList());
                return ImmutableHasNodeLabels.builder().in(subject).addAllNodeLabels(nodeLabels).build();
            } else if (variable.name().equals("r")) {
                var relationshipTypes = labels.stream().map(l -> l.string).map(RelationshipType::of).collect(Collectors.toList());
                return ImmutableHasRelationshipTypes.builder().in(subject).addAllRelationshipTypes(relationshipTypes).build();
            }
        }
        // Fallback, if the subject can not be identified as node or relationship variable
        var labelStrings = labels.stream().map(l -> l.string).collect(Collectors.toList());

        return ImmutableHasLabelsOrTypes
            .builder()
            .in(subject)
            .addAllLabelsOrTypes(labelStrings)
            .build();
    }

    @Override
    public Expression.UnaryExpression.Property property(
        Expression subject,
        ASTFactory.StringPos<InputPosition> propertyKeyName
    ) {
        var propertyKey = propertyKeyName.string;
        var propertyType = properties.getOrDefault(propertyKey, ValueType.UNKNOWN);

        return ImmutableProperty.builder().in(subject).propertyKey(propertyKey).valueType(propertyType).build();
    }

    @Override
    public Expression or(InputPosition p, Expression lhs, Expression rhs) {
        return ImmutableOr.builder().lhs(lhs).rhs(rhs).build();
    }

    @Override
    public Expression xor(InputPosition p, Expression lhs, Expression rhs) {
        return ImmutableXor.builder().lhs(lhs).rhs(rhs).build();
    }

    @Override
    public Expression and(InputPosition p, Expression lhs, Expression rhs) {
        return ImmutableAnd.builder().lhs(lhs).rhs(rhs).build();
    }

    @Override
    public Expression not(Expression e) {
        return ImmutableNot.builder().in(e).build();
    }

    @Override
    public Expression eq(InputPosition p, Expression lhs, Expression rhs) {
        return ImmutableEqual.builder().lhs(lhs).rhs(rhs).build();
    }

    @Override
    public Expression neq(InputPosition p, Expression lhs, Expression rhs) {
        return ImmutableNotEqual.builder().lhs(lhs).rhs(rhs).build();
    }

    @Override
    public Expression neq2(InputPosition p, Expression lhs, Expression rhs) {
        return ImmutableNotEqual.builder().lhs(lhs).rhs(rhs).build();
    }

    @Override
    public Expression lte(InputPosition p, Expression lhs, Expression rhs) {
        return ImmutableLessThanOrEquals.builder().lhs(lhs).rhs(rhs).build();
    }

    @Override
    public Expression gte(InputPosition p, Expression lhs, Expression rhs) {
        return ImmutableGreaterThanOrEquals.builder().lhs(lhs).rhs(rhs).build();
    }

    @Override
    public Expression lt(InputPosition p, Expression lhs, Expression rhs) {
        return ImmutableLessThan.builder().lhs(lhs).rhs(rhs).build();
    }

    @Override
    public Expression gt(InputPosition p, Expression lhs, Expression rhs) {
        return ImmutableGreaterThan.builder().lhs(lhs).rhs(rhs).build();
    }

    @Override
    public InputPosition inputPosition(int offset, int line, int column) {
        return ImmutableInputPosition.of(offset, line, column);
    }
}
