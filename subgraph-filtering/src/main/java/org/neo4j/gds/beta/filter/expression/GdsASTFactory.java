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

import org.neo4j.gds.api.nodeproperties.ValueType;
import org.opencypher.v9_0.ast.factory.ASTFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.StringSimilarity.prettySuggestions;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class GdsASTFactory extends ASTFactoryAdapter {

    private static final String LONG_MIN_VALUE_DECIMAL_STRING = Long.toString(Long.MIN_VALUE).substring(1);

    // TODO: make ValidationContext mutable
    private ValidationContext validationContext;

    GdsASTFactory(ValidationContext validationContext) {
        this.validationContext = validationContext;
    }

    @Override
    public Expression.LeafExpression.Variable newVariable(InputPosition p, String name) {
        return ImmutableVariable.builder().name(name).build();
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
        var labelStrings = labels.stream().map(l -> l.string).collect(Collectors.toList());

        // TODO: move validation into extra method
        String elementType = validationContext.context() == ValidationContext.Context.NODE
            ? "label"
            : "relationship type";

        Set<String> availableLabelsOrTypes = validationContext.availableLabelsOrTypes();

        for (String labelOrType : labelStrings) {
            if (!availableLabelsOrTypes.contains(labelOrType)) {
                validationContext = validationContext.withError(SemanticErrors.SemanticError.of(prettySuggestions(
                    formatWithLocale(
                        "Unknown %s `%s`.",
                        elementType,
                        labelOrType
                    ),
                    labelOrType,
                    availableLabelsOrTypes
                )));
            }
        }

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

        // validation
        var propertyExists = validationContext.availableProperties().contains(propertyKey);

        if (!propertyExists) {
            validationContext = validationContext.withError(SemanticErrors.SemanticError.of(prettySuggestions(
                formatWithLocale(
                    "Unknown property `%s`.",
                    propertyKey
                ),
                propertyKey,
                validationContext.availableProperties()
            )));
        }

        var propertyType = propertyExists
            ? validationContext.availablePropertiesWithTypes().get(propertyKey)
            : ValueType.UNKNOWN;

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

    private void validateTypes(Expression.BinaryExpression.BinaryArithmeticExpression bae) {
        var leftType = bae.lhs().valueType();
        var rightType = bae.rhs().valueType();

        // If one of the types is UNKNOWN, the corresponding property does not exist
        // in the graph store, and we already reported this as an error when parsing
        // the property expression. There is no need to add additional info.
        if (leftType != rightType && leftType != ValueType.UNKNOWN && rightType != ValueType.UNKNOWN) {
            validationContext = validationContext.withError(SemanticErrors.SemanticError.of(
                formatWithLocale(
                    "Incompatible types `%s` and `%s` in binary expression `%s`",
                    leftType,
                    rightType,
                    bae.debugString()
                )));
        }
    }

    @Override
    public Expression eq(InputPosition p, Expression lhs, Expression rhs) {
        var bae = ImmutableEqual.builder().lhs(lhs).rhs(rhs).valueType(lhs.valueType()).build();
        validateTypes(bae);
        return bae;
    }

    @Override
    public Expression neq(InputPosition p, Expression lhs, Expression rhs) {
        var bae = ImmutableNotEqual.builder().lhs(lhs).rhs(rhs).valueType(lhs.valueType()).build();
        validateTypes(bae);
        return bae;
    }

    @Override
    public Expression neq2(InputPosition p, Expression lhs, Expression rhs) {
        var bae = ImmutableNotEqual.builder().lhs(lhs).rhs(rhs).valueType(lhs.valueType()).build();
        validateTypes(bae);
        return bae;
    }

    @Override
    public Expression lte(InputPosition p, Expression lhs, Expression rhs) {
        var bae = ImmutableLessThanOrEquals.builder().lhs(lhs).rhs(rhs).valueType(lhs.valueType()).build();
        validateTypes(bae);
        return bae;
    }

    @Override
    public Expression gte(InputPosition p, Expression lhs, Expression rhs) {
        var bae = ImmutableGreaterThanOrEquals.builder().lhs(lhs).rhs(rhs).valueType(lhs.valueType()).build();
        validateTypes(bae);
        return bae;
    }

    @Override
    public Expression lt(InputPosition p, Expression lhs, Expression rhs) {
        var bae = ImmutableLessThan.builder().lhs(lhs).rhs(rhs).valueType(lhs.valueType()).build();
        validateTypes(bae);
        return bae;
    }

    @Override
    public Expression gt(InputPosition p, Expression lhs, Expression rhs) {
        var bae = ImmutableGreaterThan.builder().lhs(lhs).rhs(rhs).valueType(lhs.valueType()).build();
        validateTypes(bae);
        return bae;
    }

    @Override
    public InputPosition inputPosition(int offset, int line, int column) {
        return ImmutableInputPosition.of(offset, line, column);
    }
}
