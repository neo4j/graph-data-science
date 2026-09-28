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
import org.neo4j.gds.utils.StringJoining;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.StringSimilarity.prettySuggestions;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public interface Expression {
    double TRUE = 1.0D;
    double FALSE = 0.0D;
    double EPSILON = 1E-5;
    double VARIABLE = Double.NaN;

    double evaluate(EvaluationContext context);

    default String prettyString() {
        return toString();
    }

    default ValidationContext validate(ValidationContext context) {
        return context;
    }

    default ValueType valueType() {
        return ValueType.DOUBLE;
    }

    interface LeafExpression extends Expression {

        record Variable(String name) implements LeafExpression {
            @Override
            public double evaluate(EvaluationContext context) {
                return VARIABLE;
            }

            @Override
            public ValidationContext validate(ValidationContext context) {
                if (context.context() == ValidationContext.Context.NODE) {
                    if (!name.equals("n")) {
                        return context.withError(new SemanticErrors.SemanticError(formatWithLocale(
                            "Invalid variable `%s`. Only `n` is allowed for nodes",
                            name
                        )));
                    }
                } else if (context.context() == ValidationContext.Context.RELATIONSHIP) {
                    if (!name.equals("r")) {
                        return context.withError(new SemanticErrors.SemanticError(formatWithLocale(
                            "Invalid variable `%s`. Only `r` is allowed for relationships",
                            name
                        )));
                    }
                }
                return context;
            }

            @Override
            public String prettyString() {
                return name;
            }
        }

    }

    interface UnaryExpression extends Expression {

        Expression in();

        @Override
        default ValidationContext validate(ValidationContext context) {
            return in().validate(context);
        }

        record Property(Expression in, String propertyKey, ValueType valueType) implements UnaryExpression {
            @Override
            public double evaluate(EvaluationContext context) {
                return context.getProperty(propertyKey, valueType);
            }

            @Override
            public ValidationContext validate(ValidationContext context) {
                context = in.validate(context);
                Set<String> availablePropertyKeys = context.availableProperties().keySet();
                if (!availablePropertyKeys.contains(propertyKey())) {
                    return context.withError(new SemanticErrors.SemanticError(prettySuggestions(
                        formatWithLocale("Unknown property `%s`.", propertyKey()),
                        propertyKey(),
                        availablePropertyKeys
                    )));
                }
                var propertyType = context.availableProperties().get(propertyKey());
                return switch (propertyType) {
                    case LONG, DOUBLE, DOUBLE_ARRAY, FLOAT_ARRAY, LONG_ARRAY -> context;
                    default -> context.withError(new SemanticErrors.SemanticError(
                        formatWithLocale(
                            "Unsupported property type `%s` for expression `%s`. Supported types %s",
                            propertyType.name(),
                            prettyString(),
                            StringJoining.join(List.of(ValueType.LONG.name(), ValueType.DOUBLE.name()))
                        )));
                };
            }
            @Override
            public String prettyString() {
                return in.prettyString() + "." + propertyKey();
            }
        }

        record HasNodeLabels(Expression in, List<NodeLabel> nodeLabels) implements UnaryExpression {
            @Override
            public double evaluate(EvaluationContext context) {
                return context.hasNodeLabels(nodeLabels) ? TRUE : FALSE;
            }

            @Override
            public ValidationContext validate(ValidationContext context) {
                context = in.validate(context);
                var availableNodeLabels = context.availableNodeLabels();
                for (var nodeLabel : nodeLabels()) {
                    if (!availableNodeLabels.contains(nodeLabel)) {
                        context = trackMissingLabelOrTypeError(
                            context,
                            nodeLabel.name,
                            availableNodeLabels.stream().map(NodeLabel::name).collect(Collectors.toList())
                        );
                    }
                }
                return context;
            }
        }

        record HasRelationshipTypes(Expression in, List<RelationshipType> relationshipTypes) implements UnaryExpression {
            @Override
            public double evaluate(EvaluationContext context) {
                return context.hasRelationshipTypes(relationshipTypes) ? TRUE : FALSE;
            }

            @Override
            public ValidationContext validate(ValidationContext context) {
                context = in.validate(context);
                var availableRelationshipTypes = context.availableRelationshipTypes();
                for (var relationshipType : relationshipTypes()) {
                    if (!availableRelationshipTypes.contains(relationshipType)) {
                        context = trackMissingLabelOrTypeError(
                            context,
                            relationshipType.name,
                            availableRelationshipTypes.stream().map(RelationshipType::name).toList()
                        );
                    }
                }
                return context;
            }
        }

        record Not(Expression in) implements UnaryExpression {
            @Override
            public double evaluate(EvaluationContext context) {
                return in.evaluate(context) == FALSE ? TRUE : FALSE;
            }
        }

        record NewParameter(LeafExpression.Variable in) implements UnaryExpression {
            @Override
            public ValueType valueType() {
                return ValueType.UNKNOWN;
            }

            @Override
            public double evaluate(EvaluationContext context) {
                var resolvedParameter = context.resolveParameter(in.name());
                if (resolvedParameter instanceof Number numberParameter) {
                    if (numberParameter instanceof Long) {
                        return numberParameter.longValue();
                    }
                    return numberParameter.doubleValue();
                }
                throw new IllegalStateException(formatWithLocale("Expected parameter to be a number, but got `%s`", resolvedParameter.getClass()));
            }

            @Override
            public ValidationContext validate(ValidationContext context) {
                return context;
            }
        }
    }

    interface BinaryExpression extends Expression {

        Expression lhs();

        Expression rhs();

        @Override
        default ValidationContext validate(ValidationContext context) {
            context = lhs().validate(context);
            return rhs().validate(context);
        }

        record And(Expression lhs, Expression rhs) implements BinaryExpression {
            @Override
            public double evaluate(EvaluationContext context) {
                return lhs.evaluate(context) == TRUE && rhs.evaluate(context) == TRUE
                    ? TRUE
                    : FALSE;
            }
        }

        record Or(Expression lhs, Expression rhs) implements BinaryExpression {
            @Override
            public double evaluate(EvaluationContext context) {
                return lhs.evaluate(context) == TRUE || rhs.evaluate(context) == TRUE
                    ? TRUE
                    : FALSE;
            }
        }

        record Xor(Expression lhs, Expression rhs) implements BinaryExpression {
            @Override
            public double evaluate(EvaluationContext context) {
                return lhs.evaluate(context) == TRUE ^ rhs.evaluate(context) == TRUE
                    ? TRUE
                    : FALSE;
            }
        }

        interface BinaryArithmeticExpression extends BinaryExpression {
            @Override
            default double evaluate(EvaluationContext context) {
                var lhsValue = lhs().evaluate(context);
                var rhsValue = rhs().evaluate(context);

                // It is sufficient to check one of the input types
                // as validation made sure that the types are equal.
                if (lhs().valueType() == ValueType.LONG) {
                    long convertedRhsValue = rhs().valueType() == ValueType.UNKNOWN
                        ? (long) rhsValue
                        : Double.doubleToRawLongBits(rhsValue);
                    return evaluateLong(Double.doubleToRawLongBits(lhsValue), convertedRhsValue);
                }

                return evaluateDouble(lhsValue, rhsValue);
            }

            double evaluateLong(long lhsValue, long rhsValue);

            double evaluateDouble(double lhsValue, double rhsValue);

            @Override
            default ValidationContext validate(ValidationContext context) {
                context = lhs().validate(context);
                context = rhs().validate(context);

                var leftType = lhs().valueType();
                var rightType = rhs().valueType();

                // If one of the types is UNKNOWN, the corresponding property does not exist
                // in the graph store, and we already reported this as an error when parsing
                // the property expression. There is no need to add additional info.
                if (leftType != rightType && leftType != ValueType.UNKNOWN && rightType != ValueType.UNKNOWN) {
                    var changeProposal = literalTypeHint(lhs(), rhs())
                        .map(s -> formatWithLocale(" Try changing the literal to `%s`.", s))
                        .orElse("");

                    return context.withError(new SemanticErrors.SemanticError(
                        formatWithLocale(
                            "Incompatible types `%s` and `%s` in binary expression `%s`.%s",
                            leftType,
                            rightType,
                            prettyString(),
                            changeProposal
                        )));
                }

                return context;
            }
        }

        record Equal(Expression lhs, Expression rhs) implements BinaryArithmeticExpression {
            @Override
            public double evaluateLong(long lhsValue, long rhsValue) {
                return lhsValue == rhsValue ? TRUE : FALSE;
            }

            @Override
            public double evaluateDouble(double lhsValue, double rhsValue) {
                return Math.abs(lhsValue - rhsValue) < EPSILON ? TRUE : FALSE;
            }

            @Override
            public String prettyString() {
                return lhs.prettyString() + " = " + rhs.prettyString();
            }
        }

        record NotEqual(Expression lhs, Expression rhs) implements BinaryArithmeticExpression {
            @Override
            public double evaluateLong(long lhsValue, long rhsValue) {
                return lhsValue != rhsValue ? TRUE : FALSE;
            }

            @Override
            public double evaluateDouble(double lhsValue, double rhsValue) {
                return Math.abs(lhsValue - rhsValue) > EPSILON ? TRUE : FALSE;
            }

            @Override
            public String prettyString() {
                return lhs.prettyString() + " <> " + rhs.prettyString();
            }
        }

        record GreaterThan(Expression lhs, Expression rhs) implements BinaryArithmeticExpression {
            @Override
            public double evaluateLong(long lhsValue, long rhsValue) {
                return lhsValue > rhsValue ? TRUE : FALSE;
            }

            @Override
            public double evaluateDouble(double lhsValue, double rhsValue) {
                return (lhsValue - rhsValue) > EPSILON ? TRUE : FALSE;
            }

            @Override
            public String prettyString() {
                return lhs.prettyString() + " > " + rhs.prettyString();
            }
        }

        record GreaterThanOrEquals(Expression lhs, Expression rhs) implements BinaryArithmeticExpression {
            @Override
            public double evaluateLong(long lhsValue, long rhsValue) {
                return lhsValue >= rhsValue ? TRUE : FALSE;
            }

            @Override
            public double evaluateDouble(double lhsValue, double rhsValue) {
                return lhsValue > rhsValue || Math.abs(lhsValue - rhsValue) < EPSILON ? TRUE : FALSE;
            }

            @Override
            public String prettyString() {
                return lhs().prettyString() + " >= " + rhs().prettyString();
            }
        }

        record LessThan(Expression lhs, Expression rhs) implements BinaryArithmeticExpression {
            @Override
            public double evaluateLong(long lhsValue, long rhsValue) {
                return lhsValue < rhsValue ? TRUE : FALSE;
            }

            @Override
            public double evaluateDouble(double lhsValue, double rhsValue) {
                return (rhsValue - lhsValue) > EPSILON ? TRUE : FALSE;
            }

            @Override
            public String prettyString() {
                return lhs.prettyString() + " < " + rhs.prettyString();
            }
        }

        record LessThanOrEquals(Expression lhs, Expression rhs) implements BinaryArithmeticExpression {
            @Override
            public double evaluateLong(long lhsValue, long rhsValue) {
                return lhsValue <= rhsValue ? TRUE : FALSE;
            }

            @Override
            public double evaluateDouble(double lhsValue, double rhsValue) {
                return lhsValue < rhsValue || (rhsValue - lhsValue) > -EPSILON ? TRUE : FALSE;
            }

            @Override
            public String prettyString() {
                return lhs().prettyString() + " <= " + rhs().prettyString();
            }
        }

        record ListContains(Expression lhs, Expression rhs) implements BinaryExpression {
            @Override
            public double evaluate(EvaluationContext context) {
                var searchElement = lhs.evaluate(context);
                return switch (rhs) {
                    case UnaryExpression.Property property -> switch (property.valueType()) {
                        case ValueType.DOUBLE_ARRAY, ValueType.DOUBLE_VECTOR -> contains(
                            context.doubleArrayProperty(property), searchElement);
                        case ValueType.FLOAT_ARRAY, ValueType.FLOAT_VECTOR -> contains(
                            context.floatArrayProperty(property), (float) searchElement);
                        case ValueType.LONG_ARRAY -> contains(
                            context.longArrayProperty(property),
                            Double.doubleToRawLongBits(searchElement)
                        );
                        default -> throw new IllegalStateException("Unexpected valueType: " + property.valueType());
                    };
                    case Literal.ListLiteral listLiteral -> switch (lhs.valueType()) {
                        case ValueType.DOUBLE -> contains(
                            listLiteral.listElements()
                                .stream()
                                .mapToDouble(expr -> expr.evaluate(context))
                                .toArray(),
                            searchElement
                        );
                        case ValueType.LONG -> contains(
                            listLiteral.listElements()
                                .stream()
                                .mapToLong(expr -> Double.doubleToRawLongBits(expr.evaluate(context)))
                                .toArray(),
                            Double.doubleToRawLongBits(searchElement)
                        );
                        default -> throw new IllegalStateException("Unexpected valueType: " + lhs.valueType());
                    };
                    case UnaryExpression.NewParameter parameter -> switch (lhs.valueType()) {
                        case ValueType.DOUBLE -> contains(
                            context.resolveParameter(parameter.in().name(), double[].class),
                            searchElement
                        );
                        case ValueType.LONG -> contains(
                            context.resolveParameter(parameter.in().name(), long[].class),
                            Double.doubleToRawLongBits(searchElement)
                        );
                        default -> throw new IllegalStateException("Unexpected valueType: " + lhs.valueType());
                    };
                    default -> throw new IllegalStateException(formatWithLocale("Expected a list value but got `%s`.", rhs.valueType()));
                };
            }

            @Override
            public ValidationContext validate(ValidationContext context) {
                context = lhs().validate(context);
                context = rhs().validate(context);

                var leftType = lhs().valueType();
                var rightType = rhs().valueType();

                return switch (leftType) {
                    case LONG -> {
                        if (rightType != ValueType.LONG_ARRAY) {
                            yield context.withError(new SemanticErrors.SemanticError(
                                formatWithLocale(
                                    "Expected list type to match scalar element, but list was of type `%s` with element of type `%s`.",
                                    rightType,
                                    leftType
                                )
                            ));
                        }
                        yield context;
                    }
                    case DOUBLE -> {
                        if (rightType != ValueType.DOUBLE_ARRAY) {
                            yield context.withError(new SemanticErrors.SemanticError(
                                formatWithLocale(
                                    "Expected list type to match scalar element, but list was of type `%s` with element of type `%s`.",
                                    rightType,
                                    leftType
                                )
                            ));
                        }
                        yield context;
                    }
                    default -> context.withError(new SemanticErrors.SemanticError(
                        formatWithLocale(
                            "Expected element to be a scalar type, but was `%s` in binary expression `%s`.",
                            leftType,
                            prettyString()
                        )));
                };
            }

            private static double contains(double[] array, double searchElement) {
                for (var element : array) {
                    if (searchElement == element) {
                        return TRUE;
                    }
                }
                return FALSE;
            }

            private static double contains(long[] array, long searchElement) {
                for (var element : array) {
                    if (searchElement == element) {
                        return TRUE;
                    }
                }
                return FALSE;
            }

            private static double contains(float[] array, float searchElement) {
                for (var element : array) {
                    if (searchElement == element) {
                        return TRUE;
                    }
                }
                return FALSE;
            }
        }
    }

    interface Literal extends Expression {

        record LongLiteral(long value) implements Literal {
            @Override
            public ValueType valueType() {
                return ValueType.LONG;
            }

            @Override
            public double evaluate(EvaluationContext context) {
                return Double.longBitsToDouble(value);
            }

            @Override
            public String prettyString() {
                return Long.toString(value);
            }
        }

        record DoubleLiteral(double value) implements Literal {
            @Override
            public ValueType valueType() {
                return ValueType.DOUBLE;
            }

            @Override
            public double evaluate(EvaluationContext context) {
                return value;
            }

            @Override
            public String prettyString() {
                return Double.toString(value);
            }
        }

        record TrueLiteral() implements Literal {
            public static final TrueLiteral INSTANCE = new TrueLiteral();

            @Override
            public double evaluate(EvaluationContext context) {
                return TRUE;
            }
        }

        record FalseLiteral() implements Literal {
            public static final FalseLiteral INSTANCE = new FalseLiteral();

            @Override
            public double evaluate(EvaluationContext context) {
                return FALSE;
            }
        }

        record StringLiteral(String value) implements Literal {
            @Override
            public ValueType valueType() {
                return ValueType.DOUBLE;
            }

            @Override
            public double evaluate(EvaluationContext context) {
                return value().hashCode();
            }

            @Override
            public String prettyString() {
                return value;
            }
        }

        record ListLiteral(List<Expression> listElements) implements Literal {
            @Override
            public ValueType valueType() {
                return ValueType.UNKNOWN;
            }

            @Override
            public double evaluate(EvaluationContext context) {
                throw new UnsupportedOperationException("List literals cannot be evaluated directly. Use a list operation instead.");
            }
        }
    }

    static Optional<String> literalTypeHint(Expression lhs, Expression rhs) {
        var lhsIsLiteral = lhs instanceof Literal;
        var rhsIsLiteral = rhs instanceof Literal;
        if (lhsIsLiteral && rhsIsLiteral || !lhsIsLiteral && !rhsIsLiteral) {
            return Optional.empty();
        }

        var lit = lhs instanceof Literal ? lhs : rhs;
        var nonLit = lhs instanceof Literal ? rhs : lhs;

        var proposedLiteral = Optional.<String>empty();

        if (lit.valueType() == ValueType.DOUBLE && nonLit.valueType() == ValueType.LONG) {
            var doubleLiteral = (Literal.DoubleLiteral) lit;
            proposedLiteral = Optional.of(Long.toString((long) doubleLiteral.value()));
        }
        if (lit.valueType() == ValueType.LONG && nonLit.valueType() == ValueType.DOUBLE) {
            var longLiteral = (Literal.LongLiteral) lit;
            proposedLiteral = Optional.of(Double.toString((double) longLiteral.value()));
        }

        return proposedLiteral;
    }

    static ValidationContext trackMissingLabelOrTypeError(
        ValidationContext context,
        String labelOrType,
        Collection<String> availableLabelsOrTypes
    ) {
        var elementType = context.context() == ValidationContext.Context.NODE
            ? "label"
            : "relationship type";

        return context.withError(new SemanticErrors.SemanticError(prettySuggestions(
            formatWithLocale(
                "Unknown %s `%s`.",
                elementType,
                labelOrType
            ),
            labelOrType,
            availableLabelsOrTypes
        )));
    }

    interface Function extends Expression {

        record Degree(Set<RelationshipType> typeSelection) implements Function {

            public static final String NAME = "degree";

            @Override
            public ValueType valueType() {
                return ValueType.LONG;
            }

            @Override
            public double evaluate(EvaluationContext context) {
                long degree = context.degree(this.typeSelection);
                return Double.longBitsToDouble(degree);
            }
        }
    }

}
