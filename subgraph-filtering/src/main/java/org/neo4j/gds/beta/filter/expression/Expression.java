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

import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.utils.StringJoining;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.neo4j.gds.core.StringSimilarity.prettySuggestions;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

@SuppressWarnings("immutables:subtype")
public interface Expression {
    double TRUE = 1.0D;
    double FALSE = 0.0D;
    double EPSILON = 1E-5;
    double VARIABLE = Double.NaN;

    @Value.Derived
    double evaluate(EvaluationContext context);

    default String prettyString() {
        return toString();
    }

    @Value.Derived
    default ValidationContext validate(ValidationContext context) {
        return context;
    }

    @Value.Default
    default ValueType valueType() {
        return ValueType.DOUBLE;
    }

    interface LeafExpression extends Expression {

        @ValueClass
        interface Variable extends LeafExpression {

            String name();

            @Override
            default double evaluate(EvaluationContext context) {
                return VARIABLE;
            }

            @Override
            default ValidationContext validate(ValidationContext context) {
                if (context.context() == ValidationContext.Context.NODE) {
                    if (!name().equals("n")) {
                        return context.withError(SemanticErrors.SemanticError.of(formatWithLocale(
                            "Invalid variable `%s`. Only `n` is allowed for nodes",
                            name()
                        )));
                    }
                } else if (context.context() == ValidationContext.Context.RELATIONSHIP) {
                    if (!name().equals("r")) {
                        return context.withError(SemanticErrors.SemanticError.of(formatWithLocale(
                            "Invalid variable `%s`. Only `r` is allowed for relationships",
                            name()
                        )));
                    }
                }

                return context;
            }

            @Override
            default String prettyString() {
                return name();
            }
        }

    }

    interface UnaryExpression extends Expression {

        Expression in();

        @Override
        default ValidationContext validate(ValidationContext context) {
            return in().validate(context);
        }

        @ValueClass
        interface Property extends UnaryExpression {

            String propertyKey();

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                return context.getProperty(propertyKey(), valueType());
            }

            @Override
            default ValidationContext validate(ValidationContext context) {
                context = in().validate(context);

                Set<String> availablePropertyKeys = context.availableProperties().keySet();

                if (!availablePropertyKeys.contains(propertyKey())) {
                    return context.withError(SemanticErrors.SemanticError.of(prettySuggestions(
                        formatWithLocale(
                            "Unknown property `%s`.",
                            propertyKey()
                        ),
                        propertyKey(),
                        availablePropertyKeys
                    )));
                }
                var propertyType = context.availableProperties().get(propertyKey());
                if (propertyType != ValueType.LONG && propertyType != ValueType.DOUBLE) {
                    return context.withError(SemanticErrors.SemanticError.of(
                        formatWithLocale(
                            "Unsupported property type `%s` for expression `%s`. Supported types %s",
                            propertyType.name(),
                            prettyString(),
                            StringJoining.join(List.of(ValueType.LONG.name(), ValueType.DOUBLE.name()))
                        )));
                }
                return context;
            }

            @Override
            default String prettyString() {
                return in().prettyString() + "." + propertyKey();
            }
        }

        @ValueClass
        interface HasLabelsOrTypes extends UnaryExpression {
            List<String> labelsOrTypes();

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                return context.hasLabelsOrTypes(labelsOrTypes()) ? TRUE : FALSE;
            }

            @Override
            default ValidationContext validate(ValidationContext context) {
                context = in().validate(context);

                Set<String> availableLabelsOrTypes = context.availableLabelsOrTypes();
                String elementType = context.context() == ValidationContext.Context.NODE
                    ? "label"
                    : "relationship type";

                for (String labelOrType : labelsOrTypes()) {
                    if (!availableLabelsOrTypes.contains(labelOrType)) {
                        context = context.withError(SemanticErrors.SemanticError.of(prettySuggestions(
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

                return context;
            }
        }

        @ValueClass
        interface Not extends UnaryExpression {

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                return in().evaluate(context) == TRUE ? FALSE : TRUE;
            }

        }

        @ValueClass
        interface NewParameter extends UnaryExpression {

            @Override
            LeafExpression.Variable in();

            @Override
            default ValueType valueType() {
                return ValueType.UNKNOWN;
            }

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                var resolvedParameter = context.resolveParameter(in().name());
                if (resolvedParameter instanceof Long) {
                    return resolvedParameter.longValue();
                }
                return resolvedParameter.doubleValue();
            }

            @Override
            default ValidationContext validate(ValidationContext context) {
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

        @ValueClass
        interface And extends BinaryExpression {
            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                return lhs().evaluate(context) == TRUE && rhs().evaluate(context) == TRUE
                    ? TRUE
                    : FALSE;
            }

        }

        @ValueClass
        interface Or extends BinaryExpression {

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                return lhs().evaluate(context) == TRUE || rhs().evaluate(context) == TRUE
                    ? TRUE
                    : FALSE;
            }

        }

        @ValueClass
        interface Xor extends BinaryExpression {

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                return lhs().evaluate(context) == TRUE ^ rhs().evaluate(context) == TRUE
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

                    return context.withError(SemanticErrors.SemanticError.of(
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

        @ValueClass
        interface Equal extends BinaryArithmeticExpression {

            @Override
            default double evaluateLong(long lhsValue, long rhsValue) {
                return lhsValue == rhsValue ? TRUE : FALSE;
            }

            @Override
            default double evaluateDouble(double lhsValue, double rhsValue) {
                return Math.abs(lhsValue - rhsValue) < EPSILON ? TRUE : FALSE;
            }

            @Override
            default String prettyString() {
                return lhs().prettyString() + " = " + rhs().prettyString();
            }
        }

        @ValueClass
        interface NotEqual extends BinaryArithmeticExpression {

            @Override
            default double evaluateLong(long lhsValue, long rhsValue) {
                return lhsValue != rhsValue ? TRUE : FALSE;
            }

            @Override
            default double evaluateDouble(double lhsValue, double rhsValue) {
                return Math.abs(lhsValue - rhsValue) > EPSILON ? TRUE : FALSE;
            }

            @Override
            default String prettyString() {
                return lhs().prettyString() + " <> " + rhs().prettyString();
            }
        }

        @ValueClass
        interface GreaterThan extends BinaryArithmeticExpression {

            @Override
            default double evaluateLong(long lhsValue, long rhsValue) {
                return lhsValue > rhsValue ? TRUE : FALSE;
            }

            @Override
            default double evaluateDouble(double lhsValue, double rhsValue) {
                return (lhsValue - rhsValue) > EPSILON ? TRUE : FALSE;
            }

            @Override
            default String prettyString() {
                return lhs().prettyString() + " > " + rhs().prettyString();
            }
        }

        @ValueClass
        interface GreaterThanOrEquals extends BinaryArithmeticExpression {

            @Override
            default double evaluateLong(long lhsValue, long rhsValue) {
                return lhsValue >= rhsValue ? TRUE : FALSE;
            }

            @Override
            default double evaluateDouble(double lhsValue, double rhsValue) {
                return lhsValue > rhsValue || Math.abs(lhsValue - rhsValue) < EPSILON ? TRUE : FALSE;
            }

            @Override
            default String prettyString() {
                return lhs().prettyString() + " >= " + rhs().prettyString();
            }
        }

        @ValueClass
        interface LessThan extends BinaryArithmeticExpression {

            @Value.Derived
            @Override
            default double evaluateLong(long lhsValue, long rhsValue) {
                return lhsValue < rhsValue ? TRUE : FALSE;
            }

            @Value.Derived
            @Override
            default double evaluateDouble(double lhsValue, double rhsValue) {
                return (rhsValue - lhsValue) > EPSILON ? TRUE : FALSE;
            }

            @Override
            default String prettyString() {
                return lhs().prettyString() + " < " + rhs().prettyString();
            }
        }

        @ValueClass
        interface LessThanOrEquals extends BinaryArithmeticExpression {

            @Override
            default double evaluateLong(long lhsValue, long rhsValue) {
                return lhsValue <= rhsValue ? TRUE : FALSE;
            }

            @Override
            default double evaluateDouble(double lhsValue, double rhsValue) {
                return lhsValue < rhsValue || (rhsValue - lhsValue) > -EPSILON ? TRUE : FALSE;
            }

            @Override
            default String prettyString() {
                return lhs().prettyString() + " <= " + rhs().prettyString();
            }
        }
    }

    interface Literal extends Expression {
        @ValueClass
        interface LongLiteral extends Literal {
            long value();

            @Override
            default ValueType valueType() {
                return ValueType.LONG;
            }

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                return Double.longBitsToDouble(value());
            }

            @Override
            default String prettyString() {return Long.toString(value());}
        }

        @ValueClass
        interface DoubleLiteral extends Literal {
            double value();

            @Override
            default ValueType valueType() {
                return ValueType.DOUBLE;
            }

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                return value();
            }

            default String prettyString() {return Double.toString(value());}

        }

        @ValueClass
        interface TrueLiteral extends Literal {

            TrueLiteral INSTANCE = ImmutableTrueLiteral.builder().build();

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                return TRUE;
            }
        }

        @ValueClass
        interface FalseLiteral extends Literal {

            FalseLiteral INSTANCE = ImmutableFalseLiteral.builder().build();

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                return FALSE;
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

}
