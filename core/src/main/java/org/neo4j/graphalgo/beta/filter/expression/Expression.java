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
package org.neo4j.graphalgo.beta.filter.expression;

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.ValueClass;

import java.util.List;
import java.util.Set;

import static org.neo4j.graphalgo.core.StringSimilarity.prettySuggestions;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public interface Expression {
    double TRUE = 1.0D;
    double FALSE = 0.0D;
    double EPSILON = 1E-5;
    double VARIABLE = Double.NaN;

    @Value.Derived
    double evaluate(EvaluationContext context);

    @Value.Derived
    default ValidationContext validate(ValidationContext context) {
        return context;
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
                return context.getProperty(propertyKey());
            }

            @Override
            default ValidationContext validate(ValidationContext context) {
                context = in().validate(context);

                Set<String> availablePropertyKeys = context.availableProperties();

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

                return context;
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

        @ValueClass
        interface Equal extends BinaryExpression {

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                var lhsValue = lhs().evaluate(context);
                var rhsValue = rhs().evaluate(context);

                return Math.abs(lhsValue - rhsValue) < EPSILON ? TRUE : FALSE;
            }
        }

        @ValueClass
        interface NotEqual extends BinaryExpression {

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                var lhsValue = lhs().evaluate(context);
                var rhsValue = rhs().evaluate(context);

                return Math.abs(lhsValue - rhsValue) > EPSILON ? TRUE : FALSE;
            }
        }

        @ValueClass
        interface GreaterThan extends BinaryExpression {
            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                var lhsValue = lhs().evaluate(context);
                var rhsValue = rhs().evaluate(context);

                return (lhsValue - rhsValue) > EPSILON ? TRUE : FALSE;
            }
        }

        @ValueClass
        interface GreaterThanOrEquals extends BinaryExpression {
            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                var lhsValue = lhs().evaluate(context);
                var rhsValue = rhs().evaluate(context);

                return lhsValue > rhsValue || Math.abs(lhsValue - rhsValue) < EPSILON ? TRUE : FALSE;
            }
        }

        @ValueClass
        interface LessThan extends BinaryExpression {

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                var lhsValue = lhs().evaluate(context);
                var rhsValue = rhs().evaluate(context);

                return (rhsValue - lhsValue) > EPSILON ? TRUE : FALSE;
            }
        }

        @ValueClass
        interface LessThanOrEquals extends BinaryExpression {

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                var lhsValue = lhs().evaluate(context);
                var rhsValue = rhs().evaluate(context);

                return lhsValue < rhsValue || (rhsValue - lhsValue) > -EPSILON ? TRUE : FALSE;
            }
        }
    }

    interface Literal extends Expression {

        @ValueClass
        interface LongLiteral extends Literal {
            long value();

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                return ((Long) value()).doubleValue();
            }
        }

        @ValueClass
        interface DoubleLiteral extends Literal {
            double value();

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                return value();
            }
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
}


