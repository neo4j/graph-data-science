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

import org.immutables.value.Value;
import org.neo4j.graphalgo.annotation.ValueClass;

import java.util.List;

public interface Expression {
    double TRUE = 1.0D;
    double FALSE = 0.0D;
    double EPSILON = 0.000001D;
    double VARIABLE = Double.NaN;

    double evaluate(EvaluationContext context);

    interface LeafExpression extends Expression {

        @ValueClass
        interface Variable extends LeafExpression {

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                return VARIABLE;
            }
        }


        @ValueClass
        interface Property extends LeafExpression {

            String propertyKey();

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                return context.getProperty(propertyKey());
            }
        }

        @ValueClass
        interface HasLabelsOrTypes extends LeafExpression {
            List<String> labelsOrTypes();

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                return context.hasLabelsOrTypes(labelsOrTypes()) ? TRUE : FALSE;
            }
        }
    }

    interface UnaryExpression extends Expression {

        Expression in();

        @ValueClass
        interface Not extends UnaryExpression {

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                var inValue = in().evaluate(context) == TRUE;

                return inValue ? FALSE : TRUE;
            }
        }
    }

    interface BinaryExpression extends Expression {

        Expression lhs();

        Expression rhs();

        @ValueClass
        interface And extends BinaryExpression {

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                var lhsValue = lhs().evaluate(context) == TRUE;
                var rhsValue = rhs().evaluate(context) == TRUE;

                return lhsValue && rhsValue ? TRUE : FALSE;
            }
        }

        @ValueClass
        interface Or extends BinaryExpression {

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                var lhsValue = lhs().evaluate(context) == TRUE;
                var rhsValue = rhs().evaluate(context) == TRUE;

                return lhsValue || rhsValue ? TRUE : FALSE;
            }
        }

        @ValueClass
        interface Xor extends BinaryExpression {

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                var lhsValue = lhs().evaluate(context) == TRUE;
                var rhsValue = rhs().evaluate(context) == TRUE;

                return lhsValue ^ rhsValue ? TRUE : FALSE;
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
        interface GreaterThanEquals extends BinaryExpression {
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
        interface LessThanEquals extends BinaryExpression {

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                var lhsValue = lhs().evaluate(context);
                var rhsValue = rhs().evaluate(context);

                return lhsValue < rhsValue || (rhsValue - lhsValue) > EPSILON ? TRUE : FALSE;
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

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                return TRUE;
            }
        }

        @ValueClass
        interface FalseLiteral extends Literal {

            @Value.Derived
            @Override
            default double evaluate(EvaluationContext context) {
                return FALSE;
            }
        }
    }
}


