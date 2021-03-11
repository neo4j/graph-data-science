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

import java.util.List;

public interface Expression {
    double TRUE = 1.0D;
    double FALSE = 0.0D;
    double EPSILON = 0.000001D;

    double evaluate(EvaluationContext context);

    class Variable implements Expression {
        public String variable;

        Variable(String variable) {
            this.variable = variable;
        }

        @Override
        public double evaluate(EvaluationContext context) {
            return Double.NaN;
        }
    }

    class Property implements Expression {
        public Expression subject;

        public String propertyKey;

        Property(Expression subject, String propertyKey) {
            this.subject = subject;
            this.propertyKey = propertyKey;
        }

        @Override
        public double evaluate(EvaluationContext context) {
            return context.getProperty(propertyKey);
        }
    }

    class HasLabelsOrTypes implements Expression {
        private final Expression subject;
        private final List<String> labelsOrTypes;

        public HasLabelsOrTypes(Expression subject, List<String> labelsOrTypes) {
            this.subject = subject;
            this.labelsOrTypes = labelsOrTypes;
        }

        @Override
        public double evaluate(EvaluationContext context) {
            if (context.hasLabelsOrTypes(labelsOrTypes)) {
                return TRUE;
            } else {
                return FALSE;
            }
        }
    }

    abstract class BinaryExpression implements Expression {

        private final Expression lhs;

        private final Expression rhs;

        BinaryExpression(Expression lhs, Expression rhs) {
            this.lhs = lhs;
            this.rhs = rhs;
        }

        Expression lhs() {
            return lhs;
        }

        Expression rhs() {
            return rhs;
        }

        static class And extends BinaryExpression {
            And(Expression lhs, Expression rhs) {
                super(lhs, rhs);
            }

            @Override
            public double evaluate(EvaluationContext context) {
                var lhsValue = lhs().evaluate(context);
                var rhsValue = rhs().evaluate(context);

                if (lhsValue == Literal.TRUE && rhsValue == Literal.TRUE) {
                    return Literal.TRUE;
                } else {
                    return Literal.FALSE;
                }
            }
        }

        static class Or extends BinaryExpression {
            Or(Expression lhs, Expression rhs) {
                super(lhs, rhs);
            }

            @Override
            public double evaluate(EvaluationContext context) {
                var lhsValue = lhs().evaluate(context);
                var rhsValue = rhs().evaluate(context);

                if (lhsValue == Literal.TRUE || rhsValue == Literal.TRUE) {
                    return Literal.TRUE;
                } else {
                    return Literal.FALSE;
                }
            }
        }

        static class GreaterThan extends BinaryExpression {
            GreaterThan(Expression lhs, Expression rhs) {
                super(lhs, rhs);
            }

            @Override
            public double evaluate(EvaluationContext context) {
                var lhsValue = lhs().evaluate(context);
                var rhsValue = rhs().evaluate(context);

                if ((lhsValue - rhsValue) > EPSILON) {
                    return TRUE;
                } else {
                    return FALSE;
                }
            }
        }

        static class GreaterThanEquals extends BinaryExpression {
            GreaterThanEquals(Expression lhs, Expression rhs) {
                super(lhs, rhs);
            }

            @Override
            public double evaluate(EvaluationContext context) {
                var lhsValue = lhs().evaluate(context);
                var rhsValue = rhs().evaluate(context);

                return lhsValue > rhsValue || Math.abs(lhsValue - rhsValue) < EPSILON ? TRUE : FALSE;
            }
        }

        static class LessThanEquals extends BinaryExpression {
            LessThanEquals(Expression lhs, Expression rhs) {
                super(lhs, rhs);
            }

            @Override
            public double evaluate(EvaluationContext context) {
                var lhsValue = lhs().evaluate(context);
                var rhsValue = rhs().evaluate(context);

                if (rhsValue - lhsValue >= -EPSILON) {
                    return TRUE;
                } else {
                    return FALSE;
                }
            }
        }
    }

    interface Literal extends Expression {

        class LongLiteral implements Literal {
            public long value;

            LongLiteral(long value) {
                this.value = value;
            }

            @Override
            public double evaluate(EvaluationContext context) {
                return ((Long) value).doubleValue();
            }
        }

        class DoubleLiteral implements Literal {
            public double value;

            DoubleLiteral(double value) {
                this.value = value;
            }

            @Override
            public double evaluate(EvaluationContext context) {
                return value;
            }
        }

        class TrueLiteral implements Literal {
            @Override
            public double evaluate(EvaluationContext context) {
                return TRUE;
            }
        }

        class FalseLiteral implements Literal {
            @Override
            public double evaluate(EvaluationContext context) {
                return FALSE;
            }
        }
    }
}


