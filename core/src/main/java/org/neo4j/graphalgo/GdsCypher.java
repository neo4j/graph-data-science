/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo;

import org.immutables.builder.Builder;
import org.immutables.value.Value;
import org.neo4j.cypher.internal.v3_5.ast.prettifier.ExpressionStringifier;
import org.neo4j.cypher.internal.v3_5.ast.prettifier.ExpressionStringifier$;
import org.neo4j.cypher.internal.v3_5.expressions.DecimalDoubleLiteral;
import org.neo4j.cypher.internal.v3_5.expressions.Divide;
import org.neo4j.cypher.internal.v3_5.expressions.Expression;
import org.neo4j.cypher.internal.v3_5.expressions.False;
import org.neo4j.cypher.internal.v3_5.expressions.ListLiteral;
import org.neo4j.cypher.internal.v3_5.expressions.MapExpression;
import org.neo4j.cypher.internal.v3_5.expressions.PropertyKeyName;
import org.neo4j.cypher.internal.v3_5.expressions.SignedDecimalIntegerLiteral;
import org.neo4j.cypher.internal.v3_5.expressions.StringLiteral;
import org.neo4j.cypher.internal.v3_5.expressions.True;
import org.neo4j.cypher.internal.v3_5.util.InputPosition;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import scala.Function1;
import scala.Tuple2;
import scala.collection.mutable.ListBuffer;
import scala.runtime.AbstractFunction1;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;

@Value.Style(builderVisibility = Value.Style.BuilderVisibility.PACKAGE)
public abstract class GdsCypher {

    public static QueryBuilder call(String algorithm) {
        return new StagedBuilder(algorithm);
    }

    public enum ExecutionMode {
        WRITE, STATS, STREAM
    }

    public interface QueryBuilder {
        CreationBuildStage executionMode(ExecutionMode mode);

        CreationBuildStage estimationMode(ExecutionMode mode);

        default CreationBuildStage writeMode() {
            return executionMode(ExecutionMode.WRITE);
        }

        default CreationBuildStage statsMode() {
            return executionMode(ExecutionMode.STATS);
        }

        default CreationBuildStage streamMode() {
            return executionMode(ExecutionMode.STREAM);
        }

        default CreationBuildStage writeEstimation() {
            return estimationMode(ExecutionMode.WRITE);
        }

        default CreationBuildStage statsEstimation() {
            return estimationMode(ExecutionMode.STATS);
        }

        default CreationBuildStage streamEstimation() {
            return estimationMode(ExecutionMode.STREAM);
        }
    }

    public interface CreationBuildStage {
        ParametersBuildStage implicitCreation(GraphCreateConfig config);

        ParametersBuildStage explicitCreation(String graphName);
    }

    public interface ParametersBuildStage {

        ParametersBuildStage addParameter(String key, Object value);

        ParametersBuildStage addParameter(Map.Entry<String, ?> entry);

        ParametersBuildStage addAllParameters(Map<String, ?> entries);

        String yields(String... elements);

        String yields(Iterable<String> elements);
    }

    @Builder.Factory
    static String call(
        @Builder.Parameter String algoName,
        ExecutionMode executionMode,
        boolean runEstimation,
        Optional<GraphCreateConfig> implicitCreateConfig,
        Optional<String> explicitGraphName,
        Map<String, Object> parameters,
        @SuppressWarnings("TypeMayBeWeakened") List<String> yields
    ) {
        StringJoiner argumentsString = new StringJoiner(", ");
        explicitGraphName.ifPresent(name -> argumentsString.add(toCypherString(name)));

        if (implicitCreateConfig.isPresent()) {
            GraphCreateConfig config = implicitCreateConfig.get();
            Map<String, Object> newParameters = new HashMap<>(parameters.size() + 4);
            newParameters.put("nodeProjection", config.nodeProjection().toObject());
            newParameters.put("relationshipProjection", config.relationshipProjection().toObject());
            newParameters.put("nodeProperties", config.nodeProperties().toObject(false));
            newParameters.put("relationshipProperties", config.relationshipProperties().toObject(true));
            newParameters.putAll(parameters);
            parameters = newParameters;
        }

        if (!parameters.isEmpty()) {
            String cypherString = toCypherString(parameters);
            if (!cypherString.isEmpty()) {
                argumentsString.add(cypherString);
            }
        }

        StringJoiner yieldsString = new StringJoiner(", ", " YIELD ", "");
        yieldsString.setEmptyValue("");
        yields.forEach(yieldsString::add);

        return String.format(
            "CALL gds.algo.%s.%s%s(%s)%s",
            algoName,
            executionMode.name().toLowerCase(Locale.ENGLISH),
            runEstimation ? ".estimate" : "",
            argumentsString,
            yieldsString
        );
    }

    private static final Function1<Expression, String> AS_CANONICAL_STRING_FALLBACK =
        new AbstractFunction1<Expression, String>() {
            @Override
            public String apply(Expression v1) {
                return v1.asCanonicalStringVal();
            }
        };

    private static final ExpressionStringifier STRINGIFIER =
        ExpressionStringifier$.MODULE$.apply(AS_CANONICAL_STRING_FALLBACK);

    private static String toCypherString(Object value) {
        return toCypher(value).map(STRINGIFIER::apply).orElse("");
    }

    private static Optional<Expression> toCypher(Object value) {
        if (value instanceof CharSequence) {
            Expression expression = StringLiteral.apply(String.valueOf(value), InputPosition.NONE());
            return Optional.of(expression);
        } else if (value instanceof Number) {
            double v = ((Number) value).doubleValue();
            if (Double.isNaN(v)) {
                return Optional.of(Divide.apply(
                    DecimalDoubleLiteral.apply("0.0", InputPosition.NONE()),
                    DecimalDoubleLiteral.apply("0.0", InputPosition.NONE()),
                    InputPosition.NONE()
                ));
            }
            Expression expression = (long) v == v
                ? SignedDecimalIntegerLiteral.apply(Long.toString((long) v), InputPosition.NONE())
                : DecimalDoubleLiteral.apply(value.toString(), InputPosition.NONE());
            return Optional.of(expression);
        } else if (value instanceof Boolean) {
            Expression expression = Boolean.TRUE.equals(value)
                ? True.apply(InputPosition.NONE())
                : False.apply(InputPosition.NONE());
            return Optional.of(expression);
        } else if (value instanceof Iterable) {
            ListBuffer<Expression> list = new ListBuffer<>();
            ((Iterable<?>) value).forEach(val -> {
                Optional<Expression> expression = toCypher(val);
                expression.ifPresent(list::$plus$eq);
            });
            if (list.nonEmpty()) {
                return Optional.of(ListLiteral.apply(list.toList(), InputPosition.NONE()));
            } else {
                return Optional.empty();
            }
        } else if (value instanceof Map) {
            ListBuffer<Tuple2<PropertyKeyName, Expression>> list = new ListBuffer<>();
            ((Map<?, ?>) value).forEach((key, val) -> {
                Optional<Expression> expression = toCypher(val);
                expression.ifPresent(e -> {
                    PropertyKeyName keyName = PropertyKeyName.apply(String.valueOf(key), InputPosition.NONE());
                    list.$plus$eq(Tuple2.apply(keyName, e));
                });
            });
            if (list.nonEmpty()) {
                return Optional.of(MapExpression.apply(list.toList(), InputPosition.NONE()));
            } else {
                return Optional.empty();
            }
        } else if (value == null) {
            return Optional.empty();
        } else {
            throw new IllegalArgumentException(String.format(
                "Unsupported type [%s] of value [%s]",
                value.getClass().getSimpleName(),
                value
            ));
        }
    }

    private static final class StagedBuilder implements QueryBuilder, CreationBuildStage, ParametersBuildStage {
        private final CallBuilder builder;

        private StagedBuilder(String algoName) {
            builder = new CallBuilder(algoName);
        }

        @Override
        public CreationBuildStage executionMode(ExecutionMode mode) {
            builder.executionMode(mode).runEstimation(false);
            return this;
        }

        @Override
        public CreationBuildStage estimationMode(ExecutionMode mode) {
            builder.executionMode(mode).runEstimation(true);
            return this;
        }

        @Override
        public ParametersBuildStage implicitCreation(GraphCreateConfig config) {
            builder
                .implicitCreateConfig(config)
                .explicitGraphName(Optional.empty());
            return this;
        }

        @Override
        public ParametersBuildStage explicitCreation(String graphName) {
            builder
                .explicitGraphName(graphName)
                .implicitCreateConfig(Optional.empty());
            return this;
        }

        @Override
        public ParametersBuildStage addParameter(String key, Object value) {
            builder.putParameters(key, value);
            return this;
        }

        @Override
        public ParametersBuildStage addParameter(Map.Entry<String, ?> entry) {
            builder.putParameters(entry);
            return this;
        }

        @Override
        public ParametersBuildStage addAllParameters(Map<String, ?> entries) {
            builder.putAllParameters(entries);
            return this;
        }

        @Override
        public String yields(String... elements) {
            return builder.yields(Arrays.asList(elements)).build();
        }

        @Override
        public String yields(Iterable<String> elements) {
            return builder.yields(elements).build();
        }
    }
}
