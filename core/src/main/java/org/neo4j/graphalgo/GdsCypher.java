/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.cypher.v3_5.CypherPrinter;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.newapi.ImmutableGraphCreateFromStoreConfig;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Value.Style(builderVisibility = Value.Style.BuilderVisibility.PACKAGE, depluralize = true, deepImmutablesDetection = true)
public abstract class GdsCypher {

    private static final int ADDITIONAL_PARAMETERS_FOR_IMPLICIT_GRAPH_CREATION = 4;
    private static final Pattern PERIOD = Pattern.compile(Pattern.quote("."));
    private static final CypherPrinter PRINTER = new CypherPrinter();

    public static CreationBuildStage call() {
        return new StagedBuilder();
    }

    interface ExecutionMode {
    }

    public enum ExecutionModes implements ExecutionMode {
        WRITE, STATS, STREAM
    }

    @SuppressWarnings("unused")
    public interface ImplicitCreationInlineBuilder {

        default QueryBuilder loadEverything() {
            return loadEverything(Projection.NATURAL);
        }

        default QueryBuilder loadEverything(Projection projection) {
            return this
                .withNodeLabel("*", NodeProjection.empty())
                .withRelationshipType("*", RelationshipProjection.empty().withProjection(projection));
        }

        default ImplicitCreationBuildStage withAnyLabel() {
            return withNodeLabel("*", NodeProjection.empty());
        }

        default ImplicitCreationBuildStage withNodeLabel(String label) {
            return withNodeLabel(label, NodeProjection.builder().label(label).build());
        }

        default ImplicitCreationBuildStage withNodeLabel(String label, String neoLabel) {
            return withRelationshipType(label, RelationshipProjection.builder().type(neoLabel).build());
        }

        ImplicitCreationBuildStage withNodeLabel(String labelKey, NodeProjection nodeProjection);

        default ImplicitCreationBuildStage withAnyRelationshipType() {
            return withRelationshipType("*", RelationshipProjection.empty());
        }

        default ImplicitCreationBuildStage withRelationshipType(String type) {
            return withRelationshipType(type, RelationshipProjection.builder().type(type).build());
        }

        default ImplicitCreationBuildStage withRelationshipType(String type, String neoType) {
            return withRelationshipType(type, RelationshipProjection.builder().type(neoType).build());
        }

        ImplicitCreationBuildStage withRelationshipType(String type, RelationshipProjection relationshipProjection);

        default ImplicitCreationBuildStage withNodeProperty(String nodeProperty) {
            return withNodeProperty(ImmutablePropertyMapping.builder().propertyKey(nodeProperty).build());
        }

        default ImplicitCreationBuildStage withNodeProperty(String propertyKey, String neoPropertyKey) {
            return withNodeProperty(ImmutablePropertyMapping
                .builder()
                .propertyKey(propertyKey)
                .neoPropertyKey(neoPropertyKey)
                .build());
        }

        default ImplicitCreationBuildStage withNodeProperty(String neoPropertyKey, double defaultValue) {
            return withNodeProperty(PropertyMapping.of(neoPropertyKey, defaultValue));
        }

        default ImplicitCreationBuildStage withNodeProperty(
            String propertyKey,
            String neoPropertyKey,
            double defaultValue
        ) {
            return withNodeProperty(PropertyMapping.of(neoPropertyKey, defaultValue));
        }

        default ImplicitCreationBuildStage withNodeProperty(
            String propertyKey,
            double defaultValue,
            DeduplicationStrategy deduplicationStrategy
        ) {
            return withNodeProperty(PropertyMapping.of(propertyKey, defaultValue, deduplicationStrategy));
        }

        default ImplicitCreationBuildStage withNodeProperty(
            String propertyKey,
            DeduplicationStrategy deduplicationStrategy
        ) {
            return withNodeProperty(PropertyMapping.of(propertyKey, deduplicationStrategy));
        }

        default ImplicitCreationBuildStage withNodeProperty(
            String propertyKey,
            String neoPropertyKey,
            double defaultValue,
            DeduplicationStrategy deduplicationStrategy
        ) {
            return withNodeProperty(PropertyMapping.of(
                propertyKey,
                neoPropertyKey,
                defaultValue,
                deduplicationStrategy
            ));
        }

        ImplicitCreationBuildStage withNodeProperty(PropertyMapping propertyMapping);

        default ImplicitCreationBuildStage withRelationshipProperty(String relationshipProperty) {
            return withRelationshipProperty(ImmutablePropertyMapping
                .builder()
                .propertyKey(relationshipProperty)
                .build());
        }

        default ImplicitCreationBuildStage withRelationshipProperty(String propertyKey, String neoPropertyKey) {
            return withRelationshipProperty(ImmutablePropertyMapping
                .builder()
                .propertyKey(propertyKey)
                .neoPropertyKey(neoPropertyKey)
                .build());
        }

        default ImplicitCreationBuildStage withRelationshipProperty(String neoPropertyKey, double defaultValue) {
            return withRelationshipProperty(PropertyMapping.of(neoPropertyKey, defaultValue));
        }

        default ImplicitCreationBuildStage withRelationshipProperty(
            String propertyKey,
            String neoPropertyKey,
            double defaultValue
        ) {
            return withRelationshipProperty(PropertyMapping.of(neoPropertyKey, defaultValue));
        }

        default ImplicitCreationBuildStage withRelationshipProperty(
            String propertyKey,
            double defaultValue,
            DeduplicationStrategy deduplicationStrategy
        ) {
            return withRelationshipProperty(PropertyMapping.of(propertyKey, defaultValue, deduplicationStrategy));
        }

        default ImplicitCreationBuildStage withRelationshipProperty(
            String propertyKey,
            DeduplicationStrategy deduplicationStrategy
        ) {
            return withRelationshipProperty(PropertyMapping.of(propertyKey, deduplicationStrategy));
        }

        default ImplicitCreationBuildStage withRelationshipProperty(
            String propertyKey,
            String neoPropertyKey,
            double defaultValue,
            DeduplicationStrategy deduplicationStrategy
        ) {
            return withRelationshipProperty(PropertyMapping.of(
                propertyKey,
                neoPropertyKey,
                defaultValue,
                deduplicationStrategy
            ));
        }

        ImplicitCreationBuildStage withRelationshipProperty(PropertyMapping propertyMapping);
    }

    public interface CreationBuildStage extends ImplicitCreationInlineBuilder {
        QueryBuilder implicitCreation(GraphCreateConfig config);

        QueryBuilder explicitCreation(String graphName);
    }

    public interface QueryBuilder {
        ParametersBuildStage graphCreate(String graphName);

        ModeBuildStage algo(Iterable<String> namespace, String algoName);

        default ModeBuildStage algo(String algoName) {
            Objects.requireNonNull(algoName, "algoName");
            List<String> nameParts = PERIOD.splitAsStream(algoName).collect(Collectors.toList());
            if (nameParts.isEmpty()) {
                // let builder implementation deal with the empty case
                return algo(Collections.emptyList(), "");
            }
            String actualAlgoName = nameParts.remove(nameParts.size() - 1);
            return algo(nameParts, actualAlgoName);
        }

        default ModeBuildStage algo(String... namespacedAlgoName) {
            Objects.requireNonNull(namespacedAlgoName, "namespacedAlgoName");
            if (namespacedAlgoName.length == 0) {
                // let builder implementation deal with the empty case
                return algo(Collections.emptyList(), "");
            }
            int lastIndex = namespacedAlgoName.length - 1;
            String[] nameParts = Arrays.copyOf(namespacedAlgoName, lastIndex);
            String actualAlgoName = namespacedAlgoName[lastIndex];
            return algo(Arrays.asList(nameParts), actualAlgoName);
        }
    }

    public interface ImplicitCreationBuildStage extends ImplicitCreationInlineBuilder, QueryBuilder {
    }

    public interface ModeBuildStage {

        ParametersBuildStage executionMode(ExecutionMode mode);

        ParametersBuildStage estimationMode(ExecutionMode mode);

        default ParametersBuildStage writeMode() {
            return executionMode(ExecutionModes.WRITE);
        }

        default ParametersBuildStage statsMode() {
            return executionMode(ExecutionModes.STATS);
        }

        default ParametersBuildStage streamMode() {
            return executionMode(ExecutionModes.STREAM);
        }

        default ParametersBuildStage writeEstimation() {
            return estimationMode(ExecutionModes.WRITE);
        }

        default ParametersBuildStage statsEstimation() {
            return estimationMode(ExecutionModes.STATS);
        }

        default ParametersBuildStage streamEstimation() {
            return estimationMode(ExecutionModes.STREAM);
        }
    }

    public interface ParametersBuildStage {

        ParametersBuildStage addParameter(String key, Object value);

        ParametersBuildStage addParameter(Map.Entry<String, ?> entry);

        ParametersBuildStage addPlaceholder(String key, String placeholder);

        ParametersBuildStage addVariable(String key, String variable);

        ParametersBuildStage addAllParameters(Map<String, ?> entries);

        @Language("Cypher")
        String yields(String... elements);

        @Language("Cypher")
        String yields(Iterable<String> elements);
    }

    enum SpecialExecution {
        NORMAL, ESTIMATE
    }

    private enum InternalExecutionMode implements ExecutionMode {
        GRAPH_CREATE
    }

    @Language("Cypher")
    @SuppressWarnings("TypeMayBeWeakened")
    @Builder.Factory
    static String call(
        List<String> algoNamespace,
        String algoName,
        ExecutionMode executionMode,
        @Builder.Switch(defaultName = "NORMAL") SpecialExecution specialExecution,
        Optional<String> explicitGraphName,
        Optional<GraphCreateConfig> implicitCreateConfig,
        Map<String, Object> parameters,
        List<String> yields
    ) {
        String procedureName = procedureName(algoNamespace, algoName, executionMode, specialExecution);
        String queryArguments = queryArguments(explicitGraphName, implicitCreateConfig, executionMode, parameters);
        String yieldsFields = yieldsFields(yields);
        return String.format("CALL %s(%s)%s", procedureName, queryArguments, yieldsFields);
    }

    private static String procedureName(
        Collection<String> algoNamespace,
        CharSequence algoName,
        ExecutionMode executionMode,
        SpecialExecution specialExecution
    ) {
        StringJoiner procedureName = new StringJoiner(".");
        if (algoNamespace.isEmpty()) {
            procedureName.add("gds");
        } else {
            algoNamespace.forEach(procedureName::add);
        }
        procedureName.add(algoName);
        if (executionMode instanceof ExecutionModes) {
            procedureName.add(((ExecutionModes) executionMode).name().toLowerCase(Locale.ENGLISH));
        }
        if (specialExecution == SpecialExecution.ESTIMATE) {
            procedureName.add("estimate");
        }

        return procedureName.toString();
    }

    private static String queryArguments(
        Optional<String> explicitGraphName,
        Optional<GraphCreateConfig> implicitCreateConfig,
        ExecutionMode executionMode,
        Map<String, Object> parameters
    ) {
        StringJoiner queryArguments = new StringJoiner(", ");
        explicitGraphName.ifPresent(name -> queryArguments.add(PRINTER.toCypherString(name)));

        if (implicitCreateConfig.isPresent()) {
            GraphCreateConfig config = implicitCreateConfig.get();
            Map<String, Object> newParameters = new LinkedHashMap<>(
                parameters.size() + ADDITIONAL_PARAMETERS_FOR_IMPLICIT_GRAPH_CREATION
            );

            if (executionMode == InternalExecutionMode.GRAPH_CREATE) {
                queryArguments.add(PRINTER.toCypherString(config.nodeProjection().toObject()));
                queryArguments.add(PRINTER.toCypherString(config.relationshipProjection().toObject()));
            } else {
                newParameters.put("nodeProjection", config.nodeProjection().toObject());
                newParameters.put("relationshipProjection", config.relationshipProjection().toObject());
            }

            newParameters.put("nodeProperties", config.nodeProperties().toObject(false));
            newParameters.put("relationshipProperties", config.relationshipProperties().toObject(true));
            newParameters.putAll(parameters);
            parameters = newParameters;
        }

        if (!parameters.isEmpty()) {
            queryArguments.add(PRINTER.toCypherStringOr(parameters, "{}"));
        }

        return queryArguments.toString();
    }

    private static String yieldsFields(Iterable<String> yields) {
        StringJoiner yieldsFields = new StringJoiner(", ", " YIELD ", "");
        yieldsFields.setEmptyValue("");
        yields.forEach(yieldsFields::add);

        return yieldsFields.toString();
    }

    private static final class StagedBuilder implements CreationBuildStage, ImplicitCreationBuildStage, QueryBuilder, ModeBuildStage, ParametersBuildStage {

        private final CallBuilder builder;
        private @Nullable InlineGraphCreateConfigBuilder graphCreateBuilder;

        private StagedBuilder() {
            builder = new CallBuilder();
        }

        @Override
        public StagedBuilder explicitCreation(String graphName) {
            graphCreateBuilder = null;
            builder
                .explicitGraphName(graphName)
                .implicitCreateConfig(Optional.empty());
            return this;
        }

        @Override
        public StagedBuilder implicitCreation(GraphCreateConfig config) {
            graphCreateBuilder()
                .graphName("")
                .nodeProjections(config.nodeProjection().projections())
                .relProjections(config.relationshipProjection().projections())
                .nodeProperties(config.nodeProperties())
                .relProperties(config.relationshipProperties());
            builder.explicitGraphName(Optional.empty());
            return this;
        }

        @Override
        public StagedBuilder withNodeLabel(
            String labelKey,
            NodeProjection nodeProjection
        ) {
            graphCreateBuilder().putNodeProjection(ElementIdentifier.of(labelKey), nodeProjection);
            return this;
        }

        @Override
        public StagedBuilder withRelationshipType(
            String type,
            RelationshipProjection relationshipProjection
        ) {
            graphCreateBuilder().putRelProjection(ElementIdentifier.of(type), relationshipProjection);
            return this;
        }

        @Override
        public StagedBuilder withNodeProperty(PropertyMapping propertyMapping) {
            graphCreateBuilder().addNodeProperty(propertyMapping);
            return this;
        }

        @Override
        public StagedBuilder withRelationshipProperty(PropertyMapping propertyMapping) {
            graphCreateBuilder().addRelProperty(propertyMapping);
            return this;
        }

        @Override
        public StagedBuilder graphCreate(String graphName) {
            return createGraph(graphName, "gds", "graph");
        }

        private StagedBuilder createGraph(String graphName, String... namespace) {
            graphCreateBuilder().graphName(graphName);
            builder
                .explicitGraphName(Optional.empty())
                .algoNamespace(Arrays.asList(namespace))
                .algoName("create")
                .executionMode(InternalExecutionMode.GRAPH_CREATE);
            return this;
        }

        @Override
        public StagedBuilder algo(Iterable<String> namespace, String algoName) {
            builder
                .algoNamespace(namespace)
                .algoName(algoName);
            return this;
        }

        @Override
        public StagedBuilder executionMode(ExecutionMode mode) {
            builder.executionMode(mode);
            return this;
        }

        @Override
        public StagedBuilder estimationMode(ExecutionMode mode) {
            builder
                .executionMode(mode)
                .estimateSpecialExecution();
            return this;
        }

        @Override
        public StagedBuilder addParameter(String key, Object value) {
            builder.putParameter(key, value);
            return this;
        }

        @Override
        public StagedBuilder addPlaceholder(String key, String placeholder) {
            builder.putParameter(key, PRINTER.parameter(placeholder));
            return this;
        }

        @Override
        public StagedBuilder addVariable(String key, String variable) {
            builder.putParameter(key, PRINTER.variable(variable));
            return this;
        }

        @Override
        public StagedBuilder addParameter(Map.Entry<String, ?> entry) {
            builder.putParameter(entry);
            return this;
        }

        @Override
        public StagedBuilder addAllParameters(Map<String, ?> entries) {
            builder.putAllParameters(entries);
            return this;
        }

        @Language("Cypher")
        @Override
        public String yields(String... elements) {
            builder.yields(Arrays.asList(elements));
            return build();
        }

        @Language("Cypher")
        @Override
        public String yields(Iterable<String> elements) {
            builder.yields(elements);
            return build();
        }

        @Language("Cypher")
        private String build() {
            if (graphCreateBuilder != null) {
                GraphCreateConfig config = graphCreateBuilder.build();
                if (config.graphName().isEmpty()) {
                    builder.explicitGraphName(Optional.empty());
                } else {
                    builder.explicitGraphName(config.graphName());
                }
                builder.implicitCreateConfig(config);
            }
            return builder.build();
        }

        private InlineGraphCreateConfigBuilder graphCreateBuilder() {
            if (graphCreateBuilder == null) {
                graphCreateBuilder = new InlineGraphCreateConfigBuilder();
            }
            return graphCreateBuilder;
        }
    }


    @Builder.Factory
    static GraphCreateConfig inlineGraphCreateConfig(
        Optional<String> graphName,
        Map<ElementIdentifier, NodeProjection> nodeProjections,
        Map<ElementIdentifier, RelationshipProjection> relProjections,
        List<PropertyMapping> nodeProperties,
        List<PropertyMapping> relProperties
    ) {
        return ImmutableGraphCreateFromStoreConfig.builder()
            .graphName(graphName.orElse(""))
            .nodeProjection(NodeProjections.create(nodeProjections))
            .relationshipProjection(RelationshipProjections.builder().putAllProjections(relProjections).build())
            .nodeProperties(PropertyMappings.of(nodeProperties))
            .relationshipProperties(PropertyMappings.of(relProperties))
            .build();
    }
}
