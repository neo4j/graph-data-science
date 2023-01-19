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
package org.neo4j.gds;

import org.immutables.builder.Builder;
import org.immutables.value.Value;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.cypherdsl.core.Cypher;
import org.neo4j.cypherdsl.core.Expression;
import org.neo4j.cypherdsl.core.SymbolicName;
import org.neo4j.cypherdsl.core.renderer.Configuration;
import org.neo4j.cypherdsl.core.renderer.Renderer;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
import org.neo4j.gds.core.Aggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.neo4j.gds.ElementProjection.PROJECT_ALL;
import static org.neo4j.gds.ElementProjection.PROPERTIES_KEY;
import static org.neo4j.gds.NodeLabel.ALL_NODES;
import static org.neo4j.gds.Orientation.NATURAL;
import static org.neo4j.gds.PropertyMapping.DEFAULT_VALUE_KEY;
import static org.neo4j.gds.PropertyMapping.PROPERTY_KEY;
import static org.neo4j.gds.RelationshipProjection.AGGREGATION_KEY;
import static org.neo4j.gds.RelationshipProjection.INDEX_INVERSE_KEY;
import static org.neo4j.gds.RelationshipProjection.ORIENTATION_KEY;
import static org.neo4j.gds.RelationshipProjection.TYPE_KEY;
import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.gds.core.Aggregation.DEFAULT;

@Value.Style(builderVisibility = Value.Style.BuilderVisibility.PACKAGE, depluralize = true, deepImmutablesDetection = true)
public abstract class GdsCypher {

    private static final Pattern PERIOD = Pattern.compile(Pattern.quote("."));

    public static QueryBuilder call(String graphName) {
        return new QueryBuilder(graphName);
    }

    interface ExecutionMode {
    }

    public enum ExecutionModes implements ExecutionMode {
        WRITE, STATS, STREAM, MUTATE, TRAIN
    }


    public static final class QueryBuilder {
        private final String graphName;

        private QueryBuilder(String graphName) {
            this.graphName = graphName;
        }

        public GraphProjectBuilder graphProject() {
            return new GraphProjectBuilder(graphName);
        }

        public ModeBuildStage algo(Iterable<String> namespace, String algoName) {
            return new AlgoBuilder(graphName, namespace, algoName);
        }

        public ModeBuildStage algo(String algoName) {
            Objects.requireNonNull(algoName, "algoName");
            List<String> nameParts = PERIOD.splitAsStream(algoName).collect(Collectors.toList());
            if (nameParts.isEmpty()) {
                // let builder implementation deal with the empty case
                return algo(Collections.emptyList(), "");
            }
            String actualAlgoName = nameParts.remove(nameParts.size() - 1);
            return algo(nameParts, actualAlgoName);
        }

        public ModeBuildStage algo(String... namespacedAlgoName) {
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

    public interface ModeBuildStage {

        ParametersBuildStage executionMode(ExecutionMode mode);

        ParametersBuildStage estimationMode(ExecutionMode mode);

        default ParametersBuildStage writeMode() {
            return executionMode(ExecutionModes.WRITE);
        }

        default ParametersBuildStage mutateMode() {
            return executionMode(ExecutionModes.MUTATE);
        }

        default ParametersBuildStage statsMode() {
            return executionMode(ExecutionModes.STATS);
        }

        default ParametersBuildStage streamMode() {
            return executionMode(ExecutionModes.STREAM);
        }

        default ParametersBuildStage trainMode() {
            return executionMode(ExecutionModes.TRAIN);
        }

        default ParametersBuildStage writeEstimation() {
            return estimationMode(ExecutionModes.WRITE);
        }

        default ParametersBuildStage mutateEstimation() {
            return estimationMode(ExecutionModes.MUTATE);
        }

        default ParametersBuildStage statsEstimation() {
            return estimationMode(ExecutionModes.STATS);
        }

        default ParametersBuildStage streamEstimation() {
            return estimationMode(ExecutionModes.STREAM);
        }

        default ParametersBuildStage trainEstimation() {
            return estimationMode(ExecutionModes.TRAIN);
        }
    }

    public interface ParametersBuildStage {

        ParametersBuildStage addParameter(String key, Object value);

        ParametersBuildStage addParameter(Map.Entry<String, ?> entry);

        ParametersBuildStage addPlaceholder(String key, String placeholder);

        ParametersBuildStage addVariable(String key, String variable);

        ParametersBuildStage addAllParameters(Map<String, ?> entries);

        @Language("Cypher")
        default String yields(String... elements) {
            return yields(Arrays.asList(elements));
        }

        @Language("Cypher")
        String yields(Iterable<String> elements);
    }

    enum SpecialExecution {
        NORMAL, ESTIMATE
    }

    @Language("Cypher")
    @SuppressWarnings("TypeMayBeWeakened")
    @Builder.Factory
    static String call(
        List<String> algoNamespace,
        String algoName,
        ExecutionMode executionMode,
        @Builder.Switch(defaultName = "NORMAL") SpecialExecution specialExecution,
        String graphName,
        Map<String, Object> parameters,
        List<String> yields
    ) {
        var procedureName = procedureName(algoNamespace, algoName, executionMode, specialExecution);
        var queryArguments = queryArguments(graphName, parameters, specialExecution == SpecialExecution.ESTIMATE);
        var yieldsFields = yieldsFields(yields);

        var query = Cypher.call(procedureName).withArgs(queryArguments);
        var statement = yieldsFields.map(fields -> query.yield(fields).build()).orElseGet(query::build);

        var cypherRenderer = Renderer.getRenderer(Configuration.defaultConfig());
        return cypherRenderer.render(statement);
    }

    private static String[] procedureName(
        Collection<String> algoNamespace,
        String algoName,
        ExecutionMode executionMode,
        SpecialExecution specialExecution
    ) {
        var procedureName = new ArrayList<String>(algoNamespace.size());
        if (algoNamespace.isEmpty()) {
            procedureName.add("gds");
        } else {
            procedureName.addAll(algoNamespace);
        }
        procedureName.add(algoName);
        if (executionMode instanceof ExecutionModes) {
            procedureName.add(((ExecutionModes) executionMode).name().toLowerCase(Locale.ENGLISH));
        }
        if (specialExecution == SpecialExecution.ESTIMATE) {
            procedureName.add("estimate");
        }

        return procedureName.toArray(new String[0]);
    }

    private static Expression[] queryArguments(
        String graphName,
        GraphProjectFromStoreConfig config,
        Map<String, Object> parameters
    ) {
        var queryArguments = new ArrayList<Expression>();
        queryArguments.add(Cypher.literalOf(graphName));

        Map<String, Object> newParameters = new LinkedHashMap<>(parameters.size());

        Optional<Object> nodeProjection = toMinimalObject(config.nodeProjections()).toObject();
        Optional<Object> relationshipProjection = toMinimalObject(config.relationshipProjections()).toObject();
        queryArguments.add(nodeProjection.map(GdsCypher::toExpression).orElseGet(() -> Cypher.literalOf("")));
        queryArguments.add(relationshipProjection.map(GdsCypher::toExpression).orElseGet(() -> Cypher.literalOf("")));

        toMinimalObject(config.nodeProperties(), false)
            .toObject()
            .ifPresent(np -> newParameters.put("nodeProperties", np));
        toMinimalObject(config.relationshipProperties(), false)
            .toObject()
            .ifPresent(rp -> newParameters.put("relationshipProperties", rp));

        newParameters.putAll(parameters);
        parameters = newParameters;

        if (!parameters.isEmpty()) {
            queryArguments.add(Objects.requireNonNullElseGet(toExpression(parameters), Cypher::mapOf));
        }

        return queryArguments.toArray(new Expression[0]);
    }

    private static Expression[] queryArguments(
        String graphName,
        Map<String, Object> parameters,
        boolean isEstimationMode
    ) {
        var queryArguments = new ArrayList<Expression>();
        queryArguments.add(Cypher.literalOf(graphName));

        if (!parameters.isEmpty() || isEstimationMode) {
            queryArguments.add(Objects.requireNonNullElseGet(toExpression(parameters), Cypher::mapOf));
        }

        return queryArguments.toArray(new Expression[0]);
    }

    private static final Pattern COMMA = Pattern.compile(",");

    private static Optional<SymbolicName[]> yieldsFields(Collection<String> yields) {
        if (yields.isEmpty()) {
            return Optional.empty();
        }

        var yieldNames = yields.stream()
            .flatMap(COMMA::splitAsStream)
            .map(String::trim)
            .map(GdsCypher::name)
            .toArray(SymbolicName[]::new);
        return Optional.of(yieldNames);
    }

    private static SymbolicName name(String name) {
        try {
            return Cypher.name(name);
        } catch (IllegalArgumentException e) {
            var message = String.format(
                Locale.ENGLISH,
                "`%s` is not a valid Cypher name: %s",
                name,
                e.getMessage()
            );
            throw new IllegalArgumentException(message, e);
        }
    }


    public static final class GraphProjectBuilder {
        private final String graphName;
        private final InlineGraphProjectConfigBuilder graphProjectConfigBuilder;
        private final Map<String, Object> parameters;

        private GraphProjectBuilder(String graphName) {
            this.graphName = graphName;
            graphProjectConfigBuilder = new InlineGraphProjectConfigBuilder().graphName(graphName);
            parameters = new LinkedHashMap<>();
        }

        public GraphProjectBuilder withGraphProjectConfig(GraphProjectFromStoreConfig config) {
            graphProjectConfigBuilder
                .graphName(config.graphName())
                .nodeProjections(config.nodeProjections().projections())
                .relProjections(config.relationshipProjections().projections())
                .nodeProperties(config.nodeProperties())
                .relProperties(config.relationshipProperties());

            return this;
        }

        /**
         * Loads all nodes of any label and relationships of any type in the
         * {@link org.neo4j.gds.Orientation#NATURAL} projection.
         *
         * Does <strong>not</strong> load any properties.
         *
         * To load properties, call one of {@link #withNodeProperty(String)}
         * or {@link #withRelationshipProperty(String)} or their variants.
         */
        public GraphProjectBuilder loadEverything() {
            return loadEverything(NATURAL);
        }

        /**
         * Loads all nodes of any label and relationships of any type in the
         * given {@code projection}.
         *
         * Does <strong>not</strong> load any properties.
         *
         * To load properties, call one of {@link #withNodeProperty(String)}
         * or {@link #withRelationshipProperty(String)} or their variants.
         */
        public GraphProjectBuilder loadEverything(Orientation orientation) {
            return this
                .withNodeLabel(ALL_NODES.name, NodeProjection.all())
                .withRelationshipType(
                    ALL_RELATIONSHIPS.name(),
                    RelationshipProjection.builder().from(RelationshipProjection.ALL).orientation(orientation).build()
                );
        }

        public GraphProjectBuilder withAnyLabel() {
            return withNodeLabel(ALL_NODES.name, NodeProjection.all());
        }

        public GraphProjectBuilder withNodeLabel(String label) {
            return withNodeLabels(label);
        }

        public GraphProjectBuilder withNodeLabel(String label, NodeProjection nodeProjection) {
            return withNodeLabels(Map.of(label, nodeProjection));
        }

        public GraphProjectBuilder withNodeLabels(String... labels) {
            return withNodeLabels(Arrays.stream(labels).collect(Collectors.toMap(
                Function.identity(),
                label -> NodeProjection.builder().label(label).build()
            )));
        }

        public GraphProjectBuilder withNodeLabels(Map<String, NodeProjection> nodeProjections) {
            nodeProjections.forEach((label, nodeProjection) -> graphProjectConfigBuilder.putNodeProjection(
                NodeLabel.of(label),
                nodeProjection
            ));
            return this;
        }

        public GraphProjectBuilder withAnyRelationshipType() {
            return withRelationshipType(ALL_RELATIONSHIPS.name(), RelationshipProjection.ALL);
        }

        public GraphProjectBuilder withRelationshipType(String type) {
            return withRelationshipType(type, RelationshipProjection.builder().type(type).build());
        }

        public GraphProjectBuilder withRelationshipType(String type, Orientation orientation) {
            return withRelationshipType(
                type,
                RelationshipProjection.builder().type(type).orientation(orientation).build()
            );
        }

        public GraphProjectBuilder withRelationshipType(String type, String neoType) {
            return withRelationshipType(type, RelationshipProjection.builder().type(neoType).build());
        }

        public GraphProjectBuilder withRelationshipType(
            String type,
            RelationshipProjection relationshipProjection
        ) {
            graphProjectConfigBuilder.putRelProjection(RelationshipType.of(type), relationshipProjection);
            return this;
        }

        public GraphProjectBuilder withNodeProperty(String nodeProperty) {
            return withNodeProperty(ImmutablePropertyMapping.builder().propertyKey(nodeProperty).build());
        }

        public GraphProjectBuilder withNodeProperty(String propertyKey, String neoPropertyKey) {
            return withNodeProperty(ImmutablePropertyMapping
                .builder()
                .propertyKey(propertyKey)
                .neoPropertyKey(neoPropertyKey)
                .build());
        }

        public GraphProjectBuilder withNodeProperty(String neoPropertyKey, DefaultValue defaultValue) {
            return withNodeProperty(PropertyMapping.of(neoPropertyKey, defaultValue));
        }

        public GraphProjectBuilder withNodeProperty(
            String propertyKey,
            String neoPropertyKey,
            DefaultValue defaultValue
        ) {
            return withNodeProperty(PropertyMapping.of(propertyKey, neoPropertyKey, defaultValue));
        }

        public GraphProjectBuilder withNodeProperty(
            String propertyKey,
            DefaultValue defaultValue,
            Aggregation aggregation
        ) {
            return withNodeProperty(PropertyMapping.of(propertyKey, defaultValue, aggregation));
        }

        public GraphProjectBuilder withNodeProperty(
            String propertyKey,
            Aggregation aggregation
        ) {
            return withNodeProperty(PropertyMapping.of(propertyKey, aggregation));
        }

        public GraphProjectBuilder withNodeProperty(
            String propertyKey,
            String neoPropertyKey,
            DefaultValue defaultValue,
            Aggregation aggregation
        ) {
            return withNodeProperty(PropertyMapping.of(
                propertyKey,
                neoPropertyKey,
                defaultValue,
                aggregation
            ));
        }

        public GraphProjectBuilder withNodeProperty(PropertyMapping propertyMapping) {
            graphProjectConfigBuilder.addNodeProperty(propertyMapping);
            return this;
        }

        public GraphProjectBuilder withNodeProperties(Iterable<String> properties, DefaultValue defaultValue) {
            properties.forEach(property -> withNodeProperty(property, defaultValue));
            return this;
        }

        public GraphProjectBuilder withRelationshipProperty(String relationshipProperty) {
            return withRelationshipProperty(ImmutablePropertyMapping
                .builder()
                .propertyKey(relationshipProperty)
                .build());
        }

        public GraphProjectBuilder withRelationshipProperty(String propertyKey, String neoPropertyKey) {
            return withRelationshipProperty(ImmutablePropertyMapping
                .builder()
                .propertyKey(propertyKey)
                .neoPropertyKey(neoPropertyKey)
                .build());
        }

        public GraphProjectBuilder withRelationshipProperty(String neoPropertyKey, DefaultValue defaultValue) {
            return withRelationshipProperty(PropertyMapping.of(neoPropertyKey, defaultValue));
        }

        public GraphProjectBuilder withRelationshipProperty(
            String propertyKey,
            String neoPropertyKey,
            DefaultValue defaultValue
        ) {
            return withRelationshipProperty(PropertyMapping.of(propertyKey, neoPropertyKey, defaultValue));
        }

        public GraphProjectBuilder withRelationshipProperty(
            String propertyKey,
            DefaultValue defaultValue,
            Aggregation aggregation
        ) {
            return withRelationshipProperty(PropertyMapping.of(propertyKey, defaultValue, aggregation));
        }

        public GraphProjectBuilder withRelationshipProperty(
            String propertyKey,
            Aggregation aggregation
        ) {
            return withRelationshipProperty(PropertyMapping.of(propertyKey, aggregation));
        }

        public GraphProjectBuilder withRelationshipProperty(
            String propertyKey,
            String neoPropertyKey,
            DefaultValue defaultValue,
            Aggregation aggregation
        ) {
            return withRelationshipProperty(PropertyMapping.of(
                propertyKey,
                neoPropertyKey,
                defaultValue,
                aggregation
            ));
        }

        public GraphProjectBuilder withRelationshipProperty(PropertyMapping propertyMapping) {
            graphProjectConfigBuilder.addRelProperty(propertyMapping);
            return this;
        }

        public GraphProjectBuilder addParameter(String key, Object value) {
            parameters.put(key, value);
            return this;
        }

        @Language("Cypher")
        public String yields(String... elements) {
            return yields(Arrays.asList(elements));
        }

        @Language("Cypher")
        public String yields(Collection<String> elements) {
            var procedureName = "gds.graph.project";

            var queryArguments = queryArguments(graphName, graphProjectConfigBuilder.build(), parameters);
            var yieldsFields = yieldsFields(elements);

            var query = Cypher.call(procedureName).withArgs(queryArguments);
            var statement = yieldsFields.map(fields -> query.yield(fields).build()).orElseGet(query::build);

            var cypherRenderer = Renderer.getRenderer(Configuration.defaultConfig());
            return cypherRenderer.render(statement);
        }
    }

    private static final class AlgoBuilder implements ModeBuildStage, ParametersBuildStage {

        private final CallBuilder builder;


        private AlgoBuilder(String graphName, Iterable<String> namespace, String algoName) {
            builder = new CallBuilder();
            builder.graphName(graphName);
            builder.algoName(algoName);
            builder.addAllAlgoNamespace(namespace);
        }

        @Override
        public AlgoBuilder executionMode(ExecutionMode mode) {
            builder.executionMode(mode);
            return this;
        }

        @Override
        public AlgoBuilder estimationMode(ExecutionMode mode) {
            builder
                .executionMode(mode)
                .estimateSpecialExecution();
            return this;
        }

        @Override
        public AlgoBuilder addParameter(String key, Object value) {
            builder.putParameter(key, value);
            return this;
        }

        @Override
        public AlgoBuilder addPlaceholder(String key, String placeholder) {
            builder.putParameter(key, Cypher.parameter(placeholder));
            return this;
        }

        @Override
        public AlgoBuilder addVariable(String key, String variable) {
            builder.putParameter(key, GdsCypher.name(variable));
            return this;
        }

        @Override
        public AlgoBuilder addParameter(Map.Entry<String, ?> entry) {
            builder.putParameter(entry);
            return this;
        }

        @Override
        public AlgoBuilder addAllParameters(Map<String, ?> entries) {
            builder.putAllParameters(entries);
            return this;
        }

        @Language("Cypher")
        @Override
        public String yields(Iterable<String> elements) {
            builder.yields(elements);
            return build();
        }

        @Language("Cypher")
        private String build() {
            return builder.build();
        }
    }


    @Builder.Factory
    static GraphProjectFromStoreConfig inlineGraphProjectConfig(
        Optional<String> graphName,
        Map<NodeLabel, NodeProjection> nodeProjections,
        Map<RelationshipType, RelationshipProjection> relProjections,
        List<PropertyMapping> nodeProperties,
        List<PropertyMapping> relProperties
    ) {
        if (nodeProjections.isEmpty()) {
            nodeProjections = new HashMap<>();
            nodeProjections.put(ALL_NODES, NodeProjection.all());
        }

        if (relProjections.isEmpty()) {
            relProjections = new HashMap<>();
            relProjections.put(ALL_RELATIONSHIPS, RelationshipProjection.ALL);
        }

        return ImmutableGraphProjectFromStoreConfig.builder()
            .graphName(graphName.orElse(""))
            .nodeProjections(NodeProjections.create(nodeProjections))
            .relationshipProjections(ImmutableRelationshipProjections.builder().putAllProjections(relProjections).build())
            .nodeProperties(ImmutablePropertyMappings.of(nodeProperties))
            .relationshipProperties(ImmutablePropertyMappings.of(relProperties))
            .build();
    }

    private static @Nullable Expression toExpression(@Nullable Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Expression) {
            return (Expression) value;
        }
        if (value instanceof Iterable) {
            return list((Iterable<?>) value);
        }
        if (value instanceof Map) {
            return map((Map<?, ?>) value);
        }
        if (value instanceof Enum) {
            value = ((Enum<?>) value).name();
        }
        if (value instanceof Number && Double.isNaN(((Number) value).doubleValue())) {
            return Cypher.literalOf(0.0).divide(Cypher.literalOf(0.0));
        }
        return Cypher.literalOf(value);
    }

    private static @Nullable Expression list(@NotNull Iterable<?> values) {
        var list = new ArrayList<Expression>();
        for (Object value : values) {
            var expression = toExpression(value);
            if (expression != null) {
                list.add(expression);
            }
        }
        if (list.isEmpty()) {
            return null;
        }
        return Cypher.listOf(list.toArray(new Expression[0]));
    }

    private static @Nullable Expression map(@NotNull Map<?, ?> values) {
        var entries = new ArrayList<>();
        values.forEach((key, value) -> {
            var expression = toExpression(value);
            if (expression != null) {
                entries.add(String.valueOf(key));
                entries.add(expression);
            }
        });
        if (entries.isEmpty()) {
            return null;
        }
        return Cypher.mapOf(entries.toArray());
    }

    @ValueClass
    @Value.Immutable(singleton = true)
    interface MinimalObject {
        Optional<String> string();

        Optional<Map<String, Object>> map();

        default boolean isEmpty() {
            return string().isEmpty() && map().isEmpty();
        }

        default MinimalObject map(
            Function<String, MinimalObject> string,
            Function<Map<String, Object>, MinimalObject> map
        ) {
            return fold(string, map).orElse(empty());
        }

        default MinimalObject map(Function<Map<String, Object>, MinimalObject> map) {
            return fold(MinimalObject::string, map).orElse(empty());
        }

        default Optional<Object> toObject() {
            return fold(s -> s, m -> m);
        }

        default <R> Optional<R> fold(
            Function<String, R> stringFunc,
            Function<Map<String, Object>, R> mapFunc
        ) {
            return string().map(stringFunc).or(() -> map().map(mapFunc));
        }

        @Value.Check
        default void validate() {
            if (string().isPresent() && map().isPresent()) {
                throw new IllegalStateException("Cannot be both string and map");
            }
        }

        static MinimalObject string(String value) {
            return ImmutableMinimalObject.builder().string(value).build();
        }

        static MinimalObject map(Map<String, Object> value) {
            if (value.isEmpty()) {
                return empty();
            }
            return ImmutableMinimalObject.builder().map(value).build();
        }

        static MinimalObject map(String key, Object value) {
            return map(Map.of(key, value));
        }

        static MinimalObject empty() {
            return ImmutableMinimalObject.of();
        }
    }

    private static <I extends ElementIdentifier, P extends ElementProjection> MinimalObject toMinimalObject(
        AbstractProjections<I, P> allProjections
    ) {
        Map<I, P> projections = allProjections.projections();
        if (projections.isEmpty()) {
            return MinimalObject.empty();
        }
        if (projections.size() == 1) {
            Map.Entry<I, P> entry = projections.entrySet().iterator().next();
            I identifier = entry.getKey();
            P projection = entry.getValue();
            if (identifier.projectAll().equals(identifier) && isAllDefault(projection)) {
                return MinimalObject.string(PROJECT_ALL);
            }
            MinimalObject projectionObject = toMinimalObject(projection, identifier);
            return projectionObject.map(m -> MinimalObject.map(identifier.name, m));
        }
        Map<String, Object> value = new LinkedHashMap<>();
        projections.forEach((identifier, projection) ->
            toMinimalObject(projection, identifier)
                .toObject()
                .ifPresent(o -> value.put(identifier.name, o)));
        return MinimalObject.map(value);
    }

    private static boolean isAllDefault(ElementProjection projection) {
        if (projection instanceof RelationshipProjection) {
            RelationshipProjection rel = (RelationshipProjection) projection;
            if (rel.orientation() != NATURAL || rel.aggregation() != DEFAULT) {
                return false;
            }
        }
        return !projection.properties().hasMappings();
    }

    private static MinimalObject toMinimalObject(
        ElementProjection projection,
        ElementIdentifier identifier
    ) {
        if (projection instanceof NodeProjection) {
            return toMinimalObject(((NodeProjection) projection), identifier);
        }
        if (projection instanceof RelationshipProjection) {
            return toMinimalObject(((RelationshipProjection) projection), identifier);
        }
        throw new IllegalArgumentException("Unexpected projection type: " + projection.getClass().getName());
    }

    private static MinimalObject toMinimalObject(
        NodeProjection projection,
        ElementIdentifier identifier
    ) {
        MinimalObject properties = toMinimalObject(projection.properties(), false);
        if (properties.isEmpty() && matchesLabel(identifier.name, projection)) {
            return MinimalObject.string(identifier.name);
        }

        Map<String, Object> value = new LinkedHashMap<>();
        value.put(NodeProjection.LABEL_KEY, projection.label());
        properties.toObject().ifPresent(o -> value.put(PROPERTIES_KEY, o));
        return MinimalObject.map(value);
    }

    private static boolean matchesLabel(String label, NodeProjection projection) {
        return Objects.equals(projection.label(), label);
    }

    private static MinimalObject toMinimalObject(
        RelationshipProjection projection,
        ElementIdentifier identifier
    ) {
        MinimalObject properties = toMinimalObject(projection.properties(), true);

        if (properties.isEmpty() && matchesType(identifier.name, projection) && allDefaults(projection)) {
            return MinimalObject.string(identifier.name);
        }

        Map<String, Object> value = new LinkedHashMap<>();
        value.put(TYPE_KEY, projection.type());
        if (projection.orientation() != NATURAL) {
            value.put(ORIENTATION_KEY, projection.orientation().name());
        }
        if (projection.aggregation() != DEFAULT) {
            value.put(AGGREGATION_KEY, projection.aggregation().name());
        }
        if (projection.indexInverse()) {
            value.put(INDEX_INVERSE_KEY, projection.indexInverse());
        }
        properties.toObject().ifPresent(o -> value.put(PROPERTIES_KEY, o));
        return MinimalObject.map(value);
    }

    private static boolean allDefaults(RelationshipProjection projection) {
        return projection.orientation() == NATURAL && projection.aggregation() == DEFAULT && !projection.indexInverse();
    }

    private static boolean matchesType(String type, RelationshipProjection projection) {
        return projection.orientation() == NATURAL
               && projection.aggregation() == DEFAULT
               && projection.type().equals(type);
    }

    private static MinimalObject toMinimalObject(
        PropertyMappings propertyMappings,
        boolean includeAggregation
    ) {
        List<PropertyMapping> mappings = propertyMappings.mappings();
        if (mappings.isEmpty()) {
            return MinimalObject.empty();
        }
        if (mappings.size() == 1) {
            return toMinimalObject(mappings.get(0), includeAggregation, true);
        }
        Map<String, Object> properties = new LinkedHashMap<>();
        for (PropertyMapping mapping : mappings) {
            toMinimalObject(mapping, includeAggregation, false)
                .map()
                .ifPresent(properties::putAll);
        }
        return MinimalObject.map(properties);
    }

    private static MinimalObject toMinimalObject(
        PropertyMapping propertyMapping,
        boolean includeAggregation,
        boolean allowStringShortcut
    ) {
        String propertyKey = propertyMapping.propertyKey();
        String neoPropertyKey = propertyMapping.neoPropertyKey();
        if (propertyKey == null || neoPropertyKey == null) {
            return MinimalObject.empty();
        }
        Map<String, Object> value = new LinkedHashMap<>();
        value.put(PROPERTY_KEY, neoPropertyKey);
        Object defaultValue = propertyMapping.defaultValue().getObject();
        if (defaultValue != null) {
            value.put(DEFAULT_VALUE_KEY, defaultValue);
        }
        Aggregation aggregation = propertyMapping.aggregation();
        if (includeAggregation && aggregation != DEFAULT) {
            value.put(AGGREGATION_KEY, aggregation.name());
        }
        if (allowStringShortcut && value.size() == 1 && propertyKey.equals(propertyMapping.neoPropertyKey())) {
            return MinimalObject.string(propertyKey);
        }
        return MinimalObject.map(propertyKey, value);
    }
}
