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
package org.neo4j.graphalgo;

import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.utils.GdsFeatureToggles;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.GdsEditionUtils.setToEnterpriseAndRun;
import static org.neo4j.graphalgo.Orientation.NATURAL;
import static org.neo4j.graphalgo.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.graphalgo.TestSupport.FactoryType.CYPHER;
import static org.neo4j.graphalgo.TestSupport.getCypherAggregation;
import static org.neo4j.graphalgo.core.Aggregation.DEFAULT;
import static org.neo4j.graphalgo.core.Aggregation.NONE;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;
import static org.neo4j.graphalgo.utils.StringJoining.join;

public final class TestGraphLoader {

    private final GraphDatabaseAPI db;

    private final Set<String> nodeLabels;
    private final Set<String> relTypes;

    private PropertyMappings nodeProperties = PropertyMappings.of();
    private PropertyMappings relProperties = PropertyMappings.of();
    private boolean addRelationshipPropertiesToLoader;

    private Optional<Aggregation> maybeAggregation = Optional.empty();
    private Optional<Log> maybeLog = Optional.empty();

    public static TestGraphLoader from(@NotNull GraphDatabaseAPI db) {
        return new TestGraphLoader(db);
    }

    private TestGraphLoader(GraphDatabaseAPI db) {
        this.db = db;
        this.nodeLabels = new HashSet<>();
        this.relTypes = new HashSet<>();
    }

    public TestGraphLoader withLabels(String... labels) {
        nodeLabels.addAll(Arrays.asList(labels));
        return this;
    }

    public TestGraphLoader withRelationshipTypes(String... types) {
        relTypes.addAll(Arrays.asList(types));
        return this;
    }

    public TestGraphLoader withNodeProperties(PropertyMappings nodeProperties) {
        this.nodeProperties = nodeProperties;
        return this;
    }

    public TestGraphLoader withRelationshipProperties(PropertyMapping... relProperties) {
        return withRelationshipProperties(PropertyMappings.of(relProperties));
    }

    public TestGraphLoader withRelationshipProperties(PropertyMappings relProperties) {
        return withRelationshipProperties(relProperties, true);
    }

    public TestGraphLoader withRelationshipProperties(PropertyMappings relProperties, boolean addToLoader) {
        this.relProperties = relProperties;
        this.addRelationshipPropertiesToLoader = addToLoader;
        return this;
    }

    public TestGraphLoader withDefaultAggregation(Aggregation aggregation) {
        this.maybeAggregation = Optional.of(aggregation);
        return this;
    }

    public TestGraphLoader withLog(Log log) {
        this.maybeLog = Optional.of(log);
        return this;
    }

    public Graph graph(TestSupport.FactoryType factoryType) {
        return graphStore(factoryType).getUnion();
    }

    public GraphStore graphStore(TestSupport.FactoryType factoryType) {
        try (Transaction ignored = db.beginTx()) {
            if (factoryType == TestSupport.FactoryType.NATIVE_BIT_ID_MAP) {
                var graphStore = new AtomicReference<GraphStore>();
                setToEnterpriseAndRun(() ->
                    GdsFeatureToggles.USE_BIT_ID_MAP.enableAndRun(() ->
                        graphStore.set(loader(factoryType).graphStore())));

                return graphStore.get();
            }
            return loader(factoryType).graphStore();
        }
    }

    private GraphLoader loader(TestSupport.FactoryType factoryType) {
        return factoryType == CYPHER ? cypherLoader() : storeLoader();
    }

    private GraphLoader cypherLoader() {
        CypherLoaderBuilder cypherLoaderBuilder = new CypherLoaderBuilder().api(db);

        String nodeQueryTemplate = "MATCH (n) %s RETURN id(n) AS id%s%s";

        String labelString = nodeLabels.isEmpty()
            ? ""
            : nodeLabels
                .stream()
                .map(l -> "n:" + l)
                .collect(Collectors.joining(" OR ", "WHERE ", ""));

        String nodePropertiesString = getNodePropertiesString(nodeProperties, "n");

        cypherLoaderBuilder.nodeQuery(formatWithLocale(nodeQueryTemplate,
            labelString,
            nodeLabels.isEmpty() ? "" : ", labels(n) AS labels",
            nodePropertiesString));

        String relationshipQueryTemplate = "MATCH (n)-[r%s]->(m) RETURN ";
        if (!Arrays.asList(DEFAULT, NONE).contains(maybeAggregation.orElse(NONE))) {
            relationshipQueryTemplate += "DISTINCT ";
        }

        relationshipQueryTemplate += relTypes.isEmpty()
            ? " id(n) AS source, id(m) AS target%s"
            : " type(r) AS type, id(n) AS source, id(m) AS target%s";

        String relTypeString = relTypes.isEmpty()
            ? ""
            : join(relTypes, "|", ":", "");

        relProperties = getUniquePropertyMappings(relProperties);
        String relPropertiesString = getRelationshipPropertiesString(relProperties, "r");

        cypherLoaderBuilder.relationshipQuery(formatWithLocale(
            relationshipQueryTemplate,
            relTypeString,
            relPropertiesString
        ));

        cypherLoaderBuilder.validateRelationships(false);

        cypherLoaderBuilder.log(maybeLog);

        return cypherLoaderBuilder.build();
    }

    private GraphLoader storeLoader() {
        StoreLoaderBuilder storeLoaderBuilder = new StoreLoaderBuilder().api(db);
        nodeLabels.forEach(storeLoaderBuilder::addNodeLabel);

        if (relTypes.isEmpty()) {
            storeLoaderBuilder.putRelationshipProjectionsWithIdentifier(
                ALL_RELATIONSHIPS.name,
                RelationshipProjection.all().withAggregation(maybeAggregation.orElse(DEFAULT))
            );
        } else {
            relTypes.forEach(relType -> {
                RelationshipProjection template = RelationshipProjection.builder()
                    .type(relType)
                    .aggregation(maybeAggregation.orElse(DEFAULT))
                    .build();
                storeLoaderBuilder.addRelationshipProjection(template.withOrientation(NATURAL));
            });
        }
        storeLoaderBuilder.globalAggregation(maybeAggregation.orElse(DEFAULT));
        if (!nodeProperties.mappings().isEmpty()) storeLoaderBuilder.nodeProperties(nodeProperties);
        if (addRelationshipPropertiesToLoader) storeLoaderBuilder.relationshipProperties(relProperties);

        storeLoaderBuilder.log(maybeLog);

        return storeLoaderBuilder.build();
    }

    private PropertyMappings getUniquePropertyMappings(PropertyMappings propertyMappings) {
        MutableInt mutableInt = new MutableInt(0);
        return PropertyMappings.of(propertyMappings.stream()
            .map(mapping -> PropertyMapping.of(
                mapping.propertyKey(),
                addSuffix(mapping.neoPropertyKey(), mutableInt.getAndIncrement()),
                mapping.defaultValue(),
                mapping.aggregation() == DEFAULT ? maybeAggregation.orElse(NONE) : mapping.aggregation()
            ))
            .toArray(PropertyMapping[]::new)
        );
    }

    private String getNodePropertiesString(PropertyMappings propertyMappings, String entityVar) {
        return propertyMappings.hasMappings()
            ? propertyMappings
                .stream()
                .map(mapping -> formatWithLocale(
                    "COALESCE(%s.%s, %s) AS %s",
                    entityVar,
                    mapping.neoPropertyKey(),
                    mapping.defaultValue().getObject(),
                    mapping.propertyKey()
                ))
                .collect(Collectors.joining(", ", ", ", ""))
            : "";
    }

    private String getRelationshipPropertiesString(PropertyMappings propertyMappings, String entityVar) {
        return propertyMappings.hasMappings()
            ? propertyMappings
            .stream()
            .map(mapping -> formatWithLocale(
                "%s AS %s",
                getCypherAggregation(
                    mapping.aggregation().name(),
                    formatWithLocale(
                        "COALESCE(%s.%s, %f)",
                        entityVar,
                        removeSuffix(mapping.neoPropertyKey()),
                        mapping.defaultValue().getObject()
                    )
                ),
                mapping.propertyKey()
            ))
            .collect(Collectors.joining(", ", ", ", ""))
            : "";
    }

    private static final String SUFFIX = "___";

    private static String addSuffix(String propertyKey, int id) {
        return formatWithLocale("%s%s%d", propertyKey, SUFFIX, id);
    }

    private String removeSuffix(String propertyKey) {
        return propertyKey.substring(0, propertyKey.indexOf(SUFFIX));
    }
}
