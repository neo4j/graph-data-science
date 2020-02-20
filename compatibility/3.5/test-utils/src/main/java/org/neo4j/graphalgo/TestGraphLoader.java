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

import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphalgo.core.loading.GraphsByRelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.Orientation.NATURAL;
import static org.neo4j.graphalgo.core.Aggregation.DEFAULT;

public final class TestGraphLoader {

    private final GraphDatabaseAPI db;

    private final Set<String> nodeLabels;
    private final Set<String> relTypes;

    private PropertyMappings nodeProperties = PropertyMappings.of();
    private boolean addNodePropertiesToLoader;
    private PropertyMappings relProperties = PropertyMappings.of();
    private boolean addRelationshipPropertiesToLoader;

    private Optional<Aggregation> maybeAggregation = Optional.empty();

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
        return withNodeProperties(nodeProperties, true);
    }

    public TestGraphLoader withNodeProperties(PropertyMappings nodeProperties, boolean addToLoader) {
        this.nodeProperties = nodeProperties;
        this.addNodePropertiesToLoader = addToLoader;
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

    public <T extends GraphFactory> Graph buildGraph(Class<T> graphFactory) {
        try (Transaction ignored = db.beginTx()) {
            return loader(graphFactory).build(graphFactory).build().graphs().getUnion();
        }
    }

    public <T extends GraphFactory> GraphsByRelationshipType buildGraphs(Class<T> graphFactory) {
        try (Transaction ignored = db.beginTx()) {
            return loader(graphFactory).build(graphFactory).build().graphs();
        }
    }

    private <T extends GraphFactory> GraphLoader loader(Class<T> graphFactory) {
        return graphFactory.isAssignableFrom(CypherGraphFactory.class) ? cypherLoader() : storeLoader();
    }

    private GraphLoader cypherLoader() {
        CypherLoaderBuilder cypherLoaderBuilder = new CypherLoaderBuilder().api(db);

        String nodeQueryTemplate = "MATCH (n) %s RETURN id(n) AS id%s";

        String labelString = nodeLabels.isEmpty()
            ? ""
            : nodeLabels
                .stream()
                .map(l -> "n:" + l)
                .collect(Collectors.joining(" OR ", "WHERE ", ""));

        nodeProperties = getUniquePropertyMappings(nodeProperties);
        String nodePropertiesString = getPropertiesString(nodeProperties, "n");

        cypherLoaderBuilder.nodeQuery(String.format(nodeQueryTemplate, labelString, nodePropertiesString));

        String relationshipQueryTemplate = relTypes.isEmpty()
            ? "MATCH (n)-[r%s]->(m) RETURN id(n) AS source, id(m) AS target%s"
            : "MATCH (n)-[r%s]->(m) RETURN type(r) AS type, id(n) AS source, id(m) AS target%s";

        String relTypeString = relTypes.isEmpty()
            ? ""
            : relTypes.stream().collect(Collectors.joining("|", ":", ""));

        relProperties = getUniquePropertyMappings(relProperties);
        String relPropertiesString = getPropertiesString(relProperties, "r");

        cypherLoaderBuilder.relationshipQuery(String.format(
            relationshipQueryTemplate,
            relTypeString,
            relPropertiesString
        ));

        cypherLoaderBuilder.globalAggregation(maybeAggregation.orElse(DEFAULT));
        if (addNodePropertiesToLoader) cypherLoaderBuilder.nodeProperties(nodeProperties);
        if (addRelationshipPropertiesToLoader) cypherLoaderBuilder.relationshipProperties(relProperties);

        return cypherLoaderBuilder.build();
    }

    private GraphLoader storeLoader() {
        StoreLoaderBuilder storeLoaderBuilder = new StoreLoaderBuilder().api(db);
        if (nodeLabels.isEmpty()) {
            storeLoaderBuilder.loadAnyLabel();
        } else {
            nodeLabels.forEach(storeLoaderBuilder::addNodeLabel);
        }

        if (relTypes.isEmpty()) {
            storeLoaderBuilder.putRelationshipProjectionsWithIdentifier(
                "*",
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
        if (addNodePropertiesToLoader) storeLoaderBuilder.nodeProperties(nodeProperties);
        if (addRelationshipPropertiesToLoader) storeLoaderBuilder.relationshipProperties(relProperties);
        return storeLoaderBuilder.build();
    }

    private PropertyMappings getUniquePropertyMappings(PropertyMappings propertyMappings) {
        MutableInt mutableInt = new MutableInt(0);
        return PropertyMappings.of(propertyMappings.stream()
            .map(mapping -> PropertyMapping.of(
                mapping.propertyKey(),
                addSuffix(mapping.neoPropertyKey(), mutableInt.getAndIncrement()),
                mapping.defaultValue(),
                mapping.aggregation()
            ))
            .toArray(PropertyMapping[]::new)
        );
    }

    private String getPropertiesString(PropertyMappings propertyMappings, String entityVar) {
        return propertyMappings.hasMappings()
            ? propertyMappings
            .stream()
            .map(mapping -> String.format(
                "COALESCE(%s.%s, %f) AS %s",
                entityVar,
                removeSuffix(mapping.neoPropertyKey()),
                mapping.defaultValue(),
                mapping.neoPropertyKey()
            ))
            .collect(Collectors.joining(", ", ", ", ""))
            : "";
    }

    private static final String SUFFIX = "___";

    public static String addSuffix(String propertyKey, int id) {
        return String.format("%s%s%d", propertyKey, SUFFIX, id);
    }

    private String removeSuffix(String propertyKey) {
        return propertyKey.substring(0, propertyKey.indexOf(SUFFIX));
    }
}
