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
import org.neo4j.graphalgo.core.utils.ProjectionParser;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Optional;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.Projection.NATURAL;
import static org.neo4j.graphalgo.core.Aggregation.DEFAULT;

public final class TestGraphLoader {

    private final GraphDatabaseAPI db;

    private Optional<String> maybeLabel = Optional.empty();
    private Optional<String> maybeRelType = Optional.empty();

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
    }

    public TestGraphLoader withLabel(String label) {
        this.maybeLabel = Optional.of(label);
        return this;
    }

    public TestGraphLoader withRelationshipType(String relType) {
        this.maybeRelType = Optional.of(relType);
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
        String labelString = maybeLabel
            .map(s -> ProjectionParser
                .parse(s)
                .stream()
                .map(l -> "n:" + l)
                .collect(Collectors.joining(" OR ", "WHERE ", "")))
            .orElse("");
        nodeProperties = getUniquePropertyMappings(nodeProperties);
        String nodePropertiesString = getPropertiesString(nodeProperties, "n");

        cypherLoaderBuilder.nodeQuery(String.format(nodeQueryTemplate, labelString, nodePropertiesString));

        String relationshipQueryTemplate = maybeRelType.isPresent()
            ? "MATCH (n)-[r%s]->(m) RETURN type(r) AS type, id(n) AS source, id(m) AS target%s"
            : "MATCH (n)-[r%s]->(m) RETURN id(n) AS source, id(m) AS target%s";

        String relTypeString = maybeRelType.map(s -> ":" + s).orElse("");
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
        if (maybeLabel.isPresent()) {
            storeLoaderBuilder.addNodeLabel(maybeLabel.get());
        } else {
            storeLoaderBuilder.loadAnyLabel();
        }
        if (maybeRelType.isPresent()) {
            ProjectionParser.parse(maybeRelType.get())
                .forEach(relType -> {
                    RelationshipProjection template = RelationshipProjection.empty()
                        .withType(relType)
                        .withAggregation(maybeAggregation.orElse(DEFAULT));
                    storeLoaderBuilder.addRelationshipProjection(template.withProjection(NATURAL));
                });
        } else {
            storeLoaderBuilder.putRelationshipProjectionsWithIdentifier(
                "*",
                RelationshipProjection.empty().withAggregation(maybeAggregation.orElse(DEFAULT))
            );
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
