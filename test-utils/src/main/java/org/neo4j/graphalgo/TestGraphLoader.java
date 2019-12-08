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

import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphalgo.core.loading.GraphsByRelationshipType;
import org.neo4j.graphalgo.core.utils.ProjectionParser;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Optional;
import java.util.stream.Collectors;

public final class TestGraphLoader {

    private final GraphDatabaseAPI db;

    private Optional<String> maybeLabel = Optional.empty();
    private Optional<String> maybeRelType = Optional.empty();

    private PropertyMappings nodeProperties = PropertyMappings.of();
    private boolean addNodePropertiesToLoader;
    private PropertyMappings relProperties = PropertyMappings.of();
    private boolean addRelationshipPropertiesToLoader;

    private Direction direction = Direction.OUTGOING;
    private Optional<DeduplicationStrategy> maybeDeduplicationStrategy = Optional.empty();

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

    public TestGraphLoader withDirection(Direction direction) {
        this.direction = direction;
        return this;
    }

    public TestGraphLoader withDeduplicationStrategy(DeduplicationStrategy deduplicationStrategy) {
        this.maybeDeduplicationStrategy = Optional.of(deduplicationStrategy);
        return this;
    }

    public <T extends GraphFactory> Graph buildGraph(Class<T> graphFactory) {
        try (Transaction ignored = db.beginTx()) {
            return loader(graphFactory).build(graphFactory).build();
        }
    }

    public <T extends GraphFactory> GraphsByRelationshipType buildGraphs(Class<T> graphFactory) {
        try (Transaction ignored = db.beginTx()) {
            return loader(graphFactory).build(graphFactory).importAllGraphs();
        }
    }

    private <T extends GraphFactory> GraphLoader loader(Class<T> graphFactory) {
        GraphLoader graphLoader = new GraphLoader(db).withDirection(direction);

        if (graphFactory.isAssignableFrom(CypherGraphFactory.class)) {
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

            String nodeQuery = String.format(nodeQueryTemplate, labelString, nodePropertiesString);
            graphLoader.withLabel(nodeQuery);

            String relationshipQueryTemplate = maybeRelType.isPresent()
                ? "MATCH (n)-[r%s]->(m) RETURN type(r) AS type, id(n) AS source, id(m) AS target%s"
                : "MATCH (n)-[r%s]->(m) RETURN id(n) AS source, id(m) AS target%s";

            String relTypeString = maybeRelType.map(s -> ":" + s).orElse("");
            relProperties = getUniquePropertyMappings(relProperties);
            String relPropertiesString = getPropertiesString(relProperties, "r");

            graphLoader.withRelationshipType(String.format(
                relationshipQueryTemplate,
                relTypeString,
                relPropertiesString
            ));
        } else {
            maybeLabel.ifPresent(graphLoader::withLabel);
            maybeRelType.ifPresent(graphLoader::withRelationshipType);
        }
        graphLoader.withDeduplicationStrategy(maybeDeduplicationStrategy.orElse(DeduplicationStrategy.SINGLE));
        if (addNodePropertiesToLoader) graphLoader.withOptionalNodeProperties(nodeProperties);
        if (addRelationshipPropertiesToLoader) graphLoader.withRelationshipProperties(relProperties);

        return graphLoader;
    }

    private PropertyMappings getUniquePropertyMappings(PropertyMappings propertyMappings) {
        MutableInt mutableInt = new MutableInt(0);
        return PropertyMappings.of(propertyMappings.stream()
            .map(mapping -> PropertyMapping.of(
                mapping.propertyKey(),
                addSuffix(mapping.neoPropertyKey(), mutableInt.getAndIncrement()),
                mapping.defaultValue(),
                mapping.deduplicationStrategy()
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
