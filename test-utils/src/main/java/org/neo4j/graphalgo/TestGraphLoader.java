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

import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.api.MultipleRelTypesSupport;
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
    private boolean isMultiRelTypeQuery;

    private PropertyMappings nodeProperties = PropertyMappings.of();
    private PropertyMappings relProperties = PropertyMappings.of();

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
        return withRelationshipType(relType, false);
    }

    public TestGraphLoader withRelationshipType(String relType, boolean isMultiRelTypeQuery) {
        this.maybeRelType = Optional.of(relType);
        this.isMultiRelTypeQuery = isMultiRelTypeQuery;
        return this;
    }

    public TestGraphLoader withNodeProperties(PropertyMappings nodeProperties) {
        this.nodeProperties = nodeProperties;
        return this;
    }

    public TestGraphLoader withRelationshipProperties(PropertyMapping... relProperties) {
        this.relProperties = PropertyMappings.of(relProperties);
        return this;
    }

    public TestGraphLoader withRelationshipProperties(PropertyMappings relProperties) {
        this.relProperties = relProperties;
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

    // TODO: remove type constraints when we merge MultipleRelTypesSupport into GraphFactory
    public <T extends GraphFactory & MultipleRelTypesSupport> GraphsByRelationshipType buildGraphs(Class<T> graphFactory) {
        try (Transaction ignored = db.beginTx()) {
            return loader(graphFactory).build(graphFactory).importAllGraphs();
        }
    }

    private <T extends GraphFactory> GraphLoader loader(Class<T> graphFactory) {
        GraphLoader graphLoader = new GraphLoader(db).withDirection(direction);

        if (graphFactory.isAssignableFrom(CypherGraphFactory.class)) {
            String nodeQueryTemplate = "MATCH (n) %s RETURN id(n) AS id%s";
            String labelString = maybeLabel
                .map(s -> "WHERE " + ProjectionParser.parse(s).stream().map(l -> "n:" + l).collect(Collectors.joining(" OR ")))
                .orElse("");
            // CypherNodeLoader not yet supports parsing node props from return items ...
            String nodePropertiesString = nodeProperties.hasMappings()
                ? ", " + nodeProperties.stream()
                .map(PropertyMapping::neoPropertyKey)
                .map(k -> "n." + k + " AS " + k)
                .collect(Collectors.joining(", "))
                : "";

            String nodeQuery = String.format(nodeQueryTemplate, labelString, nodePropertiesString);
            graphLoader.withLabel(nodeQuery);

            String relsQuery = isMultiRelTypeQuery ? getMultiTypeRelationshipsQuery() : getAnyTypeRelationshipsQuery();

            graphLoader.withRelationshipType(relsQuery);
        } else {
            maybeLabel.ifPresent(graphLoader::withLabel);
            maybeRelType.ifPresent(graphLoader::withRelationshipType);
        }
        graphLoader.withDeduplicationStrategy(maybeDeduplicationStrategy.orElse(DeduplicationStrategy.SINGLE));
        graphLoader.withRelationshipProperties(relProperties);
        graphLoader.withOptionalNodeProperties(nodeProperties);

        return graphLoader;
    }

    private String getAnyTypeRelationshipsQuery() {
        String relsQueryTemplate = "MATCH (n)-[r%s]->(m) RETURN id(n) AS source, id(m) AS target%s";
        String relTypeString = maybeRelType.map(s -> ":" + s).orElse("");

        assert relProperties.numberOfMappings() <= 1;
        String relPropertiesString = relProperties.hasMappings()
            ? ", " + relProperties.stream()
            .map(PropertyMapping::propertyKey)
            .map(k -> "r." + k + " AS weight")
            .collect(Collectors.joining(", "))
            : "";

        return String.format(relsQueryTemplate, relTypeString, relPropertiesString);
    }

    private String getMultiTypeRelationshipsQuery() {
        String relsQueryTemplate = "MATCH (n)-[r%s]->(m) RETURN type(r) AS type, id(n) AS source, id(m) AS target%s";
        String relTypeString = maybeRelType.map(s -> ":" + s).orElse("");

        String relPropertiesString = relProperties.hasMappings()
            ? ", " + relProperties.stream()
            .map(PropertyMapping::neoPropertyKey)
            .map(k -> "r." + k + " AS " + k)
            .collect(Collectors.joining(", "))
            : "";

        return String.format(relsQueryTemplate, relTypeString, relPropertiesString);
    }
}
