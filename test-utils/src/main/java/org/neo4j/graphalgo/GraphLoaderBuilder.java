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
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Optional;
import java.util.stream.Collectors;

public final class GraphLoaderBuilder {

    private final GraphDatabaseAPI db;
    private final Class<? extends GraphFactory> graphFactory;

    private Optional<String> maybeLabel = Optional.empty();
    private Optional<String> maybeRelType = Optional.empty();

    private PropertyMappings nodeProperties = PropertyMappings.EMPTY;
    private PropertyMappings relProperties = PropertyMappings.EMPTY;

    private Direction direction = Direction.OUTGOING;
    private Optional<DeduplicationStrategy> maybeDeduplicationStrategy = Optional.empty();

    public static GraphLoaderBuilder from(@NotNull GraphDatabaseAPI db, @NotNull Class<? extends GraphFactory> graphFactory) {
        return new GraphLoaderBuilder(db, graphFactory);
    }

    private GraphLoaderBuilder(GraphDatabaseAPI db, Class<? extends GraphFactory> graphFactory) {
        this.db = db;
        this.graphFactory = graphFactory;
    }

    public GraphLoaderBuilder withLabel(String label) {
        this.maybeLabel = Optional.of(label);
        return this;
    }

    public GraphLoaderBuilder withRelType(String relType) {
        this.maybeRelType = Optional.of(relType);
        return this;
    }

    public GraphLoaderBuilder withNodeProperties(PropertyMappings nodeProperties) {
        this.nodeProperties = nodeProperties;
        return this;
    }

    public GraphLoaderBuilder withRelProperties(PropertyMappings relProperties) {
        this.relProperties = relProperties;
        return this;
    }

    public GraphLoaderBuilder withDirection(Direction direction) {
        this.direction = direction;
        return this;
    }

    public GraphLoaderBuilder withDeduplicationStrategy(DeduplicationStrategy deduplicationStrategy) {
        this.maybeDeduplicationStrategy = Optional.of(deduplicationStrategy);
        return this;
    }

    public Graph load() {
        try (Transaction ignored = db.beginTx()) {
            return build().load(graphFactory);
        }
    }

    public GraphLoader build() {
        GraphLoader graphLoader = new GraphLoader(db).withDirection(direction);

        String nodeQueryTemplate = "MATCH (n%s) RETURN id(n) AS id%s";
        String relsQueryTemplate = "MATCH (n)-[r%s]->(m) RETURN id(n) AS source, id(m) AS target%s";

        if (graphFactory.isAssignableFrom(CypherGraphFactory.class)) {
            String labelString = maybeLabel.map(s -> ":" + s).orElse("");
            // CypherNodeLoader not yet supports parsing node props from return items ...
            String nodePropertiesString = nodeProperties.hasMappings()
                ? ", " + nodeProperties.stream()
                .map(PropertyMapping::neoPropertyKey)
                .map(k -> "n." + k + " AS " + k)
                .collect(Collectors.joining(", "))
                : "";

            String nodeQuery = String.format(nodeQueryTemplate, labelString, nodePropertiesString);
            graphLoader.withLabel(nodeQuery);

            String relTypeString = maybeRelType.map(s -> ":" + s).orElse("");

            assert relProperties.numberOfMappings() <= 1;
            String relPropertiesString = relProperties.hasMappings()
                ? ", " + relProperties.stream()
                .map(PropertyMapping::propertyKey)
                .map(k -> "r." + k + " AS weight")
                .collect(Collectors.joining(", "))
                : "";

            String relsQuery = String.format(relsQueryTemplate, relTypeString, relPropertiesString);
            graphLoader.withRelationshipType(relsQuery);
        } else {
            maybeLabel.ifPresent(graphLoader::withLabel);
            maybeRelType.ifPresent(graphLoader::withRelationshipType);
        }
        graphLoader.withDeduplicationStrategy(maybeDeduplicationStrategy.orElse(DeduplicationStrategy.SKIP));
        graphLoader.withRelationshipProperties(relProperties);
        graphLoader.withOptionalNodeProperties(nodeProperties);

        return graphLoader;
    }
}
