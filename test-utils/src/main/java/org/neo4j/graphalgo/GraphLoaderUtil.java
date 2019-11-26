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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.CypherGraphFactory;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Optional;
import java.util.stream.Collectors;

public final class GraphLoaderUtil {

    private GraphLoaderUtil() {}

    public static Graph initLoader(GraphDatabaseAPI db, Class<? extends GraphFactory> graphFactory) {
        try (Transaction ignored = db.beginTx()) {
            return initLoader(db, graphFactory, null, null).load(graphFactory);
        }
    }

    public static GraphLoader initLoader(GraphDatabaseAPI db, Class<? extends GraphFactory> graphFactory, String label, String relType) {
        return initLoader(
            db,
            graphFactory,
            label != null ? Optional.of(label) : Optional.empty(),
            relType != null ? Optional.of(relType) : Optional.empty(),
            PropertyMappings.EMPTY,
            PropertyMappings.EMPTY);
    }

    public static GraphLoader initLoader(
        GraphDatabaseAPI db,
        Class<? extends GraphFactory> graphFactory,
        Optional<String> maybeLabel,
        Optional<String> maybeRelType,
        PropertyMappings nodeProperties,
        PropertyMappings relProperties) {

        GraphLoader graphLoader = new GraphLoader(db);

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
            graphLoader.withRelationshipType(relsQuery).withDeduplicationStrategy(DeduplicationStrategy.SKIP);
        } else {
            maybeLabel.ifPresent(graphLoader::withLabel);
            maybeRelType.ifPresent(graphLoader::withRelationshipType);
        }
        graphLoader.withRelationshipProperties(relProperties);
        graphLoader.withOptionalNodeProperties(nodeProperties);

        return graphLoader;
    }
}
