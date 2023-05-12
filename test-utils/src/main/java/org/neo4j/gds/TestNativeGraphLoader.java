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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.GraphLoader;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.neo4j.gds.Orientation.NATURAL;
import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.gds.core.Aggregation.DEFAULT;

public final class TestNativeGraphLoader implements TestGraphLoader {

    private final GraphDatabaseService db;

    private final Set<String> nodeLabels;
    private final Set<String> relTypes;

    private PropertyMappings nodeProperties = PropertyMappings.of();
    private PropertyMappings relProperties = PropertyMappings.of();
    private boolean addRelationshipPropertiesToLoader;

    private Optional<Aggregation> maybeAggregation = Optional.empty();
    private Optional<Orientation> maybeOrientation = Optional.empty();
    private Optional<Log> maybeLog = Optional.empty();

    public TestNativeGraphLoader(GraphDatabaseService db) {
        this.db = db;
        this.nodeLabels = new HashSet<>();
        this.relTypes = new HashSet<>();
    }

    public TestNativeGraphLoader withLabels(String... labels) {
        nodeLabels.addAll(Arrays.asList(labels));
        return this;
    }

    public TestNativeGraphLoader withRelationshipTypes(String... types) {
        relTypes.addAll(Arrays.asList(types));
        return this;
    }

    public TestNativeGraphLoader withNodeProperties(PropertyMappings nodeProperties) {
        this.nodeProperties = nodeProperties;
        return this;
    }

    public TestNativeGraphLoader withRelationshipProperties(PropertyMapping... relProperties) {
        return withRelationshipProperties(PropertyMappings.of(relProperties));
    }

    public TestNativeGraphLoader withRelationshipProperties(PropertyMappings relProperties) {
        return withRelationshipProperties(relProperties, true);
    }

    public TestNativeGraphLoader withRelationshipProperties(PropertyMappings relProperties, boolean addToLoader) {
        this.relProperties = relProperties;
        this.addRelationshipPropertiesToLoader = addToLoader;
        return this;
    }

    public TestNativeGraphLoader withDefaultAggregation(Aggregation aggregation) {
        this.maybeAggregation = Optional.of(aggregation);
        return this;
    }

    public TestNativeGraphLoader withDefaultOrientation(Orientation orientation) {
        this.maybeOrientation = Optional.of(orientation);
        return this;
    }


    public TestNativeGraphLoader withLog(Log log) {
        this.maybeLog = Optional.of(log);
        return this;
    }

    public Graph graph() {
        return graphStore().getUnion();
    }

    public GraphStore graphStore() {
        try (Transaction ignored = db.beginTx()) {
            return storeLoader().graphStore();
        }
    }

    private GraphLoader storeLoader() {
        StoreLoaderBuilder storeLoaderBuilder = new StoreLoaderBuilder().databaseService(db);
        nodeLabels.forEach(storeLoaderBuilder::addNodeLabel);

        var aggregation = maybeAggregation.orElse(DEFAULT);
        var orientation = maybeOrientation.orElse(NATURAL);
        if (relTypes.isEmpty()) {
            storeLoaderBuilder.putRelationshipProjectionsWithIdentifier(
                ALL_RELATIONSHIPS.name,
                RelationshipProjection
                    .builder()
                    .from(RelationshipProjection.ALL)
                    .aggregation(aggregation)
                    .orientation(orientation)
                    .build()
            );
        } else {
            relTypes.forEach(relType -> {
                RelationshipProjection template = RelationshipProjection.builder()
                    .type(relType)
                    .aggregation(aggregation)
                    .orientation(orientation)
                    .build();
                storeLoaderBuilder.addRelationshipProjection(template);
            });
        }
        storeLoaderBuilder.globalAggregation(aggregation);
        if (!nodeProperties.mappings().isEmpty()) storeLoaderBuilder.nodeProperties(nodeProperties);
        if (addRelationshipPropertiesToLoader) storeLoaderBuilder.relationshipProperties(relProperties);

        storeLoaderBuilder.log(maybeLog);

        return storeLoaderBuilder.build();
    }
}
