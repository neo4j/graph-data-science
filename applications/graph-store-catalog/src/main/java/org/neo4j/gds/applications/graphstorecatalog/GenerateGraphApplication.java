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
package org.neo4j.gds.applications.graphstorecatalog;

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.beta.generator.PropertyProducer;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RandomGraphGeneratorBuilder;
import org.neo4j.gds.config.RandomGraphGeneratorConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.CSRGraphStoreUtil;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.logging.Log;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_MAX_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_MIN_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_NAME_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_TYPE_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_VALUE_KEY;

public class GenerateGraphApplication {
    private final Log log;
    private final GraphStoreCatalogService graphStoreCatalogService;

    public GenerateGraphApplication(Log log, GraphStoreCatalogService graphStoreCatalogService) {
        this.log = log;
        this.graphStoreCatalogService = graphStoreCatalogService;
    }

    GraphGenerationStats compute(
        DatabaseId databaseId,
        long averageDegree,
        RandomGraphGeneratorConfig configuration
    ) {
        try {
            return generateGraph(databaseId, configuration.graphName(), averageDegree, configuration);
        } catch (RuntimeException e) {
            log.warn("Graph creation failed", e);
            throw e;
        }
    }

    private GraphGenerationStats generateGraph(
        DatabaseId databaseId,
        String name,
        long averageDegree,
        RandomGraphGeneratorConfig config
    ) {
        GraphGenerationStats stats = new GraphGenerationStats(name, averageDegree, config);

        try (ProgressTimer ignored = ProgressTimer.start(time -> stats.generateMillis = time)) {
            RandomGraphGenerator generator = initializeGraphGenerator(config);

            HugeGraph graph = generator.generate();

            Optional<String> relationshipProperty = generator
                .getMaybeRelationshipPropertyProducer()
                .map(PropertyProducer::getPropertyName);

            GraphStore graphStore = CSRGraphStoreUtil.createFromGraph(
                databaseId,
                graph,
                relationshipProperty,
                config.typedReadConcurrency()
            );

            stats.nodes = graphStore.nodeCount();
            stats.relationships = graphStore.relationshipCount();

            graphStoreCatalogService.set(config, graphStore);
        }

        return stats;
    }

    static RandomGraphGenerator initializeGraphGenerator(RandomGraphGeneratorConfig config) {
        RandomGraphGeneratorBuilder builder = RandomGraphGenerator.builder()
            .nodeCount(config.nodeCount())
            .averageDegree(config.averageDegree())
            .relationshipDistribution(config.relationshipDistribution())
            .relationshipType(config.relationshipType())
            .aggregation(config.aggregation())
            .direction(Direction.fromOrientation(config.orientation()))
            .allowSelfLoops(RandomGraphGeneratorConfig.AllowSelfLoops.of(config.allowSelfLoops()));
        if (config.relationshipSeed() != null) {
            builder.seed(config.relationshipSeed());
        }

        Optional<PropertyProducer<double[]>> maybeProducer = getRelationshipPropertyProducer(config.relationshipProperty());
        maybeProducer.ifPresent(builder::relationshipPropertyProducer);
        return builder.build();
    }

    private static Optional<PropertyProducer<double[]>> getRelationshipPropertyProducer(Map<String, Object> configMap) {
        if (configMap.isEmpty()) {
            return Optional.empty();
        }
        CypherMapWrapper config = CypherMapWrapper.create(configMap);

        String propertyName = config.requireString(RELATIONSHIP_PROPERTY_NAME_KEY);
        String generatorString = config.requireString(RELATIONSHIP_PROPERTY_TYPE_KEY);

        PropertyProducer<double[]> propertyProducer;
        switch (generatorString.toLowerCase(Locale.ENGLISH)) {
            case "random":
                double min = config.getDouble(RELATIONSHIP_PROPERTY_MIN_KEY, 0.0);
                double max = config.getDouble(RELATIONSHIP_PROPERTY_MAX_KEY, 1.0);
                propertyProducer = PropertyProducer.randomDouble(propertyName, min, max);
                break;

            case "fixed":
                double value = config.requireDouble(RELATIONSHIP_PROPERTY_VALUE_KEY);
                propertyProducer = PropertyProducer.fixedDouble(propertyName, value);
                break;

            default:
                throw new IllegalArgumentException("Unknown Relationship property generator: " + generatorString);
        }
        return Optional.of(propertyProducer);
    }
}
