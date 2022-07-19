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
package org.neo4j.gds.beta.generator;

import org.neo4j.gds.BaseProc;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.config.RandomGraphGeneratorConfig;
import org.neo4j.gds.config.RandomGraphGeneratorConfig.AllowSelfLoops;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.huge.HugeGraph;
import org.neo4j.gds.core.loading.CSRGraphStoreUtil;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_MAX_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_MIN_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_NAME_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_TYPE_KEY;
import static org.neo4j.gds.config.RandomGraphGeneratorConfig.RELATIONSHIP_PROPERTY_VALUE_KEY;
import static org.neo4j.procedure.Mode.READ;

public final class GraphGenerateProc extends BaseProc {

    @Procedure(name = "gds.beta.graph.generate", mode = READ)
    @Description(value = "Computes a random graph, which will be stored in the graph catalog.")
    public Stream<GraphGenerationStats> generate(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeCount") long nodeCount,
        @Name(value = "averageDegree") long averageDegree,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        validateGraphName(username(), graphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        RandomGraphGeneratorConfig config = RandomGraphGeneratorConfig.of(
            username(),
            graphName,
            nodeCount,
            averageDegree,
            cypherConfig
        );
        validateConfig(cypherConfig, config);

        // computation
        GraphGenerationStats result = runWithExceptionLogging(
            "Graph creation failed",
            () -> generateGraph(graphName, nodeCount, averageDegree, config)
        );
        // result
        return Stream.of(result);
    }

    private GraphGenerationStats generateGraph(
        String name,
        long nodeCount,
        long averageDegree,
        RandomGraphGeneratorConfig config
    ) {
        GraphGenerationStats stats = new GraphGenerationStats(name, averageDegree, config);

        try (ProgressTimer ignored = ProgressTimer.start(time -> stats.generateMillis = time)) {
            RandomGraphGenerator generator = initializeGraphGenerator(nodeCount, averageDegree, config);

            HugeGraph graph = generator.generate();

            Optional<String> relationshipProperty = generator
                .getMaybeRelationshipPropertyProducer()
                .map(PropertyProducer::getPropertyName);

            GraphStore graphStore = CSRGraphStoreUtil.createFromGraph(
                GraphDatabaseApiProxy.databaseId(api),
                graph,
                config.relationshipType().name,
                relationshipProperty,
                config.readConcurrency()
            );

            stats.nodes = graphStore.nodeCount();
            stats.relationships = graphStore.relationshipCount();
            GraphStoreCatalog.set(config, graphStore);
        }

        return stats;
    }

    RandomGraphGenerator initializeGraphGenerator(long nodeCount, long averageDegree, RandomGraphGeneratorConfig config) {
        RandomGraphGeneratorBuilder builder = RandomGraphGenerator.builder()
            .nodeCount(nodeCount)
            .averageDegree(averageDegree)
            .relationshipDistribution(config.relationshipDistribution())
            .relationshipType(config.relationshipType())
            .aggregation(config.aggregation())
            .orientation(config.orientation())
            .allowSelfLoops(AllowSelfLoops.of(config.allowSelfLoops()));
       if (config.relationshipSeed() != null) {
           builder.seed(config.relationshipSeed());
       }

        Optional<PropertyProducer<double[]>> maybeProducer = getRelationshipPropertyProducer(config.relationshipProperty());
        maybeProducer.ifPresent(builder::relationshipPropertyProducer);
        return builder.build();
    }

    private Optional<PropertyProducer<double[]>> getRelationshipPropertyProducer(Map<String, Object> configMap) {
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

    @SuppressWarnings("unused")
    public static class GraphGenerationStats {
        public String name;
        public long nodes;
        public long relationships;
        public long generateMillis;
        public Long relationshipSeed;
        public double averageDegree;
        public Object relationshipDistribution;
        public Object relationshipProperty;

        GraphGenerationStats(String graphName, double averageDegree, RandomGraphGeneratorConfig configuration) {
            this.name = graphName;
            this.averageDegree = averageDegree;
            this.relationshipDistribution = configuration.relationshipDistribution().name();
            this.relationshipProperty = configuration.relationshipProperty();
            this.relationshipSeed = configuration.relationshipSeed();
        }
    }
}
