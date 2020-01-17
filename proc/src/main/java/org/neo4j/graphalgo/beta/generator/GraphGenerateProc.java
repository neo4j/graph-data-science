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
package org.neo4j.graphalgo.beta.generator;

import org.neo4j.graphalgo.BaseProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.loading.GraphsByRelationshipType;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.newapi.GraphCreateFromStoreConfig;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTY_MAX_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTY_MIN_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTY_NAME_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTY_TYPE_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTY_VALUE_KEY;

public final class GraphGenerateProc extends BaseProc {

    private static final String DUMMY_RELATIONSHIP_NAME = "RELATIONSHIP";

    @Procedure(name = "gds.beta.graph.generate", mode = Mode.READ)
    public Stream<GraphGenerationStats> generate(
        @Name(value = "graphName") String graphName,
        @Name(value = "nodeCount") long nodeCount,
        @Name(value = "averageDegree") long averageDegree,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        validateGraphName(getUsername(), graphName);

        // input
        CypherMapWrapper cypherConfig = CypherMapWrapper.create(configuration);
        RandomGraphGeneratorConfig config = RandomGraphGeneratorConfig.of(
            getUsername(),
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

        if (GraphCatalog.exists(getUsername(), name)) {
            throw new IllegalArgumentException(String.format("A graph with name '%s' is already loaded.", name));
        }

        try (ProgressTimer ignored = ProgressTimer.start(time -> stats.generateMillis = time)) {
            RandomGraphGenerator generator = initializeGraphGenerator(nodeCount, averageDegree, config);

            HugeGraph graph = generator.generate();

            GraphsByRelationshipType graphFromType;

            if (generator.shouldGenerateRelationshipProperty()) {
                Map<String, Map<String, Graph>> mapping = Collections.singletonMap(
                    DUMMY_RELATIONSHIP_NAME,
                    Collections.singletonMap(generator.getMaybePropertyProducer().get().getPropertyName(), graph)
                );
                graphFromType = GraphsByRelationshipType.of(mapping);
            } else {
                graphFromType = GraphsByRelationshipType.of(graph);
            }

            stats.nodes = graphFromType.nodeCount();
            stats.relationships = graphFromType.relationshipCount();
            GraphCatalog.set(GraphCreateFromStoreConfig.emptyWithName(getUsername(), name), graphFromType);
        }

        return stats;
    }

    RandomGraphGenerator initializeGraphGenerator(long nodeCount, long averageDegree, RandomGraphGeneratorConfig config) {
        return new RandomGraphGenerator(
                nodeCount,
                averageDegree,
                config.relationshipDistribution(),
                config.relationshipSeed(),
                getRelationshipPropertyProducer(config.relationshipProperty()),
                AllocationTracker.EMPTY
        );
    }

    private Optional<RelationshipPropertyProducer> getRelationshipPropertyProducer(Map<String, Object> configMap) {
        if (configMap.isEmpty()) {
            return Optional.empty();
        }
        CypherMapWrapper config = CypherMapWrapper.create(configMap);

        String propertyName = config.requireString(RELATIONSHIP_PROPERTY_NAME_KEY);
        String generatorString = config.requireString(RELATIONSHIP_PROPERTY_TYPE_KEY);

        RelationshipPropertyProducer propertyProducer;
        switch (generatorString.toLowerCase()) {
            case "random":
                double min = config.getDouble(RELATIONSHIP_PROPERTY_MIN_KEY, 0.0);
                double max = config.getDouble(RELATIONSHIP_PROPERTY_MAX_KEY, 1.0);
                propertyProducer = RelationshipPropertyProducer.random(propertyName, min, max);
                break;

            case "fixed":
                double value = config.requireDouble(RELATIONSHIP_PROPERTY_VALUE_KEY);
                propertyProducer = RelationshipPropertyProducer.fixed(propertyName, value);
                break;

            default:
                throw new IllegalArgumentException("Unknown Relationship property generator: " + generatorString);
        }
        return Optional.of(propertyProducer);
    }

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
