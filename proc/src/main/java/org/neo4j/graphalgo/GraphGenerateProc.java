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
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.GraphLoadFactory;
import org.neo4j.graphalgo.core.loading.GraphsByRelationshipType;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.generator.RandomGraphGenerator;
import org.neo4j.graphalgo.impl.generator.RelationshipDistribution;
import org.neo4j.graphalgo.impl.generator.RelationshipPropertyProducer;
import org.neo4j.graphalgo.utils.ConfigMapHelper;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_DISTRIBUTION_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTIES_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTY_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTY_MAX_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTY_MIN_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTY_NAME_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTY_TYPE_KEY;
import static org.neo4j.graphalgo.core.ProcedureConstants.RELATIONSHIP_PROPERTY_VALUE_KEY;

public final class GraphGenerateProc extends BaseProc {

    public static final String DUMMY_RELATIONSHIP_NAME = "RELATIONSHIP";


    @Procedure(name = "algo.beta.graph.generate", mode = Mode.READ)
    @Description("CALL algo.beta.graph.generate(" +
                 "name:String, nodeCount:Integer, averageDegree:Integer" +
                 "{distribution: 'UNIFORM,RANDOM,POWERLAW', relationshipProperty: {name: '[PROPERTY_NAME]' type: 'FIXED,RANDOM', min: 0.0, max: 1.0, value: 1.0}}) " +
                 "YIELD name, nodes, relationships, generateMillis, averageDegree, relationshipDistribution, relationshipProperty")
    public Stream<GraphGenerationStats> generate(
            @Name(value = "name") String name,
            @Name(value = "nodeCount") Long nodeCount,
            @Name(value = "averageDegree") Long averageDegree,
            @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {

        final ProcedureConfiguration configuration = newConfig(null, null, config);
        GraphGenerationStats stats = runWithExceptionLogging(
                "Graph generation failed",
                () -> generateGraph(configuration, name, nodeCount, averageDegree));
        return Stream.of(stats);
    }

    private GraphGenerationStats generateGraph(
            ProcedureConfiguration config,
            String name,
            long nodeCount,
            long averageDegree) {
        GraphGenerationStats stats = new GraphGenerationStats(name, averageDegree, config);

        if (GraphLoadFactory.exists(name)) {
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
            GraphLoadFactory.set(name, graphFromType);
        }

        return stats;
    }

    RandomGraphGenerator initializeGraphGenerator(long nodeCount, long averageDegree, ProcedureConfiguration config) {
        return new RandomGraphGenerator(
                nodeCount,
                averageDegree,
                getRelationshipDistribution(config),
                getRelationshipPropertyProducer(config),
                AllocationTracker.EMPTY
        );
    }

    private RelationshipDistribution getRelationshipDistribution(ProcedureConfiguration config) {
        String distributionValue = config
                .getString(RELATIONSHIP_DISTRIBUTION_KEY, RelationshipDistribution.UNIFORM.name())
                .toLowerCase();
        return RelationshipDistribution.valueOf(distributionValue.toUpperCase());
    }

    private Optional<RelationshipPropertyProducer> getRelationshipPropertyProducer(ProcedureConfiguration config) {
        Object maybeMap = config.get(RELATIONSHIP_PROPERTY_KEY);

        if ((maybeMap instanceof Map)) {
            Map<String, Object> configMap = (Map<String, Object>) maybeMap;

            if (configMap.isEmpty()) {
                return Optional.empty();
            }

            String propertyName = ConfigMapHelper.getString(configMap, RELATIONSHIP_PROPERTY_NAME_KEY);
            String generatorString = ConfigMapHelper.getString(configMap, RELATIONSHIP_PROPERTY_TYPE_KEY);

            RelationshipPropertyProducer propertyProducer;
            switch (generatorString.toLowerCase()) {
                case "random":
                    double min = ConfigMapHelper.getDouble(configMap, RELATIONSHIP_PROPERTY_MIN_KEY, 0.0);
                    double max = ConfigMapHelper.getDouble(configMap, RELATIONSHIP_PROPERTY_MAX_KEY, 1.0);
                    propertyProducer = RelationshipPropertyProducer.random(propertyName, min, max);
                    break;

                case "fixed":
                    double value = ConfigMapHelper.getDouble(configMap, RELATIONSHIP_PROPERTY_VALUE_KEY);
                    propertyProducer = RelationshipPropertyProducer.fixed(propertyName, value);
                    break;

                default:
                    throw new IllegalArgumentException("Unknown Relationship property generator: " + generatorString);
            }
            return Optional.of(propertyProducer);
        } else if (maybeMap == null) {
            return Optional.empty();
        } else {
            throw new IllegalArgumentException(String.format(
                    "Expected the value of `%s` to be a Map but got `%s`",
                    RELATIONSHIP_PROPERTIES_KEY,
                    maybeMap.getClass().getSimpleName()));
        }
    }

    @Override
    protected GraphLoader configureLoader(GraphLoader loader, ProcedureConfiguration config) {
        return null;
    }

    public static class GraphGenerationStats {
        public String name;
        public long nodes, relationships, generateMillis;
        public double averageDegree;
        public Object relationshipDistribution, relationshipProperty;

        GraphGenerationStats(String graphName, double averageDegree, ProcedureConfiguration configuration) {
            this.name = graphName;
            this.averageDegree = averageDegree;
            this.relationshipDistribution = configuration.getString(RELATIONSHIP_DISTRIBUTION_KEY, "UNIFORM");
            this.relationshipProperty = configuration.get(RELATIONSHIP_PROPERTY_KEY, null);
        }
    }
}
