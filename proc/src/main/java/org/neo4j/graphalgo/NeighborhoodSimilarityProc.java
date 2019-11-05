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
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.ExceptionUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.impl.jaccard.NeighborhoodSimilarity;
import org.neo4j.graphalgo.impl.jaccard.NeighborhoodSimilarityFactory;
import org.neo4j.graphalgo.impl.jaccard.SimilarityGraphResult;
import org.neo4j.graphalgo.impl.jaccard.SimilarityResult;
import org.neo4j.graphalgo.impl.results.AbstractResultBuilder;
import org.neo4j.graphalgo.impl.results.MemRecResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.storable.Values;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.graphdb.Direction.OUTGOING;

public class NeighborhoodSimilarityProc extends BaseAlgoProc<NeighborhoodSimilarity> {

    private static final String SIMILARITY_CUTOFF_KEY = "similarityCutoff";
    private static final double SIMILARITY_CUTOFF_DEFAULT = 0.0;

    private static final String DEGREE_CUTOFF_KEY = "degreeCutoff";
    private static final int DEGREE_CUTOFF_DEFAULT = 1;

    private static final String TOP_KEY = "top";
    private static final int TOP_DEFAULT = 0;

    private static final String TOP_K_KEY = "topK";
    private static final int TOP_K_DEFAULT = 10;

    private static final String WRITE_RELATIONSHIP_TYPE_KEY = "writeRelationshipType";
    private static final String WRITE_RELATIONSHIP_TYPE_DEFAULT = "SIMILAR";

    private static final String WRITE_PROPERTY_KEY = "writeProperty";
    private static final String WRITE_PROPERTY_DEFAULT = "score";

    private static final Direction COMPUTE_DIRECTION_DEFAULT = OUTGOING;

    @Procedure(name = "algo.beta.jaccard.stream", mode = Mode.READ)
    @Description("CALL algo.beta.jaccard.stream(" +
                 "nodeFilter, relationshipFilter, {" +
                 "  similarityCutoff: 0.0, degreeCutoff: 0, top: 0, topK: 10," +
                 "  graph: 'graph', direction: 'OUTGOING', concurrency: 4, readConcurrency: 4" +
                 "}) " +
                 "YIELD node1, node2, similarity - computes neighborhood similarities based on the Jaccard index")
    public Stream<SimilarityResult> stream(
        @Name(value = "nodeFilter", defaultValue = "") String nodeFilter,
        @Name(value = "relationshipFilter", defaultValue = "") String relationshipFilter,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {
        AllocationTracker tracker = AllocationTracker.create();
        ProcedureConfiguration configuration = newConfig(nodeFilter, relationshipFilter, config);
        Graph graph = loadGraph(configuration, tracker, new WriteResultBuilder());

        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }

        NeighborhoodSimilarity neighborhoodSimilarity = newAlgorithm(graph, configuration, tracker);

        Direction direction = configuration.getDirection(COMPUTE_DIRECTION_DEFAULT);
        return neighborhoodSimilarity.computeToStream(direction);
    }

    @Procedure(name = "algo.beta.jaccard", mode = Mode.WRITE)
    @Description("CALL algo.beta.jaccard(" +
                 "nodeFilter, relationshipFilter, {" +
                 "  similarityCutoff: 0.0, degreeCutoff: 0, top: 0, topK: 10," +
                 "  graph: 'graph', direction: 'OUTGOING', concurrency: 4, readConcurrency: 4," +
                 "  write: 'true', writeRelationshipType: 'SIMILAR_TO', writeProperty: 'similarity', writeConcurrency: 4" +
                 "}) " +
                 "YIELD nodesCompared, relationships, write, writeRelationshipType, writeProperty - computes neighborhood similarities based on the Jaccard index")
    public Stream<NeighborhoodSimilarityResult> write(
        @Name(value = "nodeFilter", defaultValue = "") String nodeFilter,
        @Name(value = "relationshipFilter", defaultValue = "") String relationshipFilter,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {
        WriteResultBuilder resultBuilder = new WriteResultBuilder();
        AllocationTracker tracker = AllocationTracker.create();
        ProcedureConfiguration configuration = newConfig(nodeFilter, relationshipFilter, config);
        Graph graph = loadGraph(configuration, tracker, resultBuilder);

        String writeRelationshipType = configuration.get(WRITE_RELATIONSHIP_TYPE_KEY, WRITE_RELATIONSHIP_TYPE_DEFAULT);
        String writeProperty = configuration.get(WRITE_PROPERTY_KEY, WRITE_PROPERTY_DEFAULT);

        resultBuilder
            .withWriteRelationshipType(writeRelationshipType)
            .withWrite(configuration.isWriteFlag())
            .withWriteProperty(writeProperty);

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(resultBuilder.build());
        }

        NeighborhoodSimilarity neighborhoodSimilarity = newAlgorithm(graph, configuration, tracker);

        Direction direction = configuration.getDirection(COMPUTE_DIRECTION_DEFAULT);
        SimilarityGraphResult similarityGraphResult = neighborhoodSimilarity.computeToGraph(direction);
        Graph similarityGraph = similarityGraphResult.similarityGraph();
        resultBuilder
            .withNodesCompared(similarityGraphResult.comparedNodes())
            .withRelationshipCount(similarityGraph.relationshipCount());

        if (configuration.isWriteFlag() && similarityGraph.relationshipCount() > 0) {
            Exporter.of(api, similarityGraph)
                .withLog(log)
                .build()
                .writeRelationshipAndProperty(writeRelationshipType, writeProperty,
                    (ops, relType, propertyType) -> {
                        similarityGraph.forEachNode(nodeId -> {
                            similarityGraph.forEachRelationship(
                                nodeId,
                                OUTGOING,
                                0.0,
                                writeBack(relType, propertyType, similarityGraph, ops)
                            );
                            return true;
                        });
                    }
                );
        }
        return Stream.of(resultBuilder.build());
    }

    @Procedure(value = "algo.beta.jaccard.memrec")
    public Stream<MemRecResult> memrec(
        @Name(value = "nodeFilter", defaultValue = "") String nodeFilter,
        @Name(value = "relationshipFilter", defaultValue = "") String relationshipFilter,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {
        ProcedureConfiguration configuration = newConfig(nodeFilter, relationshipFilter, config);
        MemoryTreeWithDimensions memoryEstimation = this.memoryEstimation(configuration);
        return Stream.of(new MemRecResult(memoryEstimation));
    }

    private static RelationshipWithPropertyConsumer writeBack(int relType, int propertyType, Graph graph, Write ops) {
        return (source, target, property) -> {
            try {
                final long relId = ops.relationshipCreate(
                    graph.toOriginalNodeId(source),
                    relType,
                    graph.toOriginalNodeId(target)
                );
                ops.relationshipSetProperty(
                    relId,
                    propertyType,
                    Values.doubleValue(property)
                );
            } catch (KernelException e) {
                ExceptionUtil.throwKernelException(e);
            }
            return true;
        };
    }

    @Override
    protected GraphLoader configureAlgoLoader(GraphLoader loader, ProcedureConfiguration config) {
        return loader;
    }

    @Override
    protected AlgorithmFactory<NeighborhoodSimilarity> algorithmFactory(ProcedureConfiguration config) {
        // TODO: Should check if we are writing or streaming, but how to do that in memrec?
        boolean computesSimilarityGraph = true;
        return new NeighborhoodSimilarityFactory(
            config(config),
            computesSimilarityGraph
        );
    }

    NeighborhoodSimilarity.Config config(ProcedureConfiguration procedureConfiguration) {
        int topK = validTopK(procedureConfiguration);
        int degreeCutoff = validDegreeCutoff(procedureConfiguration);
        double similarityCutoff = procedureConfiguration
            .getNumber(SIMILARITY_CUTOFF_KEY, SIMILARITY_CUTOFF_DEFAULT)
            .doubleValue();
        int top = procedureConfiguration.getInt(TOP_KEY, TOP_DEFAULT);
        int concurrency = procedureConfiguration.getConcurrency();
        int batchSize = procedureConfiguration.getBatchSize();
        return new NeighborhoodSimilarity.Config(similarityCutoff, degreeCutoff, top, topK, concurrency, batchSize);
    }

    private int validTopK(ProcedureConfiguration config) {
        int topK = config.getInt(TOP_K_KEY, TOP_K_DEFAULT);
        if (topK == 0) {
            throw new IllegalArgumentException("Must set non-zero topk value");
        }
        return topK;
    }

    private int validDegreeCutoff(ProcedureConfiguration config) {
        int degreeCutoff = config.getInt(DEGREE_CUTOFF_KEY, DEGREE_CUTOFF_DEFAULT);
        if (degreeCutoff < 1) {
            throw new IllegalArgumentException("Must set degree cutoff to 1 or greater");
        }
        return degreeCutoff;
    }

    public static class NeighborhoodSimilarityResult {
        public final long loadMillis;
        public final long computeMillis;
        public final long writeMillis;

        public final long nodesCompared;
        public final long relationships;
        public final boolean write;
        public final String writeRelationshipType;
        public final String writeProperty;

        NeighborhoodSimilarityResult(
            long loadMillis,
            long computeMillis,
            long writeMillis,
            long nodesCompared,
            long relationships,
            boolean write,
            String writeRelationshipType,
            String writeProperty
        ) {
            this.loadMillis = loadMillis;
            this.computeMillis = computeMillis;
            this.writeMillis = writeMillis;
            this.nodesCompared = nodesCompared;
            this.relationships = relationships;
            this.write = write;
            this.writeRelationshipType = writeRelationshipType;
            this.writeProperty = writeProperty;
        }
    }

    private static class WriteResultBuilder extends AbstractResultBuilder<NeighborhoodSimilarityResult> {

        private long nodesCompared = 0L;

        private String writeRelationshipType;

        WriteResultBuilder withNodesCompared(long nodesCompared) {
            this.nodesCompared = nodesCompared;
            return this;
        }

        WriteResultBuilder withWriteRelationshipType(String writeRelationshipType) {
            this.writeRelationshipType = writeRelationshipType;
            return this;
        }

        @Override
        public NeighborhoodSimilarityResult build() {
            return new NeighborhoodSimilarityResult(
                loadMillis,
                computeMillis,
                writeMillis,
                nodesCompared,
                relationshipCount,
                write,
                writeRelationshipType,
                writeProperty
            );
        }
    }

}
