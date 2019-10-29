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
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.jaccard.NeighborhoodSimilarity;
import org.neo4j.graphalgo.impl.jaccard.NeighborhoodSimilarityFactory;
import org.neo4j.graphalgo.impl.jaccard.SimilarityGraphResult;
import org.neo4j.graphalgo.impl.jaccard.SimilarityResult;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.graphdb.Direction.OUTGOING;

public class NeighborhoodSimilarityProc extends BaseAlgoProc<NeighborhoodSimilarity> {

    private static final String SIMILARITY_CUTOFF_KEY = "similarityCutoff";
    private static final double SIMILARITY_CUTOFF_DEFAULT = -1.0;

    private static final String DEGREE_CUTOFF_KEY = "degreeCutOff";
    private static final int DEGREE_CUTOFF_DEFAULT = 0;

    private static final String TOP_KEY = "top";
    private static final int TOP_DEFAULT = 0;

    private static final String TOP_K_KEY = "topK";
    private static final int TOP_K_DEFAULT = 0;

    private static final String WRITE_RELATIONSHIP_TYPE_KEY = "writeRelationshipType";
    private static final String WRITE_RELATIONSHIP_TYPE_DEFAULT = "SIMILAR";

    private static final String WRITE_PROPERTY_KEY = "writeProperty";
    private static final String WRITE_PROPERTY_DEFAULT = "score";

    private static final Direction COMPUTE_DIRECTION_DEFAULT = OUTGOING;

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    @Procedure(name = "algo.neighborhoodSimilarity.stream", mode = Mode.READ)
    @Description("CALL algo.neighborhoodSimilarity.stream(" +
                 "labelPredicate, relationshipPredicate, {" +
                 "  similarityCutoff: -1.0, degreeCutoff: 0, top: 0, topK: 0," +
                 "  graph: 'graph', direction: 'OUTGOING', concurrency: 4, readConcurrency: 4" +
                 "}) " +
                 "YIELD node1, node2, similarity - computes neighborhood similarities based on the Jaccard index")
    public Stream<SimilarityResult> stream(
        @Name(value = "label", defaultValue = "") String label,
        @Name(value = "relationship", defaultValue = "") String relationshipType,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {
        AllocationTracker tracker = AllocationTracker.create();
        ProcedureConfiguration configuration = newConfig(label, relationshipType, config);
        final Graph graph = loadGraph(configuration, tracker);

        if (graph.isEmpty()) {
            graph.release();
            return Stream.empty();
        }

        NeighborhoodSimilarity neighborhoodSimilarity = newAlgorithm(graph, configuration, tracker);

        Direction direction = configuration.getDirection(COMPUTE_DIRECTION_DEFAULT);
        return neighborhoodSimilarity.computeToStream(direction);
    }

    @Procedure(name = "algo.neighborhoodSimilarity", mode = Mode.WRITE)
    @Description("CALL algo.neighborhoodSimilarity(" +
                 "labelPredicate, relationshipPredicate, {" +
                 "  similarityCutoff: -1.0, degreeCutoff: 0, top: 0, topK: 0," +
                 "  graph: 'graph', direction: 'OUTGOING', concurrency: 4, readConcurrency: 4," +
                 "  write: 'true', writeRelationshipType: 'SIMILAR_TO', writeProperty: 'similarity', writeConcurrency: 4" +
                 "}) " +
                 "YIELD nodesCompared, relationships, write, writeRelationshipType, writeProperty - computes neighborhood similarities based on the Jaccard index")
    public Stream<WriteResult> write(
        @Name(value = "label", defaultValue = "") String label,
        @Name(value = "relationship", defaultValue = "") String relationshipType,
        @Name(value = "config", defaultValue = "{}") Map<String, Object> config
    ) {
        AllocationTracker tracker = AllocationTracker.create();
        ProcedureConfiguration configuration = newConfig(label, relationshipType, config);
        final Graph graph = loadGraph(configuration, tracker);

        String writeRelationshipType = configuration.get(WRITE_RELATIONSHIP_TYPE_KEY, WRITE_RELATIONSHIP_TYPE_DEFAULT);
        String writeProperty = configuration.get(WRITE_PROPERTY_KEY, WRITE_PROPERTY_DEFAULT);

        WriteResult writeResult = new WriteResult(
            configuration.isWriteFlag(),
            writeRelationshipType,
            writeProperty
        );

        if (graph.isEmpty()) {
            graph.release();
            return Stream.of(writeResult);
        }

        NeighborhoodSimilarity neighborhoodSimilarity = newAlgorithm(graph, configuration, tracker);

        Direction direction = configuration.getDirection(COMPUTE_DIRECTION_DEFAULT);
        SimilarityGraphResult similarityGraphResult = neighborhoodSimilarity.computeToGraph(direction);
        Graph similarityGraph = similarityGraphResult.similarityGraph();
        writeResult.nodesCompared = similarityGraphResult.comparedNodes();
        writeResult.relationships = similarityGraph.relationshipCount();

        if (configuration.isWriteFlag() && similarityGraph.relationshipCount() > 0) {
            // write
        }

        return Stream.of(writeResult);
    }

    @Override
    protected GraphLoader configureAlgoLoader(GraphLoader loader, ProcedureConfiguration config) {
        return loader;
    }

    @Override
    protected AlgorithmFactory<NeighborhoodSimilarity> algorithmFactory(ProcedureConfiguration config) {
        return new NeighborhoodSimilarityFactory(new NeighborhoodSimilarity.Config(
            config.get(SIMILARITY_CUTOFF_KEY, SIMILARITY_CUTOFF_DEFAULT),
            config.get(DEGREE_CUTOFF_KEY, DEGREE_CUTOFF_DEFAULT),
            config.get(TOP_KEY, TOP_DEFAULT),
            config.get(TOP_K_KEY, TOP_K_DEFAULT),
            config.getConcurrency(),
            config.getBatchSize()
        ));
    }

    private static class WriteResult {
        public long nodesCompared;
        public long relationships;
        public boolean write;
        public String writeRelationshipType;
        public String writeProperty;

        WriteResult(boolean write, String writeRelationshipType, String writeProperty) {
            this(0L, 0L, write, writeRelationshipType, writeProperty);
        }

        WriteResult(
            long nodesCompared,
            long relationships,
            boolean write,
            String writeRelationshipType,
            String writeProperty
        ) {
            this.nodesCompared = nodesCompared;
            this.relationships = relationships;
            this.write = write;
            this.writeRelationshipType = writeRelationshipType;
            this.writeProperty = writeProperty;
        }
    }

}
