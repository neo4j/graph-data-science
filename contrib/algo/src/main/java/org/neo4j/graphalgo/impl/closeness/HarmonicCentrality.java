/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.impl.closeness;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.Algorithm;
import org.neo4j.graphalgo.impl.msbfs.BfsConsumer;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Harmonic Centrality Algorithm.
 *
 * Harmonic centrality (also known as valued centrality) is a variant of closeness centrality,
 * that was invented to solve the problem the original formula had when dealing with unconnected graphs.
 *
 * @author mknblch
 */
public class HarmonicCentrality extends Algorithm<HarmonicCentrality> implements HarmonicCentralityAlgorithm {

    private Graph graph;
    private final AtomicDoubleArray inverseFarness;
    private final int concurrency;
    private ExecutorService executorService;
    private final int nodeCount;

    public HarmonicCentrality(Graph graph, int concurrency, ExecutorService executorService) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.concurrency = concurrency;
        this.executorService = executorService;
        inverseFarness = new AtomicDoubleArray(nodeCount);
    }

    /**
     * compute centrality using MSBFS
     * @return
     */
    public HarmonicCentrality compute() {
        final ProgressLogger progressLogger = getProgressLogger();
        final BfsConsumer consumer = (nodeId, depth, sourceNodeIds) -> {
            inverseFarness.add(nodeId, sourceNodeIds.size() * (1.0 / depth));
            progressLogger.logProgress((double) nodeId / (nodeCount - 1));
        };
        new MultiSourceBFS(graph, graph, Direction.BOTH, consumer)
                .run(concurrency, executorService);
        return this;
    }

    /**
     * result stream of nodeId to closeness value
     * @return
     */
    public Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(nodeId -> new Result(
                        graph.toOriginalNodeId(nodeId),
                        inverseFarness.get(nodeId) / (double) (nodeCount - 1)));
    }

    /**
     * export results
     * @param propertyName
     * @param exporter
     */
    public void export(final String propertyName, final Exporter exporter) {
        exporter.write(
                propertyName,
                inverseFarness,
                (PropertyTranslator.OfDouble<AtomicDoubleArray>)
                        (data, nodeId) -> data.get((int) nodeId) / (double) (nodeCount - 1));
    }

    @Override
    public HarmonicCentrality me() {
        return this;
    }

    /**
     * release inner data structures
     * @return
     */
    @Override
    public HarmonicCentrality release() {
        graph = null;
        executorService = null;
        return this;
    }
}
