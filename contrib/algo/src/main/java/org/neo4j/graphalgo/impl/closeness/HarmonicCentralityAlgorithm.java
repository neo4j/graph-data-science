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
package org.neo4j.graphalgo.impl.closeness;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.Exporter;

import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

/**
 *
 * @author mknblch
 */
public interface HarmonicCentralityAlgorithm {

    /**
     * compute centrality
     */
    HarmonicCentralityAlgorithm compute();

    /**
     * return result stream with nodeId-closeness value
     * @return
     */
    Stream<Result> resultStream();

    HarmonicCentralityAlgorithm withProgressLogger(ProgressLogger wrap);

    HarmonicCentralityAlgorithm withTerminationFlag(TerminationFlag wrap);

    HarmonicCentralityAlgorithm release();

    void export(final String propertyName, final Exporter exporter);

    static HarmonicCentralityAlgorithm instance(Graph graph, AllocationTracker tracker, ExecutorService pool, int concurrency) {
        if (graph instanceof HugeGraph) {
            return new HugeHarmonicCentrality((HugeGraph) graph,
                    tracker,
                    concurrency,
                    pool);
        }
        return new HarmonicCentrality(graph,
                concurrency, pool);
    }

    /**
     * Result class used for streaming
     */
    final class Result {

        public final long nodeId;
        public final double centrality;

        public Result(long nodeId, double centrality) {
            this.nodeId = nodeId;
            this.centrality = centrality;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "nodeId=" + nodeId +
                    ", centrality=" + centrality +
                    '}';
        }
    }
}
