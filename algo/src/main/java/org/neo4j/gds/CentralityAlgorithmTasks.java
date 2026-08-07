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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.articulationpoints.ArticulationPointsProgressTaskCreator;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.betweenness.BetweennessCentralityParameters;
import org.neo4j.gds.betweenness.BetweennessCentralityProgressTask;
import org.neo4j.gds.bridges.BridgeProgressTaskCreator;
import org.neo4j.gds.closeness.ClosenessCentralityTask;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.degree.DegreeCentralityProgressTask;
import org.neo4j.gds.harmonic.HarmonicCentralityProgressTask;
import org.neo4j.gds.hits.HitsConfig;
import org.neo4j.gds.hits.HitsProgressTrackerCreator;
import org.neo4j.gds.indexinverse.InverseRelationshipsParameters;
import org.neo4j.gds.indirectExposure.IndirectExposureConfig;
import org.neo4j.gds.influenceMaximization.CELFParameters;
import org.neo4j.gds.influenceMaximization.CELFProgressTask;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.pagerank.ArticleRankConfig;
import org.neo4j.gds.pagerank.EigenvectorConfig;
import org.neo4j.gds.pagerank.PageRankConfig;
import org.neo4j.gds.progress.tasks.Task;
import org.neo4j.gds.progress.tasks.Tasks;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.ArticleRank;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.EigenVector;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.PageRank;

public final class CentralityAlgorithmTasks {
    private CentralityAlgorithmTasks() {}

    public static Task articulationPoints(Graph graph, Concurrency concurrency) {
        return ArticulationPointsProgressTaskCreator.progressTask(concurrency, graph.nodeCount());
    }

    public static Task betweennessCentrality(Graph graph, BetweennessCentralityParameters parameters) {
        return BetweennessCentralityProgressTask.create(graph.nodeCount(), parameters);
    }

    public static Task CELF(Graph graph, CELFParameters parameters) {
        return CELFProgressTask.create(graph.nodeCount(), parameters);
    }

    public static Task bridges(Graph graph, Concurrency concurrency) {
        return BridgeProgressTaskCreator.progressTask(concurrency, graph.nodeCount());
    }

    public static Task closenessCentrality(Graph graph, Concurrency concurrency) {
        return ClosenessCentralityTask.create(concurrency, graph.nodeCount());
    }

    public static Task degreeCentrality(Graph graph, Concurrency concurrency) {
        return DegreeCentralityProgressTask.create(concurrency, graph.nodeCount());
    }

    public static Task harmonicCentrality(Concurrency concurrency) {
        return HarmonicCentralityProgressTask.create(concurrency);
    }

    public static Task articleRank(Graph graph, ArticleRankConfig configuration) {
        return Pregel.progressTask(graph.nodeCount(), configuration, MemoryRange.empty(), ArticleRank.asString());
    }

    public static Task eigenVector(Graph graph, EigenvectorConfig configuration) {
        return Pregel.progressTask(graph.nodeCount(), configuration, MemoryRange.empty(), EigenVector.asString());
    }

    public static Task pageRank(Graph graph, PageRankConfig configuration) {
        return Pregel.progressTask(graph.nodeCount(), configuration, MemoryRange.empty(), PageRank.asString());
    }

    public static Task hits(Graph graph, HitsConfig configuration) {
        return HitsProgressTrackerCreator.progressTask(
            configuration.concurrency(),
            graph.nodeCount(),
            configuration.maxIterations()
        );
    }

    public static Task hits(
        GraphStore graphStore,
        HitsConfig configuration,
        InverseRelationshipsParameters inverseRelationshipsParameters
    ) {
        return HitsProgressTrackerCreator.progressTaskWithInvertedIndex(
            graphStore.nodeCount(),
            configuration.maxIterations(),
            inverseRelationshipsParameters
        );
    }

    public static Task indirectExposure(Graph graph, IndirectExposureConfig configuration) {
        return Tasks.task(
            AlgorithmLabel.IndirectExposure.asString(),
            configuration.concurrency(), Tasks.leaf("TotalTransfers", configuration.concurrency(), graph.nodeCount()),
            Pregel.progressTask(graph.nodeCount(), configuration, MemoryRange.empty(), "ExposurePropagation")
        );
    }

}
