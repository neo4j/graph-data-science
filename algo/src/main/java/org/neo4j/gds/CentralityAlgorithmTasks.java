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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.articulationpoints.ArticulationPointsProgressTaskCreator;
import org.neo4j.gds.beta.pregel.Pregel;
import org.neo4j.gds.betweenness.BetweennessCentralityParameters;
import org.neo4j.gds.betweenness.BetweennessCentralityProgressTask;
import org.neo4j.gds.bridges.BridgeProgressTaskCreator;
import org.neo4j.gds.closeness.ClosenessCentralityTask;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.degree.DegreeCentralityProgressTask;
import org.neo4j.gds.harmonic.HarmonicCentralityProgressTask;
import org.neo4j.gds.hits.HitsConfig;
import org.neo4j.gds.hits.HitsProgressTrackerCreator;
import org.neo4j.gds.influenceMaximization.CELFParameters;
import org.neo4j.gds.influenceMaximization.CELFProgressTask;
import org.neo4j.gds.pagerank.ArticleRankConfig;
import org.neo4j.gds.pagerank.EigenvectorConfig;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.ArticleRank;

public final class CentralityAlgorithmTasks {

    public Task articulationPoints(Graph graph) {
        return ArticulationPointsProgressTaskCreator.progressTask(graph.nodeCount());
    }

    public Task betweennessCentrality(Graph graph, BetweennessCentralityParameters parameters){
        return BetweennessCentralityProgressTask.create(graph.nodeCount(),parameters);
    }

    public Task CELF(Graph graph, CELFParameters parameters){
        return CELFProgressTask.create(graph.nodeCount(), parameters);
    }

    public Task bridges(Graph graph){
        return BridgeProgressTaskCreator.progressTask(graph.nodeCount());
    }

    public Task closenessCentrality(Graph graph){
        return ClosenessCentralityTask.create(graph.nodeCount());
    }

    public Task degreeCentrality(Graph graph){
        return DegreeCentralityProgressTask.create(graph.nodeCount());
    }

    public Task harmonicCentrality(){ return HarmonicCentralityProgressTask.create(); }

    public  Task articleRank(Graph graph, ArticleRankConfig configuration){
        return Pregel.progressTask(graph, configuration, ArticleRank.asString());
    }

    public  Task eigenVector(Graph graph, EigenvectorConfig configuration){
        return Pregel.progressTask(graph, configuration, ArticleRank.asString());
    }

    public Task hits(Graph graph, HitsConfig configuration){
        return HitsProgressTrackerCreator.progressTask(
            graph.nodeCount(),
            configuration.maxIterations(),
            AlgorithmLabel.HITS.asString()
        );
    }

}
