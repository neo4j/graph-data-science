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
package org.neo4j.gds.applications.algorithms.centrality;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.betweenness.BetweennessCentrality;
import org.neo4j.gds.betweenness.BetweennessCentralityBaseConfig;
import org.neo4j.gds.betweenness.BetwennessCentralityResult;
import org.neo4j.gds.betweenness.ForwardTraverser;
import org.neo4j.gds.betweenness.FullSelectionStrategy;
import org.neo4j.gds.betweenness.RandomDegreeSelectionStrategy;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import static org.neo4j.gds.applications.algorithms.centrality.AlgorithmLabels.BETWEENNESS_CENTRALITY;

public class CentralityAlgorithms {
    private final ProgressTrackerCreator progressTrackerCreator;

    public CentralityAlgorithms(ProgressTrackerCreator progressTrackerCreator) {
        this.progressTrackerCreator = progressTrackerCreator;
    }

    BetwennessCentralityResult betweennessCentrality(Graph graph, BetweennessCentralityBaseConfig configuration) {
        var parameters = configuration.toParameters();

        var samplingSize = parameters.samplingSize();
        var samplingSeed = parameters.samplingSeed();

        var selectionStrategy = samplingSize.isPresent() && samplingSize.get() < graph.nodeCount()
            ? new RandomDegreeSelectionStrategy(samplingSize.get(), samplingSeed)
            : new FullSelectionStrategy();

        var traverserFactory = parameters.hasRelationshipWeightProperty()
            ? ForwardTraverser.Factory.weighted()
            : ForwardTraverser.Factory.unweighted();

        var task = Tasks.leaf(BETWEENNESS_CENTRALITY, samplingSize.orElse(graph.nodeCount()));
        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = new BetweennessCentrality(
            graph,
            selectionStrategy,
            traverserFactory,
            DefaultPool.INSTANCE,
            parameters.concurrency(),
            progressTracker
        );

        return algorithm.compute();
    }
}
