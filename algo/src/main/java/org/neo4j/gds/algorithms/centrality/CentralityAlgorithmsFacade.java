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
package org.neo4j.gds.algorithms.centrality;

import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.runner.AlgorithmRunner;
import org.neo4j.gds.betweenness.BetweennessCentralityBaseConfig;
import org.neo4j.gds.betweenness.BetweennessCentralityFactory;
import org.neo4j.gds.betweenness.BetwennessCentralityResult;
import org.neo4j.gds.closeness.ClosenessCentralityAlgorithmFactory;
import org.neo4j.gds.closeness.ClosenessCentralityBaseConfig;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.degree.DegreeCentralityConfig;
import org.neo4j.gds.degree.DegreeCentralityFactory;
import org.neo4j.gds.degree.DegreeCentralityResult;

import java.util.Optional;

public class CentralityAlgorithmsFacade {

    private final AlgorithmRunner algorithmRunner;

    public CentralityAlgorithmsFacade(AlgorithmRunner algorithmRunner) {
        this.algorithmRunner = algorithmRunner;
    }

    AlgorithmComputationResult<BetwennessCentralityResult> betweennessCentrality(
        String graphName,
        BetweennessCentralityBaseConfig config
    ) {
        return algorithmRunner.run(
            graphName,
            config,
            config.relationshipWeightProperty(),
            new BetweennessCentralityFactory<>()
        );
    }

    AlgorithmComputationResult<DegreeCentralityResult> degreeCentrality(
        String graphName,
        DegreeCentralityConfig config

    ) {
        return algorithmRunner.run(
            graphName,
            config,
            config.relationshipWeightProperty(),
            new DegreeCentralityFactory<>()

        );
    }

    AlgorithmComputationResult<ClosenessCentralityResult> closenessCentrality(
        String graphName,
        ClosenessCentralityBaseConfig config

    ) {
        return algorithmRunner.run(
            graphName,
            config,
            Optional.empty(),
            new ClosenessCentralityAlgorithmFactory<>()

        );
    }



}
