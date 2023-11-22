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

import com.carrotsearch.hppc.LongDoubleScatterMap;
import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.betweenness.BetweennessCentralityStreamConfig;
import org.neo4j.gds.betweenness.BetwennessCentralityResult;
import org.neo4j.gds.closeness.ClosenessCentralityResult;
import org.neo4j.gds.closeness.ClosenessCentralityStreamConfig;
import org.neo4j.gds.degree.DegreeCentralityResult;
import org.neo4j.gds.degree.DegreeCentralityStreamConfig;
import org.neo4j.gds.harmonic.HarmonicCentralityStreamConfig;
import org.neo4j.gds.harmonic.HarmonicResult;
import org.neo4j.gds.pagerank.PageRankConfig;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.influenceMaximization.InfluenceMaximizationStreamConfig;

public class CentralityAlgorithmsStreamBusinessFacade {

    private final CentralityAlgorithmsFacade centralityAlgorithmsFacade;

    public CentralityAlgorithmsStreamBusinessFacade(CentralityAlgorithmsFacade centralityAlgorithmsFacade) {
        this.centralityAlgorithmsFacade = centralityAlgorithmsFacade;
    }

    public StreamComputationResult<BetwennessCentralityResult> betweennessCentrality(
        String graphName,
        BetweennessCentralityStreamConfig config
    ) {

        var result = centralityAlgorithmsFacade.betweennessCentrality(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    public StreamComputationResult<DegreeCentralityResult> degreeCentrality(
        String graphName,
        DegreeCentralityStreamConfig config
    ) {

        var result = centralityAlgorithmsFacade.degreeCentrality(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    public StreamComputationResult<ClosenessCentralityResult> closenessCentrality(
        String graphName,
        ClosenessCentralityStreamConfig config
    ) {

        var result = centralityAlgorithmsFacade.closenessCentrality(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    public StreamComputationResult<HarmonicResult> harmonicCentrality(
        String graphName,
        HarmonicCentralityStreamConfig config
    ) {

        var result = centralityAlgorithmsFacade.harmonicCentrality(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }
    public StreamComputationResult<PageRankResult> pageRank(
        String graphName,
        PageRankConfig config
    ) {

        var result = this.centralityAlgorithmsFacade.pageRank(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    public StreamComputationResult<LongDoubleScatterMap> celf(
        String graphName,
        InfluenceMaximizationStreamConfig config
    ) {

        var result = centralityAlgorithmsFacade.CELF(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    public StreamComputationResult<PageRankResult> articleRank(
        String graphName,
        PageRankConfig config
    ) {

        var result = this.centralityAlgorithmsFacade.articleRank(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    public StreamComputationResult<PageRankResult> eigenvector(
        String graphName,
        PageRankConfig config
    ) {

        var result = this.centralityAlgorithmsFacade.eigenvector(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    // ################################################################################################################


    // FIXME: the following method is duplicate, find a good place for it.
    private <RESULT> StreamComputationResult<RESULT> createStreamComputationResult(
        AlgorithmComputationResult<RESULT> result
    ) {
        return StreamComputationResult.of(
            result.result(),
            result.graph()
        );
    }
    //FIXME: here ends the fixme-block

}
