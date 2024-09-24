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
package org.neo4j.gds.procedures.algorithms.centrality;

import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.bridges.Bridge;
import org.neo4j.gds.pagerank.ArticleRankMutateConfig;
import org.neo4j.gds.pagerank.EigenvectorMutateConfig;
import org.neo4j.gds.pagerank.PageRankMutateConfig;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.ArticulationPointsMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.BetaClosenessCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.BetweennessCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.CelfMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.ClosenessCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.DegreeCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.HarmonicCentralityMutateStub;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.PageRankMutateStub;

import java.util.Map;
import java.util.stream.Stream;

public interface CentralityProcedureFacade {
    Stream<AlphaHarmonicStreamResult> alphaHarmonicCentralityStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<AlphaHarmonicWriteResult> alphaHarmonicCentralityWrite(
        String graphName,
        Map<String, Object> configuration
    );

    PageRankMutateStub<ArticleRankMutateConfig> articleRankMutateStub();

    Stream<PageRankStatsResult> articleRankStats(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> articleRankStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<CentralityStreamResult> articleRankStream(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> articleRankStreamEstimate(
        Object graphNameOrConfiguration, Map<String, Object> algorithmConfiguration
    );

    Stream<PageRankWriteResult> articleRankWrite(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> articleRankWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    BetaClosenessCentralityMutateStub betaClosenessCentralityMutateStub();

    Stream<BetaClosenessCentralityWriteResult> betaClosenessCentralityWrite(
        String graphName,
        Map<String, Object> configuration
    );

    BetweennessCentralityMutateStub betweennessCentralityMutateStub();

    Stream<CentralityStatsResult> betweennessCentralityStats(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> betweennessCentralityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<CentralityStreamResult> betweennessCentralityStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> betweennessCentralityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<CentralityWriteResult> betweennessCentralityWrite(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> betweennessCentralityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<ArticulationPoint> articulationPointsStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> articulationPointsStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    ArticulationPointsMutateStub articulationPointsMutateStub();

    Stream<MemoryEstimateResult> articulationPointsMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<ArticulationPointsStatsResult> articulationPointsStats(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> articulationPointsStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<ArticulationPointsWriteResult> articulationPointsWrite(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> articulationPointsWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<Bridge> bridgesStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> bridgesStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    CelfMutateStub celfMutateStub();

    Stream<CELFStatsResult> celfStats(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> celfStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<CELFStreamResult> celfStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> celfStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<CELFWriteResult> celfWrite(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> celfWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    ClosenessCentralityMutateStub closenessCentralityMutateStub();

    Stream<CentralityStatsResult> closenessCentralityStats(String graphName, Map<String, Object> configuration);

    Stream<CentralityStreamResult> closenessCentralityStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<CentralityWriteResult> closenessCentralityWrite(String graphName, Map<String, Object> configuration);

    DegreeCentralityMutateStub degreeCentralityMutateStub();

    Stream<CentralityStatsResult> degreeCentralityStats(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> degreeCentralityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<CentralityStreamResult> degreeCentralityStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> degreeCentralityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<CentralityWriteResult> degreeCentralityWrite(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> degreeCentralityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    PageRankMutateStub<EigenvectorMutateConfig> eigenVectorMutateStub();

    Stream<PageRankStatsResult> eigenvectorStats(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> eigenvectorStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<CentralityStreamResult> eigenvectorStream(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> eigenvectorStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<PageRankWriteResult> eigenvectorWrite(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> eigenvectorWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    HarmonicCentralityMutateStub harmonicCentralityMutateStub();

    Stream<CentralityStatsResult> harmonicCentralityStats(String graphName, Map<String, Object> configuration);

    Stream<CentralityStreamResult> harmonicCentralityStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<CentralityWriteResult> harmonicCentralityWrite(String graphName, Map<String, Object> configuration);

    PageRankMutateStub<PageRankMutateConfig> pageRankMutateStub();

    Stream<PageRankStatsResult> pageRankStats(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> pageRankStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<CentralityStreamResult> pageRankStream(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> pageRankStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<PageRankWriteResult> pageRankWrite(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> pageRankWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );
}
