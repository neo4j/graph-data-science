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
import org.neo4j.gds.procedures.algorithms.centrality.mutate.PushbackCentralityMutateProcedureFacade;
import org.neo4j.gds.procedures.algorithms.centrality.stats.PushbackCentralityStatsProcedureFacade;
import org.neo4j.gds.procedures.algorithms.centrality.stream.PushbackCentralityStreamProcedureFacade;
import org.neo4j.gds.procedures.algorithms.centrality.stubs.CentralityStubs;

import java.util.Map;
import java.util.stream.Stream;

public class PushbackCentralityProcedureFacade implements CentralityProcedureFacade{

    private final PushbackCentralityStreamProcedureFacade streamProcedureFacade;
    private final PushbackCentralityStatsProcedureFacade statsProcedureFacade;
    private final PushbackCentralityMutateProcedureFacade mutateProcedureFacade;


    public PushbackCentralityProcedureFacade(
        PushbackCentralityStreamProcedureFacade streamProcedureFacade,
        PushbackCentralityStatsProcedureFacade statsProcedureFacade,
        PushbackCentralityMutateProcedureFacade mutateProcedureFacade
    ) {
        this.streamProcedureFacade = streamProcedureFacade;
        this.statsProcedureFacade = statsProcedureFacade;
        this.mutateProcedureFacade = mutateProcedureFacade;
    }

    @Override
    public CentralityStubs centralityStubs() {
        return null;
    }

    @Override
    public Stream<AlphaHarmonicStreamResult> alphaHarmonicCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamProcedureFacade.alphaHarmonic(graphName,configuration);
    }

    @Override
    public Stream<AlphaHarmonicWriteResult> alphaHarmonicCentralityWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PageRankMutateResult> articleRankMutate(String graphName, Map<String, Object> configuration) {
        return mutateProcedureFacade.articleRank(
            graphName,
            configuration
        );
    }

    @Override
    public Stream<MemoryEstimateResult> articleRankMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PageRankStatsResult> articleRankStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.articleRank(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> articleRankStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CentralityStreamResult> articleRankStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.articleRank(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> articleRankStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PageRankWriteResult> articleRankWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> articleRankWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<BetaClosenessCentralityMutateResult> betaClosenessCentralityMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return mutateProcedureFacade.betaCloseness(graphName,configuration);
    }

    @Override
    public Stream<BetaClosenessCentralityWriteResult> betaClosenessCentralityWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CentralityMutateResult> betweennessCentralityMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> betweennessCentralityMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CentralityStatsResult> betweennessCentralityStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        return statsProcedureFacade.betweenness(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> betweennessCentralityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CentralityStreamResult> betweennessCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamProcedureFacade.betweenness(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> betweennessCentralityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CentralityWriteResult> betweennessCentralityWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> betweennessCentralityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<ArticulationPointStreamResult> articulationPointsStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamProcedureFacade.articulationPoints(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> articulationPointsStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<ArticulationPointsMutateResult> articulationPointsMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return mutateProcedureFacade.articulationPoints(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> articulationPointsMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<ArticulationPointsStatsResult> articulationPointsStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        return statsProcedureFacade.articulationPoints(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> articulationPointsStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<ArticulationPointsWriteResult> articulationPointsWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> articulationPointsWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<BridgesStreamResult> bridgesStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.bridges(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> bridgesStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CELFMutateResult> celfMutate(String graphName, Map<String, Object> configuration) {
        return mutateProcedureFacade.celf(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> celfMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CELFStatsResult> celfStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.celf(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> celfStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CELFStreamResult> celfStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.celf(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> celfStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CELFWriteResult> celfWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> celfWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CentralityMutateResult> closenessCentralityMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return mutateProcedureFacade.closeness(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> closenessCentralityMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CentralityStatsResult> closenessCentralityStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.closeness(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> closenessCentralityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CentralityStreamResult> closenessCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamProcedureFacade.closeness(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> closenessCentralityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CentralityWriteResult> closenessCentralityWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> closenessCentralityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CentralityMutateResult> degreeCentralityMutate(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> degreeCentralityMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CentralityStatsResult> degreeCentralityStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.degree(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> degreeCentralityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CentralityStreamResult> degreeCentralityStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.degree(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> degreeCentralityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CentralityWriteResult> degreeCentralityWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> degreeCentralityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PageRankMutateResult> eigenvectorMutate(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> eigenvectorMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PageRankStatsResult> eigenvectorStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.eigenVector(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> eigenvectorStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CentralityStreamResult> eigenvectorStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.eigenVector(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> eigenvectorStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PageRankWriteResult> eigenvectorWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> eigenvectorWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CentralityMutateResult> harmonicCentralityMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> harmonicCentralityMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CentralityStatsResult> harmonicCentralityStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.harmonic(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> harmonicCentralityStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CentralityStreamResult> harmonicCentralityStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamProcedureFacade.harmonic(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> harmonicCentralityStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CentralityWriteResult> harmonicCentralityWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> harmonicCentralityWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PageRankMutateResult> pageRankMutate(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> pageRankMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PageRankStatsResult> pageRankStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.pageRank(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> pageRankStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<CentralityStreamResult> pageRankStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.pageRank(graphName,configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> pageRankStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PageRankWriteResult> pageRankWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> pageRankWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<HitsStreamResult> hitsStream(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> hitsStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<HitsStatsResult> hitsStats(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> hitsStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<HitsWriteResult> hitsWrite(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> hitsWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<HitsMutateResult> hitsMutate(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> hitsMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }
}
