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
package org.neo4j.gds.algorithms.community;

import org.neo4j.gds.algorithms.AlgorithmComputationResult;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutResult;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutBaseConfig;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.conductance.ConductanceResult;
import org.neo4j.gds.conductance.ConductanceStreamConfig;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.k1coloring.K1ColoringStreamConfig;
import org.neo4j.gds.kcore.KCoreDecompositionBaseConfig;
import org.neo4j.gds.kcore.KCoreDecompositionResult;
import org.neo4j.gds.kmeans.KmeansBaseConfig;
import org.neo4j.gds.kmeans.KmeansResult;
import org.neo4j.gds.labelpropagation.LabelPropagationBaseConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationResult;
import org.neo4j.gds.leiden.LeidenBaseConfig;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.louvain.LouvainBaseConfig;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.modularity.ModularityBaseConfig;
import org.neo4j.gds.modularity.ModularityResult;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationResult;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationStreamConfig;
import org.neo4j.gds.scc.SccBaseConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientResult;
import org.neo4j.gds.triangle.LocalClusteringCoefficientStreamConfig;
import org.neo4j.gds.triangle.TriangleCountBaseConfig;
import org.neo4j.gds.triangle.TriangleCountResult;
import org.neo4j.gds.wcc.WccBaseConfig;

public class CommunityAlgorithmsStreamBusinessFacade {
    private final CommunityAlgorithmsFacade communityAlgorithmsFacade;

    public CommunityAlgorithmsStreamBusinessFacade(CommunityAlgorithmsFacade communityAlgorithmsFacade) {
        this.communityAlgorithmsFacade = communityAlgorithmsFacade;
    }

    public StreamComputationResult<DisjointSetStruct> wcc(
        String graphName,
        WccBaseConfig config
    ) {
        var result = this.communityAlgorithmsFacade.wcc(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }


    public StreamComputationResult<KCoreDecompositionResult> kCore(
        String graphName,
        KCoreDecompositionBaseConfig config
    ) {

        var result = this.communityAlgorithmsFacade.kCore(
            graphName,
            config
        );
        
        return createStreamComputationResult(result);

    }

    public StreamComputationResult<LouvainResult> louvain(
        String graphName,
        LouvainBaseConfig config
    ) {

        var result = this.communityAlgorithmsFacade.louvain(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    public StreamComputationResult<LeidenResult> leiden(
        String graphName,
        LeidenBaseConfig config
    ) {

        var result = this.communityAlgorithmsFacade.leiden(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    public StreamComputationResult<HugeLongArray> scc(
        String graphName,
        SccBaseConfig config
    ) {

        var result = this.communityAlgorithmsFacade.scc(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    public StreamComputationResult<TriangleCountResult> triangleCount(
        String graphName,
        TriangleCountBaseConfig config
    ) {

        var result = this.communityAlgorithmsFacade.triangleCount(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    public StreamComputationResult<ModularityResult> modularity(
        String graphName,
        ModularityBaseConfig config
    ) {

        var result = this.communityAlgorithmsFacade.modularity(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }


    public StreamComputationResult<LabelPropagationResult> labelPropagation(
        String graphName,
        LabelPropagationBaseConfig configuration
    ) {

        var result = this.communityAlgorithmsFacade.labelPropagation(
            graphName,
            configuration
        );

        return createStreamComputationResult(result);
    }

    public StreamComputationResult<KmeansResult> kmeans(
        String graphName,
        KmeansBaseConfig config
    ) {

        var result = this.communityAlgorithmsFacade.kmeans(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    public StreamComputationResult<LocalClusteringCoefficientResult> localClusteringCoefficient(
        String graphName,
        LocalClusteringCoefficientStreamConfig config
    ) {
        var result = this.communityAlgorithmsFacade.localClusteringCoefficient(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    public StreamComputationResult<K1ColoringResult> k1coloring(
        String graphName,
        K1ColoringStreamConfig config
    ) {

        var result = this.communityAlgorithmsFacade.k1Coloring(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    public StreamComputationResult<ConductanceResult> conductance(
        String graphName,
        ConductanceStreamConfig config
    ) {

        var result = this.communityAlgorithmsFacade.conductance(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }


    public StreamComputationResult<ApproxMaxKCutResult> approxMaxKCut(
        String graphName,
        ApproxMaxKCutBaseConfig config
    ) {

        var result = this.communityAlgorithmsFacade.approxMaxKCut(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    public StreamComputationResult<ModularityOptimizationResult> modularityOptimization(
        String graphName,
        ModularityOptimizationStreamConfig config
    ) {
        var result = this.communityAlgorithmsFacade.modularityOptimization(
            graphName,
            config
        );

        return createStreamComputationResult(result);
    }

    private <RESULT> StreamComputationResult<RESULT> createStreamComputationResult(AlgorithmComputationResult<RESULT> result) {

        return StreamComputationResult.of(
            result.result(),
            result.graph()
        );

    }

}
