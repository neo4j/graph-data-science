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
package org.neo4j.gds.applications.algorithms.community;

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutMemoryEstimateDefinition;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutBaseConfig;
import org.neo4j.gds.config.SeedConfig;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.k1coloring.K1ColoringBaseConfig;
import org.neo4j.gds.k1coloring.K1ColoringMemoryEstimateDefinition;
import org.neo4j.gds.kcore.KCoreDecompositionBaseConfig;
import org.neo4j.gds.kcore.KCoreDecompositionMemoryEstimateDefinition;
import org.neo4j.gds.kmeans.KmeansBaseConfig;
import org.neo4j.gds.kmeans.KmeansMemoryEstimateDefinition;
import org.neo4j.gds.labelpropagation.LabelPropagationBaseConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationMemoryEstimateDefinition;
import org.neo4j.gds.leiden.LeidenBaseConfig;
import org.neo4j.gds.leiden.LeidenMemoryEstimateDefinition;
import org.neo4j.gds.louvain.LouvainBaseConfig;
import org.neo4j.gds.louvain.LouvainMemoryEstimateDefinition;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.modularity.ModularityBaseConfig;
import org.neo4j.gds.modularity.ModularityCalculatorMemoryEstimateDefinition;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationBaseConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationMemoryEstimateDefinition;
import org.neo4j.gds.scc.SccBaseConfig;
import org.neo4j.gds.scc.SccMemoryEstimateDefinition;
import org.neo4j.gds.triangle.IntersectingTriangleCountMemoryEstimateDefinition;
import org.neo4j.gds.triangle.LocalClusteringCoefficientBaseConfig;
import org.neo4j.gds.triangle.LocalClusteringCoefficientMemoryEstimateDefinition;
import org.neo4j.gds.triangle.TriangleCountBaseConfig;
import org.neo4j.gds.wcc.WccBaseConfig;
import org.neo4j.gds.wcc.WccMemoryEstimateDefinition;

public class CommunityAlgorithmsEstimationModeBusinessFacade {
    private final AlgorithmEstimationTemplate algorithmEstimationTemplate;

    public CommunityAlgorithmsEstimationModeBusinessFacade(AlgorithmEstimationTemplate algorithmEstimationTemplate) {
        this.algorithmEstimationTemplate = algorithmEstimationTemplate;
    }

    public MemoryEstimation approximateMaximumKCut(ApproxMaxKCutBaseConfig configuration) {
        return new ApproxMaxKCutMemoryEstimateDefinition(configuration.toMemoryEstimationParameters()).memoryEstimation();
    }

    public MemoryEstimateResult approximateMaximumKCut(
        ApproxMaxKCutBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = approximateMaximumKCut(configuration);

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    MemoryEstimation conductance() {
        throw new MemoryEstimationNotImplementedException();
    }

    public MemoryEstimation k1Coloring() {
        return new K1ColoringMemoryEstimateDefinition().memoryEstimation();
    }

    public MemoryEstimateResult k1Coloring(K1ColoringBaseConfig configuration, Object graphNameOrConfiguration) {
        var memoryEstimation = k1Coloring();

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    public MemoryEstimation kCore() {
        return new KCoreDecompositionMemoryEstimateDefinition().memoryEstimation();
    }

    public MemoryEstimateResult kCore(KCoreDecompositionBaseConfig configuration, Object graphNameOrConfiguration) {
        var memoryEstimation = kCore();

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    public MemoryEstimation kMeans(KmeansBaseConfig configuration) {
        return new KmeansMemoryEstimateDefinition(configuration.toParameters()).memoryEstimation();
    }

    public MemoryEstimateResult kMeans(KmeansBaseConfig configuration, Object graphNameOrConfiguration) {
        var memoryEstimation = kMeans(configuration);

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    public MemoryEstimation labelPropagation() {
        return new LabelPropagationMemoryEstimateDefinition().memoryEstimation();
    }

    public MemoryEstimateResult labelPropagation(
        LabelPropagationBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = labelPropagation();

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    public MemoryEstimation lcc(LocalClusteringCoefficientBaseConfig configuration) {
        return new LocalClusteringCoefficientMemoryEstimateDefinition(configuration.seedProperty()).memoryEstimation();
    }

    public MemoryEstimateResult lcc(
        LocalClusteringCoefficientBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = lcc(configuration);

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    public MemoryEstimation leiden(LeidenBaseConfig configuration) {
        return new LeidenMemoryEstimateDefinition(configuration.toMemoryEstimationParameters()).memoryEstimation();
    }

    public MemoryEstimateResult leiden(LeidenBaseConfig configuration, Object graphNameOrConfiguration) {
        var memoryEstimation = leiden(configuration);

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    public MemoryEstimation louvain(LouvainBaseConfig configuration) {
        return new LouvainMemoryEstimateDefinition(configuration.toMemoryEstimationParameters()).memoryEstimation();
    }

    public MemoryEstimateResult louvain(LouvainBaseConfig configuration, Object graphNameOrConfiguration) {
        var memoryEstimation = louvain(configuration);

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    public MemoryEstimation modularity() {
        return new ModularityCalculatorMemoryEstimateDefinition().memoryEstimation();
    }

    public MemoryEstimateResult modularity(ModularityBaseConfig configuration, Object graphNameOrConfiguration) {
        var memoryEstimation = modularity();

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    public MemoryEstimation modularityOptimization() {
        return new ModularityOptimizationMemoryEstimateDefinition().memoryEstimation();
    }

    public MemoryEstimateResult modularityOptimization(
        ModularityOptimizationBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = modularityOptimization();

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    public MemoryEstimation scc() {
        return new SccMemoryEstimateDefinition().memoryEstimation();
    }

    public MemoryEstimateResult scc(SccBaseConfig configuration, Object graphNameOrConfiguration) {
        var memoryEstimation = scc();

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    public MemoryEstimation triangleCount() {
        return new IntersectingTriangleCountMemoryEstimateDefinition().memoryEstimation();
    }

    public MemoryEstimateResult triangleCount(TriangleCountBaseConfig configuration, Object graphNameOrConfiguration) {
        var memoryEstimation = triangleCount();

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

    MemoryEstimation triangles() {
        throw new MemoryEstimationNotImplementedException();
    }

    public MemoryEstimation wcc(SeedConfig configuration) {
        return new WccMemoryEstimateDefinition(configuration.isIncremental()).memoryEstimation();
    }

    public MemoryEstimateResult wcc(WccBaseConfig configuration, Object graphNameOrConfiguration) {
        var memoryEstimation = wcc(configuration);

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }
}
