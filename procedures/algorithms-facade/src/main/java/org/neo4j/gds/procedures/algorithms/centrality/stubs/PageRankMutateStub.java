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
package org.neo4j.gds.procedures.algorithms.centrality.stubs;

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.algorithms.centrality.CentralityAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.config.MutateNodePropertyConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.pagerank.PageRankResult;
import org.neo4j.gds.pagerank.RankConfig;
import org.neo4j.gds.procedures.algorithms.AlgorithmHandle;
import org.neo4j.gds.procedures.algorithms.centrality.PageRankMutateResult;
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.gds.procedures.algorithms.stubs.MutateStub;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

public class PageRankMutateStub<C extends RankConfig & MutateNodePropertyConfig> implements MutateStub<C, PageRankMutateResult> {
    private final GenericStub genericStub;
    private final CentralityAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade;
    private final ProcedureReturnColumns procedureReturnColumns;
    private final AlgorithmHandle<C, PageRankResult, PageRankMutateResult, NodePropertiesWritten> handle;
    private final Function<CypherMapWrapper,C> configProducer;

    public PageRankMutateStub(
        GenericStub genericStub,
        CentralityAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade,
        ProcedureReturnColumns procedureReturnColumns,
        AlgorithmHandle<C, PageRankResult, PageRankMutateResult, NodePropertiesWritten> handle,
        Function<CypherMapWrapper, C> configProducer
    ) {
        this.genericStub = genericStub;
        this.estimationModeBusinessFacade = estimationModeBusinessFacade;
        this.procedureReturnColumns = procedureReturnColumns;
        this.handle = handle;
        this.configProducer = configProducer;
    }

    @Override
    public C parseConfiguration(Map<String, Object> configuration) {
        return genericStub.parseConfiguration(configProducer, configuration);
    }

    @Override
    public MemoryEstimation getMemoryEstimation(String username, Map<String, Object> configuration) {
        return genericStub.getMemoryEstimation(
            configuration,
            configProducer,
            __ -> estimationModeBusinessFacade.pageRank()
        );
    }

    @Override
    public Stream<MemoryEstimateResult> estimate(Object graphName, Map<String, Object> configuration) {
        return genericStub.estimate(
            graphName,
            configuration,
            configProducer,
            __ -> estimationModeBusinessFacade.pageRank()
        );
    }

    @Override
    public Stream<PageRankMutateResult> execute(
        String graphNameAsString,
        Map<String, Object> rawConfiguration
    ) {
        var shouldComputeCentralityDistribution = procedureReturnColumns.contains("centralityDistribution");
        var resultBuilder = new PageRankResultBuilderForMutateMode<C>(shouldComputeCentralityDistribution);

        return genericStub.execute(
            graphNameAsString,
            rawConfiguration,
            configProducer,
            handle,
            resultBuilder
        );
    }
}
