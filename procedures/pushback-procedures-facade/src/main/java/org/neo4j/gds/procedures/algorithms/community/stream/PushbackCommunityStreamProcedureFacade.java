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
package org.neo4j.gds.procedures.algorithms.community.stream;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutStreamConfig;
import org.neo4j.gds.cliquecounting.CliqueCountingStreamConfig;
import org.neo4j.gds.community.CommunityComputeBusinessFacade;
import org.neo4j.gds.conductance.ConductanceConfigTransformer;
import org.neo4j.gds.conductance.ConductanceStreamConfig;
import org.neo4j.gds.hdbscan.HDBScanStreamConfig;
import org.neo4j.gds.k1coloring.K1ColoringStreamConfig;
import org.neo4j.gds.procedures.algorithms.community.ApproxMaxKCutStreamResult;
import org.neo4j.gds.procedures.algorithms.community.CliqueCountingStreamResult;
import org.neo4j.gds.procedures.algorithms.community.ConductanceStreamResult;
import org.neo4j.gds.procedures.algorithms.community.HDBScanStreamResult;
import org.neo4j.gds.procedures.algorithms.community.K1ColoringStreamResult;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;

import java.util.Map;
import java.util.stream.Stream;

public class PushbackCommunityStreamProcedureFacade {

    private final CommunityComputeBusinessFacade businessFacade;
    private final UserSpecificConfigurationParser configurationParser;

    public PushbackCommunityStreamProcedureFacade(
        CommunityComputeBusinessFacade businessFacade,
        UserSpecificConfigurationParser configurationParser
    ) {
        this.businessFacade = businessFacade;
        this.configurationParser = configurationParser;
    }

    public Stream<ApproxMaxKCutStreamResult> approxMaxKCut(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, ApproxMaxKCutStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.approxMaxKCut(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new ApproxMaxKCutStreamTransformer(graphResources.graph(), parameters.concurrency(),config.minCommunitySize())
        ).join();
    }

    public Stream<CliqueCountingStreamResult> cliqueCounting(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, CliqueCountingStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.cliqueCounting(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new CliqueCountingStreamTransformer(graphResources.graph())
        ).join();
    }

    public Stream<ConductanceStreamResult> conductance(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, ConductanceStreamConfig::of);

        return businessFacade.conductance(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            ConductanceConfigTransformer.toParameters(config),
            config.jobId(),
            config.logProgress(),
            graphResources -> new ConductanceStreamTransformer()
        ).join();
    }

    public Stream<HDBScanStreamResult> hdbscan(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, HDBScanStreamConfig::of);

        return businessFacade.hdbscan(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            graphResources -> new HDBScanStreamTransformer(graphResources.graph())
        ).join();
    }

    public Stream<K1ColoringStreamResult> k1Coloring(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(configuration, K1ColoringStreamConfig::of);

        var parameters = config.toParameters();
        return businessFacade.k1Coloring(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            parameters,
            config.jobId(),
            config.logProgress(),
            graphResources -> new K1ColoringCutStreamTransformer(graphResources.graph(),parameters.concurrency(),config.minCommunitySize())
        ).join();
    }

}
