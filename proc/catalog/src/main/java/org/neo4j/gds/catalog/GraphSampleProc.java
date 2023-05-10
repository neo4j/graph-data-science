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
package org.neo4j.gds.catalog;

import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ProcPreconditions;
import org.neo4j.gds.graphsampling.RandomWalkBasedNodesSampler;
import org.neo4j.gds.graphsampling.config.CommonNeighbourAwareRandomWalkConfig;
import org.neo4j.gds.graphsampling.config.RandomWalkWithRestartsConfig;
import org.neo4j.gds.graphsampling.samplers.rw.cnarw.CommonNeighbourAwareRandomWalk;
import org.neo4j.gds.graphsampling.samplers.rw.rwr.RandomWalkWithRestarts;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class GraphSampleProc extends CatalogProc {

    private static final String RWR_DESCRIPTION = "Constructs a random subgraph based on random walks with restarts";
    private static final String CNARW_DESCRIPTION = "Constructs a random subgraph based on common neighbour aware random walks";

    public static final Function<CypherMapWrapper, RandomWalkWithRestartsConfig> RWR_CONFIG_PROVIDER =
        (cypherMapWrapper) -> RandomWalkWithRestartsConfig.of(cypherMapWrapper);
    public static final Function<CypherMapWrapper, RandomWalkWithRestartsConfig> CNARW_CONFIG_PROVIDER =
        (cypherMapWrapper) -> CommonNeighbourAwareRandomWalkConfig.of(cypherMapWrapper);

    public static final Function<RandomWalkWithRestartsConfig, RandomWalkBasedNodesSampler> RWR_PROVIDER =
        (rwrConfig) -> new RandomWalkWithRestarts(rwrConfig);
    public static final Function<RandomWalkWithRestartsConfig, RandomWalkBasedNodesSampler> CNARW_PROVIDER =
        (cnarwConfig) -> new CommonNeighbourAwareRandomWalk((CommonNeighbourAwareRandomWalkConfig) cnarwConfig);


    @Procedure(name = "gds.alpha.graph.sample.rwr", mode = READ)
    @Description(RWR_DESCRIPTION)
    public Stream<RandomWalkSamplingResult> sampleRandomWalkWithRestarts(
        @Name(value = "graphName") String graphName,
        @Name(value = "fromGraphName") String fromGraphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {

        ProcPreconditions.check();
        validateGraphName(username(), graphName);
        System.out.println(username() + " " + executionContext().username());
        return SamplerOperator.performSampling(
            fromGraphName,
            graphName, configuration,
            RWR_CONFIG_PROVIDER,
            RWR_PROVIDER,
            executionContext()
        );

    }


    @Procedure(name = "gds.alpha.graph.sample.cnarw", mode = READ)
    @Description(CNARW_DESCRIPTION)
    public Stream<RandomWalkSamplingResult> sampleCNARW(
        @Name(value = "graphName") String graphName,
        @Name(value = "fromGraphName") String fromGraphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ProcPreconditions.check();
        validateGraphName(username(), graphName);
        return SamplerOperator.performSampling(
            fromGraphName,
            graphName, configuration,
            CNARW_CONFIG_PROVIDER,
            CNARW_PROVIDER,
            executionContext()
        );

    }


    public static class RandomWalkSamplingResult extends GraphProjectProc.GraphProjectResult {
        public final String fromGraphName;
        public final long startNodeCount;

        RandomWalkSamplingResult(
            String graphName,
            String fromGraphName,
            long nodeCount,
            long relationshipCount,
            long startNodeCount,
            long projectMillis
        ) {
            super(graphName, nodeCount, relationshipCount, projectMillis);
            this.fromGraphName = fromGraphName;
            this.startNodeCount = startNodeCount;
        }
    }


}
