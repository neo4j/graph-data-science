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
package org.neo4j.gds.leiden;

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class LeidenStreamProc extends AlgoBaseProc<Leiden, HugeLongArray, LeidenStreamConfig, LeidenStreamProc.StreamResult> {
    // Config
    // relationshipWeightProperty
    // maxIterations

    // Output
    // nodeId
    // communityId

    static final String DESCRIPTION =
        "Leiden is a community detection algorithm, which guarantees that communities are well connected";

    @Procedure(name = "gds.alpha.leiden.stream", mode = READ)
    @Description(DESCRIPTION)
    public Stream<StreamResult> stream(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var streamSpec = new LeidenStreamSpec();

        return new ProcedureExecutor<>(
            streamSpec,
            executionContext()
        ).compute(graphName, configuration, true, true);
    }

    @Override
    public AlgorithmFactory<?, Leiden, LeidenStreamConfig> algorithmFactory() {
        return new LeidenStreamSpec().algorithmFactory();
    }

    @Override
    public ComputationResultConsumer<Leiden, HugeLongArray, LeidenStreamConfig, Stream<StreamResult>> computationResultConsumer() {
        return new LeidenStreamSpec().computationResultConsumer();
    }

    @Override
    protected LeidenStreamConfig newConfig(String username, CypherMapWrapper config) {
        return new LeidenStreamSpec().newConfigFunction().apply(username, config);
    }

    public static class StreamResult {

        public final long nodeId;
        public final long communityId;

        StreamResult(long nodeId, long communityId) {
            this.nodeId = nodeId;
            this.communityId = communityId;
        }
    }
}
