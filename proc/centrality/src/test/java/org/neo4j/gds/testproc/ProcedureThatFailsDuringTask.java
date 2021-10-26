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
package org.neo4j.gds.testproc;

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.test.config.DummyConfig;
import org.neo4j.gds.test.config.DummyConfigImpl;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class ProcedureThatFailsDuringTask extends AlgoBaseProc<FailingAlgorithm, ProcedureThatFailsDuringTask.Output, DummyConfig> {
    @Procedure(name = "very.strange.procedure", mode = Mode.READ)
    public Stream<Output> run(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var result = compute(graphNameOrConfig, configuration);
        assert result.result() == null;
        return Stream.empty();
    }

    @Override
    protected DummyConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return new DummyConfigImpl(graphName,  maybeImplicitCreate, username, config);
    }

    @Override
    protected AlgorithmFactory<FailingAlgorithm, DummyConfig> algorithmFactory() {
        return new AlgorithmFactory<FailingAlgorithm, DummyConfig>() {
            @Override
            protected String taskName() {
                return "Failing Algorithm";
            }

            @Override
            protected FailingAlgorithm build(
                Graph graph,
                DummyConfig configuration,
                AllocationTracker allocationTracker,
                ProgressTracker progressTracker
            ) {
                return new FailingAlgorithm(progressTracker);
            }
       };
    }

    public class Output {
        public Object out;
    }
}
