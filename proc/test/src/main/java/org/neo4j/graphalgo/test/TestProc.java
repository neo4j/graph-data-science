/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.test;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.StatsProc;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class TestProc extends StatsProc<TestAlgorithm, TestAlgorithm, TestProc.TestResult, TestConfig> {

    @Procedure(value = "gds.testProc.test", mode = READ)
    @Description(STATS_DESCRIPTION)
    public Stream<TestResult> stats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<TestAlgorithm, TestAlgorithm, TestConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return stats(computationResult);
    }

    @Procedure(value = "gds.testProc.test.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimateStats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected AbstractResultBuilder<TestResult> resultBuilder(ComputationResult<TestAlgorithm, TestAlgorithm, TestConfig> computeResult) {
        return new TestAlgoResultBuilder();
    }

    @Override
    protected TestConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return TestConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<TestAlgorithm, TestConfig> algorithmFactory(TestConfig config) {
        return new AlgorithmFactory<TestAlgorithm, TestConfig>() {
            @Override
            public TestAlgorithm build(
                Graph graph, TestConfig configuration, AllocationTracker tracker, Log log
            ) {
                return new TestAlgorithm();
            }

            @Override
            public MemoryEstimation memoryEstimation(TestConfig configuration) {
                throw new MemoryEstimationNotImplementedException();
            }
        };
    }

    static class TestAlgoResultBuilder extends AbstractResultBuilder<TestResult> {
        @Override
        public TestResult build() {
            return new TestResult(
                createMillis,
                computeMillis,
                config.toMap()
            );
        }
    }

    public static final class TestResult {

        public long createMillis;
        public long computeMillis;
        public Map<String, Object> configuration;

        TestResult(long createMillis, long computeMillis, Map<String, Object> configuration) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.configuration = configuration;
        }
    }
}
