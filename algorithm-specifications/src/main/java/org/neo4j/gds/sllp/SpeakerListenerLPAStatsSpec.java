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
package org.neo4j.gds.sllp;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.NullComputationResultConsumer;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.pregel.PregelResult;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.procedures.algorithms.community.SpeakerListenerLPAStatsResult;
import org.neo4j.gds.procedures.algorithms.configuration.NewConfigFunction;
import org.neo4j.gds.sllpa.SpeakerListenerLPA;
import org.neo4j.gds.sllpa.SpeakerListenerLPAConfig;
import org.neo4j.gds.sllpa.SpeakerListenerLPAMemoryEstimateDefinition;

import java.util.stream.Stream;

@GdsCallable(
    name = "gds.sllpa.stats",
    aliases = {"gds.alpha.sllpa.stats"},
    description = Constants.SLLP_DESCRIPTION,
    executionMode = ExecutionMode.STATS
)
public class SpeakerListenerLPAStatsSpec implements AlgorithmSpec<SpeakerListenerLPA, PregelResult, SpeakerListenerLPAConfig, Stream<SpeakerListenerLPAStatsResult>, SpeakerListenerLPAStatsSpec.Factory> {

    @Override
    public String name() {
        return "SpeakerListenerLPAStream";
    }

    @Override
    public Factory algorithmFactory(ExecutionContext executionContext) {
        return new Factory();
    }

    @Override
    public NewConfigFunction<SpeakerListenerLPAConfig> newConfigFunction() {
        return (___,config) -> SpeakerListenerLPAConfig.of(config);
    }

    @Override
    public ComputationResultConsumer<SpeakerListenerLPA, PregelResult, SpeakerListenerLPAConfig, Stream<SpeakerListenerLPAStatsResult>> computationResultConsumer() {
        return new NullComputationResultConsumer<>();
    }


    static class Factory extends GraphAlgorithmFactory<SpeakerListenerLPA,SpeakerListenerLPAConfig> {

        @Override
        public SpeakerListenerLPA build(
            Graph graphOrGraphStore,
            SpeakerListenerLPAConfig configuration,
            ProgressTracker progressTracker
        ) {
            return null;
        }

        @Override
        public String taskName() {
            return "";
        }

        @Override
        public MemoryEstimation memoryEstimation(SpeakerListenerLPAConfig config) {
            return  new SpeakerListenerLPAMemoryEstimateDefinition().memoryEstimation();
        }
    }
}
