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
package org.neo4j.graphalgo.pagerank;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.MutateProc;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.pagerank.PageRankProc.PAGE_RANK_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class PageRankMutateProc extends MutateProc<PageRank, PageRank, PageRankMutateProc.MutateResult, PageRankMutateConfig> {

    @Procedure(value = "gds.pageRank.mutate", mode = READ)
    @Description(PAGE_RANK_DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<PageRank, PageRank, PageRankMutateConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return mutate(computationResult);
    }

    @Procedure(value = "gds.pageRank.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected PageRankMutateConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return PageRankMutateConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<PageRank, PageRankMutateConfig> algorithmFactory(PageRankMutateConfig config) {
        return PageRankProc.algorithmFactory(config);
    }

    @Override
    protected PropertyTranslator<PageRank> nodePropertyTranslator(ComputationResult<PageRank, PageRank, PageRankMutateConfig> computationResult) {
        return PageRankProc.ScoresTranslator.INSTANCE;
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(ComputationResult<PageRank, PageRank, PageRankMutateConfig> computeResult) {
        return PageRankProc.resultBuilder(new MutateResult.Builder(), computeResult);
    }

    public static final class MutateResult {

        public long nodePropertiesWritten;
        public long createMillis;
        public long computeMillis;
        public long mutateMillis;
        public long ranIterations;
        public boolean didConverge;
        public Map<String, Object> configuration;

        MutateResult(
            long nodePropertiesWritten,
            long createMillis,
            long computeMillis,
            long mutateMillis,
            long ranIterations,
            boolean didConverge,
            Map<String, Object> configuration
        ) {
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.mutateMillis = mutateMillis;
            this.ranIterations = ranIterations;
            this.didConverge = didConverge;
            this.configuration = configuration;
        }

        static class Builder extends PageRankProc.PageRankResultBuilder<MutateResult> {

            @Override
            public MutateResult build() {
                return new MutateResult(
                    nodePropertiesWritten,
                    createMillis,
                    computeMillis,
                    mutateMillis,
                    ranIterations,
                    didConverge,
                    config.toMap()
                );
            }
        }
    }
}
