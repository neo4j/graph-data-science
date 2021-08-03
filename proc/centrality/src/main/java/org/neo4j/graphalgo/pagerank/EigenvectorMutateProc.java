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
package org.neo4j.graphalgo.pagerank;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.pagerank.PageRankAlgorithm;
import org.neo4j.gds.pagerank.PageRankAlgorithmFactory;
import org.neo4j.gds.pagerank.PageRankMutateConfig;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.pagerank.PageRankProc.EIGENVECTOR_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class EigenvectorMutateProc extends PageRankMutateProc {

    @Override
    @Procedure(value = "gds.eigenvector.mutate", mode = READ)
    @Description(EIGENVECTOR_DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return super.mutate(graphNameOrConfig, configuration);
    }

    @Override
    @Procedure(value = "gds.eigenvector.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return super.estimate(graphNameOrConfig, configuration);
    }

    @Override
    protected AlgorithmFactory<PageRankAlgorithm, PageRankMutateConfig> algorithmFactory() {
        return new PageRankAlgorithmFactory<PageRankMutateConfig>(PageRankAlgorithmFactory.Mode.EIGENVECTOR);
    }

    @Override
    protected PageRankMutateConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        if (config.containsKey("dampingFactor")) {
            throw new IllegalArgumentException("Unexpected configuration key: dampingFactor");
        }

        return super.newConfig(username, graphName, maybeImplicitCreate, config);
    }
}
