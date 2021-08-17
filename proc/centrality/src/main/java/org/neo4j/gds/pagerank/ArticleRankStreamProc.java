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
package org.neo4j.gds.pagerank;

import org.neo4j.gds.AbstractAlgorithmFactory;
import org.neo4j.gds.common.CentralityStreamResult;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.pagerank.PageRankProc.ARTICLE_RANK_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class ArticleRankStreamProc extends PageRankStreamProc {

    @Override
    @Procedure(value = "gds.articleRank.stream", mode = READ)
    @Description(ARTICLE_RANK_DESCRIPTION)
    public Stream<CentralityStreamResult> stream(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return super.stream(graphNameOrConfig, configuration);
    }

    @Override
    @Procedure(value = "gds.articleRank.stream.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return super.computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected AbstractAlgorithmFactory<PageRankAlgorithm, PageRankStreamConfig> algorithmFactory() {
        return new PageRankAlgorithmFactory<>(PageRankAlgorithmFactory.Mode.ARTICLE_RANK);
    }
}
