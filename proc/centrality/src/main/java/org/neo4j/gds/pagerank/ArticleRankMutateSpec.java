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

import org.neo4j.gds.executor.GdsCallable;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.gds.pagerank.PageRankProc.ARTICLE_RANK_DESCRIPTION;

@GdsCallable(name = "gds.articleRank.stats", description = ARTICLE_RANK_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class ArticleRankMutateSpec extends PageRankMutateSpec {

    @Override
    public String name() {
        return "ArticleRankMutate";
    }

    @Override
    public PageRankAlgorithmFactory<PageRankMutateConfig> algorithmFactory() {
        return new PageRankAlgorithmFactory<>(PageRankAlgorithmFactory.Mode.ARTICLE_RANK);
    }




}
