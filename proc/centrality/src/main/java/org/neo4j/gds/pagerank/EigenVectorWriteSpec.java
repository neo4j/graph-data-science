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
import org.neo4j.gds.executor.NewConfigFunction;

import static org.neo4j.gds.executor.ExecutionMode.WRITE_NODE_PROPERTY;
import static org.neo4j.gds.pagerank.PageRankProc.EIGENVECTOR_DESCRIPTION;

@GdsCallable(name = "gds.eigenvector.write", description = EIGENVECTOR_DESCRIPTION, executionMode = WRITE_NODE_PROPERTY)
public class EigenVectorWriteSpec extends  PageRankWriteSpec {

    @Override
    public String name() {
        return "EigenvectorWrite";
    }

    @Override
    public PageRankAlgorithmFactory<PageRankWriteConfig> algorithmFactory() {
        return new PageRankAlgorithmFactory<>(PageRankAlgorithmFactory.Mode.EIGENVECTOR);
    }

    @Override
    public NewConfigFunction<PageRankWriteConfig> newConfigFunction() {
        return (___, config) -> {
            if (config.containsKey("dampingFactor")) {
                throw new IllegalArgumentException("Unexpected configuration key: dampingFactor");
            }
            return PageRankWriteConfig.of(config);
        };
    }

}
