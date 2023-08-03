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
package org.neo4j.gds.facade;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.wcc.WccBaseConfig;

import java.util.Optional;

@ValueClass
public interface ComputationResult<CONFIG extends WccBaseConfig> {

    /**
     * Result is empty if no computation happened, which basically means the graph was empty.
     * @return The result if computation happened.
     */
    Optional<DisjointSetStruct> result();

    Graph graph();

    CONFIG config();

    GraphStore graphStore();

    static <C extends WccBaseConfig> ComputationResult<C> of(DisjointSetStruct result, Graph graph, C config, GraphStore graphStore) {
        return ImmutableComputationResult.of(result, graph, config, graphStore);
    }

    static <C extends WccBaseConfig> ComputationResult<C> withoutAlgorithmResult(Graph graph, C config, GraphStore graphStore) {
        return ImmutableComputationResult.of(Optional.empty(), graph, config, graphStore);
    }

}
