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
package org.neo4j.gds.kmeans;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;

final class KmeansProc {

    static final String Kmeans_DESCRIPTION =
        "The Kmeans  algorithm clusters nodes into different communities based on Euclidean distance";

    private KmeansProc() {}

    static <CONFIG extends KmeansBaseConfig> GraphAlgorithmFactory<Kmeans, CONFIG> algorithmFactory() {
        return new KmeansAlgorithmFactory<>();
    }

    static <PROC_RESULT, CONFIG extends KmeansBaseConfig> AbstractCommunityResultBuilder<PROC_RESULT> resultBuilder(
        AbstractCommunityResultBuilder<PROC_RESULT> procResultBuilder,
        ComputationResult<Kmeans, KmeansResult, CONFIG> computationResult
    ) {
        return procResultBuilder.withCommunityFunction(!computationResult.isGraphEmpty() ? computationResult
            .result()
            .communities()::get : null);
    }

    static <CONFIG extends KmeansBaseConfig> NodeProperties nodeProperties(
        ComputationResult<Kmeans, KmeansResult, CONFIG> computeResult
    ) {
        return computeResult.result().communities().asNodeProperties();
    }
}
