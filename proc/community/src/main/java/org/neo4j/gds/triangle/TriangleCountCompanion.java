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
package org.neo4j.gds.triangle;

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.ParalleLongPageCreator;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.result.AbstractResultBuilder;

import java.util.Optional;

final class TriangleCountCompanion {

    static final String DESCRIPTION =
        "Triangle counting is a community detection graph algorithm that is used to " +
        "determine the number of triangles passing through each node in the graph.";


    static <CONFIG extends TriangleCountBaseConfig> NodePropertyValues nodePropertyTranslator(ComputationResult<IntersectingTriangleCount, TriangleCountResult, CONFIG> computeResult) {
        return computeResult.result().asNodeProperties();
    }

    static <PROC_RESULT, CONFIG extends TriangleCountBaseConfig> AbstractResultBuilder<PROC_RESULT> resultBuilder(
        TriangleCountResultBuilder<PROC_RESULT> procResultBuilder,
        ComputationResult<IntersectingTriangleCount, TriangleCountResult, CONFIG> computeResult
    ) {
        var result = Optional.ofNullable(computeResult.result()).orElse(EmptyResult.EMPTY_RESULT);
        return procResultBuilder.withGlobalTriangleCount(result.globalTriangles());
    }

    abstract static class TriangleCountResultBuilder<PROC_RESULT> extends AbstractResultBuilder<PROC_RESULT> {

        long globalTriangleCount = 0;

        TriangleCountResultBuilder<PROC_RESULT> withGlobalTriangleCount(long globalTriangleCount) {
            this.globalTriangleCount = globalTriangleCount;
            return this;
        }

    }

    private TriangleCountCompanion() {}

    private static final class EmptyResult implements TriangleCountResult {

        static final EmptyResult EMPTY_RESULT = new EmptyResult();

        private EmptyResult() {}

        @Override
        public HugeAtomicLongArray localTriangles() {
            return HugeAtomicLongArray.of(0, ParalleLongPageCreator.passThrough(1));
        }

        @Override
        public long globalTriangles() {
            return 0;
        }

    }
}
