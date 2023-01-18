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
package org.neo4j.gds.labelpropagation;

import org.neo4j.gds.CommunityProcCompanion;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

final class LabelPropagationProc {

    static final String LABEL_PROPAGATION_DESCRIPTION =
        "The Label Propagation algorithm is a fast algorithm for finding communities in a graph.";

    private LabelPropagationProc() {}

    static <CONFIG extends LabelPropagationBaseConfig> NodePropertyValues nodeProperties(
        ComputationResult<LabelPropagation, LabelPropagationResult, CONFIG> computationResult,
        String resultProperty
    ) {
        var config = computationResult.config();

        return CommunityProcCompanion.nodeProperties(
            config,
            resultProperty,
            computationResult.result().labels().asNodeProperties(),
            () -> computationResult.graphStore().nodeProperty(config.seedProperty())
        );
    }

    static <PROC_RESULT, CONFIG extends LabelPropagationBaseConfig> AbstractResultBuilder<PROC_RESULT> resultBuilder(
        LabelPropagationResultBuilder<PROC_RESULT> procResultBuilder,
        ComputationResult<LabelPropagation, LabelPropagationResult, CONFIG> computeResult
    ) {
        return procResultBuilder
            .didConverge(!computeResult.isGraphEmpty() ? computeResult.result().didConverge() : false)
            .ranIterations(!computeResult.isGraphEmpty() ? computeResult.result().ranIterations() : 0)
            .withCommunityFunction(!computeResult.isGraphEmpty()
                ? (nodeId) -> computeResult.result().labels().get(nodeId)
                : null
            );
    }

    abstract static class LabelPropagationResultBuilder<PROC_RESULT> extends AbstractCommunityResultBuilder<PROC_RESULT> {
        long ranIterations;

        boolean didConverge;

        LabelPropagationResultBuilder(ProcedureCallContext callContext, int concurrency) {
            super(callContext, concurrency);
        }

        LabelPropagationResultBuilder<PROC_RESULT> ranIterations(long iterations) {
            this.ranIterations = iterations;
            return this;
        }

        LabelPropagationResultBuilder<PROC_RESULT> didConverge(boolean didConverge) {
            this.didConverge = didConverge;
            return this;
        }
    }
}
