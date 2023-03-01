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
package org.neo4j.gds.louvain;

import org.neo4j.gds.CommunityProcCompanion;
import org.neo4j.gds.api.properties.nodes.LongArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

final class LouvainProc {

    static final String LOUVAIN_DESCRIPTION =
        "The Louvain method for community detection is an algorithm for detecting communities in networks.";

    private LouvainProc() {}

    static <CONFIG extends LouvainBaseConfig> NodePropertyValues nodeProperties(
        ComputationResult<Louvain, Louvain, CONFIG> computationResult,
        String resultProperty
    ) {
        var config = computationResult.config();
        var includeIntermediateCommunities = config.includeIntermediateCommunities();

        if (!includeIntermediateCommunities) {
            return CommunityProcCompanion.nodeProperties(
                computationResult.config(),
                resultProperty,
                computationResult.result().finalDendrogram().asNodeProperties(),
                () -> computationResult.graphStore().nodeProperty(config.seedProperty())
            );
        } else {
            var size = computationResult.graph().nodeCount();
            var communityResult = computationResult.result();

            return new LongArrayNodePropertyValues() {
                @Override
                public long valuesStored() {
                    return size;
                }

                @Override
                public long[] longArrayValue(long nodeId) {
                    return communityResult.getCommunities(nodeId);
                }
            };
        }
    }

    static <PROC_RESULT, CONFIG extends LouvainBaseConfig> AbstractResultBuilder<PROC_RESULT> resultBuilder(
        LouvainResultBuilder<PROC_RESULT> procResultBuilder,
        ComputationResult<Louvain, Louvain, CONFIG> computeResult
    ) {
        Louvain result = computeResult.result();
        boolean nonEmpty = !computeResult.isGraphEmpty();

        return procResultBuilder
            .withLevels(nonEmpty ? result.levels() : 0)
            .withModularity(nonEmpty ? result.modularities()[result.levels() - 1] : 0)
            .withModularities(nonEmpty ? result.modularities() : new double[0])
            .withCommunityFunction(nonEmpty ? result::getCommunity : null);
    }

    abstract static class LouvainResultBuilder<PROC_RESULT> extends AbstractCommunityResultBuilder<PROC_RESULT> {

        long levels = -1;
        double[] modularities = new double[]{};
        double modularity = -1;

        LouvainResultBuilder(
            ProcedureCallContext context,
            int concurrency
        ) {
            super(context, concurrency);
        }

        LouvainResultBuilder<PROC_RESULT> withLevels(long levels) {
            this.levels = levels;
            return this;
        }

        LouvainResultBuilder<PROC_RESULT> withModularities(double[] modularities) {
            this.modularities = modularities;
            return this;
        }

        LouvainResultBuilder<PROC_RESULT> withModularity(double modularity) {
            this.modularity = modularity;
            return this;
        }
    }
}
