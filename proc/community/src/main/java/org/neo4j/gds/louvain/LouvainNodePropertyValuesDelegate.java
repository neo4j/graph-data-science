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
import org.neo4j.gds.api.properties.nodes.EmptyLongArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.LongArrayNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.executor.ComputationResult;

final class LouvainNodePropertyValuesDelegate {
    private LouvainNodePropertyValuesDelegate() {}

    static <CONFIG extends LouvainBaseConfig> NodePropertyValues extractNodeProperties(
        ComputationResult<Louvain, LouvainResult, CONFIG> computationResult,
        String resultProperty
    ) {

        if (computationResult.result().isEmpty()) {
            return EmptyLongArrayNodePropertyValues.INSTANCE;
        }

        var result = computationResult.result().get();

        var config = computationResult.config();
        var includeIntermediateCommunities = config.includeIntermediateCommunities();

        if (includeIntermediateCommunities) {
            return longArrayNodePropertyValues(computationResult.graph().nodeCount(), result);
        }

        return CommunityProcCompanion.nodeProperties(
            computationResult.config(),
            resultProperty,
            result.dendrogramManager().getCurrent().asNodeProperties(),
            () -> computationResult.graphStore().nodeProperty(config.seedProperty())
        );
    }

    private static LongArrayNodePropertyValues longArrayNodePropertyValues(long size, LouvainResult result) {
        return new LongArrayNodePropertyValues() {
            @Override
            public long nodeCount() {
                return size;
            }

            @Override
            public long[] longArrayValue(long nodeId) {
                return result.getIntermediateCommunities(nodeId);
            }
        };
    }
}
