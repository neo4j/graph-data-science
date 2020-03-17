/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.wcc2;

import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongLongMap;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.wcc.Wcc;
import org.neo4j.graphalgo.wcc.WccWriteConfig;

abstract class WccProc {

    static final String WCC_DESCRIPTION =
        "The WCC algorithm finds sets of connected nodes in an undirected graph, where all nodes in the same set form a connected component.";

    static <CONFIG extends WccWriteConfig> PropertyTranslator<DisjointSetStruct> nodePropertyTranslator(
        AlgoBaseProc.ComputationResult<Wcc, DisjointSetStruct, CONFIG> computationResult
    ) {
        WccWriteConfig config = computationResult.config();

        boolean consecutiveIds = config.consecutiveIds();
        boolean isIncremental = config.isIncremental();
        boolean seedPropertyEqualsWriteProperty = config.writeProperty().equalsIgnoreCase(config.seedProperty());

        PropertyTranslator<DisjointSetStruct> propertyTranslator;
        if (seedPropertyEqualsWriteProperty && !consecutiveIds) {
            NodeProperties seedProperties = computationResult.graph().nodeProperties(config.seedProperty());
            propertyTranslator = new PropertyTranslator.OfLongIfChanged<>(seedProperties, DisjointSetStruct::setIdOf);
        } else if (consecutiveIds && !isIncremental) {
            propertyTranslator = new ConsecutivePropertyTranslator(
                computationResult.result(),
                computationResult.tracker()
            );
        } else {
            propertyTranslator = (PropertyTranslator.OfLong<DisjointSetStruct>) DisjointSetStruct::setIdOf;
        }

        return propertyTranslator;
    }

    static class ConsecutivePropertyTranslator implements PropertyTranslator.OfLong<DisjointSetStruct> {

        // Magic number to estimate the number of communities that need to be mapped into consecutive space
        private static final long MAPPING_SIZE_QUOTIENT = 10L;

        private final HugeLongArray communities;

        ConsecutivePropertyTranslator(DisjointSetStruct dss, AllocationTracker tracker) {

            long nextConsecutiveId = -1L;

            // TODO is there a better way to set the initial size, e.g. dss.setCount
            HugeLongLongMap setIdToConsecutiveId = new HugeLongLongMap(BitUtil.ceilDiv(
                dss.size(),
                MAPPING_SIZE_QUOTIENT
            ), tracker);
            this.communities = HugeLongArray.newArray(dss.size(), tracker);

            for (int nodeId = 0; nodeId < dss.size(); nodeId++) {
                long setId = dss.setIdOf(nodeId);
                long communityId = setIdToConsecutiveId.getOrDefault(setId, -1);
                if (communityId == -1) {
                    setIdToConsecutiveId.addTo(setId, ++nextConsecutiveId);
                    communityId = nextConsecutiveId;
                }
                communities.set(nodeId, communityId);
            }
        }

        @Override
        public long toLong(DisjointSetStruct data, long nodeId) {
            return communities.get(nodeId);
        }
    }

}
