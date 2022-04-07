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
package org.neo4j.gds.ml.pipeline.nodePipeline.train;

import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.openjdk.jol.util.Multiset;

public class LabelsAndClassCountsExtractor {

    static LabelsAndClassCounts extractLabelsAndClassCounts(
        NodeProperties targetNodeProperty,
        long nodeCount
    ) {
        var classCounts = new Multiset<Long>();
        var labels = HugeLongArray.newArray(nodeCount);
        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            labels.set(nodeId, targetNodeProperty.longValue(nodeId));
            classCounts.add(targetNodeProperty.longValue(nodeId));
        }
        return ImmutableLabelsAndClassCounts.of(labels, classCounts);
    }

    @ValueClass
    interface LabelsAndClassCounts {
        HugeLongArray labels();

        Multiset<Long> classCounts();
    }
}
