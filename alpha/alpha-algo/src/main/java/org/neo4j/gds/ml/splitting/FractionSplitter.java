/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.gds.ml.splitting;

import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

public class FractionSplitter {
    // fraction is in [0,1]
    public NodeSplit split(long nodeCount, double trainFraction) {
        long trainSize = (long) (nodeCount * trainFraction);
        long testSize = nodeCount - trainSize;
        var train = HugeLongArray.newArray(trainSize, AllocationTracker.empty());
        var test = HugeLongArray.newArray(testSize, AllocationTracker.empty());
        train.setAll(i -> i);
        test.setAll(i -> i + trainSize);
        return new NodeSplit(train, test);
    }

    public NodeSplit split(HugeLongArray ids, double trainFraction) {
        long trainSize = (long) (ids.size() * trainFraction);
        long testSize = ids.size() - trainSize;
        var train = HugeLongArray.newArray(trainSize, AllocationTracker.empty());
        var test = HugeLongArray.newArray(testSize, AllocationTracker.empty());
        train.setAll(ids::get);
        test.setAll(i -> ids.get(i + trainSize));
        return new NodeSplit(train, test);
    }
}
