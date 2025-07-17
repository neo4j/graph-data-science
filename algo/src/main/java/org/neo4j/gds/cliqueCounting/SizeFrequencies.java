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
package org.neo4j.gds.cliqueCounting;

import org.neo4j.gds.cliquecounting.CliqueCountingMode;
import org.neo4j.gds.collections.ha.HugeObjectArray;

import java.util.Arrays;
import java.util.stream.Stream;

class SizeFrequencies {
    //Intended not thread-safe!
    SizeFrequency globalCount;
    HugeObjectArray<SizeFrequency> perNodeCount;

    SizeFrequencies(long nodeCount) {
        globalCount = new SizeFrequency(); //can also be used for a subclique (but they should not be merged afterwards)
        perNodeCount = HugeObjectArray.newArray(SizeFrequency.class, nodeCount);
        perNodeCount.setAll(_x -> new SizeFrequency()); //initialized already?
    }

    public void updateFrequencies(CliqueCountingMode mode, long[] nodes, int requiredCount) {
        switch (mode) {
            case GloballyOnly -> globalCount.add(requiredCount, nodes.length-requiredCount);
            case ForEveryNode -> {
                //required nodes + global
                var requiredNodes = Arrays.copyOf(nodes, requiredCount);
                var sizeFrequencyRefs = Stream.concat(
                    Arrays.stream(requiredNodes).mapToObj(node -> perNodeCount.get(node)),
                    Stream.of(globalCount)
                ).toList();
                SizeFrequency.add(requiredCount, nodes.length-requiredCount, sizeFrequencyRefs);

//                optional nodes
                var optionalNodes = Arrays.copyOfRange(nodes, requiredCount, nodes.length);
                sizeFrequencyRefs = Arrays.stream(optionalNodes).mapToObj(node -> perNodeCount.get(node)).toList();
                SizeFrequency.add(requiredCount + 1, nodes.length - requiredCount - 1, sizeFrequencyRefs);
            }
            case ForGivenSubcliques -> globalCount.add(
                requiredCount,
                nodes.length - requiredCount
            ); //but should be handled differently afterwards!
        }
    }

    public void updateFrequencies(CliqueCountingMode mode, long[] requiredNodes, long[] optionalNodes) {
        switch (mode) {
            case GloballyOnly -> globalCount.add(requiredNodes.length, optionalNodes.length);
            case ForEveryNode -> {
                //required nodes + global
                var sizeFrequencyRefs = Stream.concat(
                    Arrays.stream(requiredNodes).mapToObj(node -> perNodeCount.get(node)),
                    Stream.of(globalCount)
                ).toList();
                SizeFrequency.add(requiredNodes.length, optionalNodes.length, sizeFrequencyRefs);

                //optional nodes
                sizeFrequencyRefs = Arrays.stream(optionalNodes).mapToObj(node -> perNodeCount.get(node)).toList();
                SizeFrequency.add(requiredNodes.length + 1, optionalNodes.length - 1, sizeFrequencyRefs);
            }
            case ForGivenSubcliques -> globalCount.add(
                requiredNodes.length,
                optionalNodes.length
            ); //but should be handled differently afterwards!
        }
    }

    public void merge(SizeFrequencies other) {
        globalCount.merge(other.globalCount);
        assert (perNodeCount.size() == other.perNodeCount.size());
        for (int idx = 0; idx < perNodeCount.size(); idx++) {
            perNodeCount.get(idx).merge(other.perNodeCount.get(idx));
        }
    }

}
