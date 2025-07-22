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

public class RecursiveCliqueNodes {

    private final long[] nodes;
    private final boolean[] required;
    private int index = 0;
    private int requiredNodes =0;

    RecursiveCliqueNodes(int maxSize) {
        this.nodes = new long[maxSize];
        this.required = new boolean[maxSize];
    }

    public static RecursiveCliqueNodes create(long node, int degree){
            var cliqueNodes = new RecursiveCliqueNodes(degree+1);
            cliqueNodes.add(node,true);
            return  cliqueNodes;
    }

    void add(long nodeId, boolean nodeRequired){
        nodes[index] = nodeId;
        required[index++] = nodeRequired;
        if (nodeRequired) requiredNodes++;
    }


    void finishRecursionLevel(){
        index--;
        if (required[index]) requiredNodes--;

    }

    long[] activeNodes(){
        var requiredPointer = 0;
        var optionalPointer = index - 1;
        long[] active = new long[index];
        for (int i=0;i < index;++i){
            var cliqueNode = nodes[i];
            if (required[i]) {
                active[requiredPointer++] = cliqueNode;
            } else {
                active[optionalPointer--] = cliqueNode;
            }
        }
        return active;
    }

    int requiredNodes(){
        return  requiredNodes;
    }

}
