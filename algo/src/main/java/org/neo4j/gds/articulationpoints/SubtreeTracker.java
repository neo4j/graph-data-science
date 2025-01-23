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
package org.neo4j.gds.articulationpoints;

import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.collections.ha.HugeLongArray;

public class SubtreeTracker {

    private final HugeLongArray totalSize;
    private final HugeLongArray minSplitChildSize;
    private final HugeLongArray maxSplitChildSize;
    private final HugeLongArray sumOfChildrenWithRoot;
    private final HugeIntArray children;
    private final HugeLongArray root;

    SubtreeTracker(long nodeCount){
        this.totalSize = HugeLongArray.newArray(nodeCount);
        this.minSplitChildSize = HugeLongArray.newArray(nodeCount);
        this.maxSplitChildSize = HugeLongArray.newArray(nodeCount);
        this.sumOfChildrenWithRoot =  HugeLongArray.newArray(nodeCount);
        this.root = HugeLongArray.newArray(nodeCount);
        this.children = HugeIntArray.newArray(nodeCount);
        totalSize.fill(1);
    }

    void recordSplitChild(long parent, long child) {
        long  childSize = totalSize.get(child);
        totalSize.addTo(parent,  childSize);
        maxSplitChildSize.set(parent, Math.max(childSize, maxSplitChildSize.get(parent)));
        children.addTo(parent,1);
        if (minSplitChildSize.get(parent)==0 || minSplitChildSize.get(parent) > childSize){
            minSplitChildSize.set(parent, childSize);
        }
    }

    void recordJoinedChild(long parent, long child) {
        long  childSize = totalSize.get(child);
        totalSize.addTo(parent,  childSize);
        sumOfChildrenWithRoot.addTo(parent,childSize);
    }

    void recordRoot(long root, long node) {
         this.root.set(node,root);
    }

    public long minComponentSize(long node){
        var candidate1= minSplitChildSize.get(node);
        if (root.get(node)==node){
            return candidate1;
        }
        var candidate2 =  totalSize.get(root.get(node)) - totalSize.get(node) + sumOfChildrenWithRoot.get(node);
        return  Math.min(candidate2, candidate1);
    }

    public long maxComponentSize(long node){
        var candidate1= maxSplitChildSize.get(node);
        var candidate2 =  totalSize.get(root.get(node)) - totalSize.get(node)+ sumOfChildrenWithRoot.get(node);
        return  Math.max(candidate2, candidate1);
    }

   public long remainingComponents(long node){
        long remainingComponents = children.get(node);
        if (root.get(node)!=node){
            remainingComponents++;
        }
        return remainingComponents;
    }

}
