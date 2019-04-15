/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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
package org.neo4j.graphalgo.core.utils.container;


import java.util.Arrays;
import java.util.function.IntPredicate;

/**
 * container for assigning nodeIds to arbitrary buckets
 *
 * @author mknblch
 */
public class Buckets {

    private final int[] buckets;

    public Buckets(int capacity) {
        buckets = new int[capacity];
        reset();
    }

    /**
     * reset all buckets
     */
    public void reset() {
        Arrays.fill(buckets, -1);
    }

    /**
     * check if any bucket has nodes left
     *
     * @return if the no nodes left, false otherwise
     */
    public boolean isEmpty() {
        for (int i = 0; i < buckets.length; i++) {
            if (buckets[i] != -1) {
                return false;
            }
        }
        return true;
    }

    /**
     * assign bucket to nodeId
     *
     * @param nodeId the node id
     * @param bucket the bucket index
     */
    public void set(int nodeId, int bucket) {
        buckets[nodeId] = bucket;
    }

    /**
     * find smallest non empty bucket index
     *
     * @return the index
     */
    public int nextNonEmptyBucket() {
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < buckets.length; i++) {
            int bucket = buckets[i];
            if (bucket == -1) {
                continue;
            }
            if (bucket < min) {
                min = bucket;
            }
        }
        return min;
    }

    /**
     * iterate over each node in bucket and call consumer.
     * also clears the bucket before executing the consumer.
     *
     * @param bucket   the bucket index
     * @param consumer the nodeConsumer
     */
    public void forEachInBucket(int bucket, IntPredicate consumer) {
        for (int nodeId = 0; nodeId < buckets.length; nodeId++) {
            int tb = buckets[nodeId];
            if (tb == bucket) {
                buckets[nodeId] = -1; // clear bucket
                if (!consumer.test(nodeId)) {
                    return;
                }
            }
        }
    }
}
