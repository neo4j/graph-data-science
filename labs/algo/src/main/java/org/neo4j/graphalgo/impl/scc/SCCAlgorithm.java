/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.scc;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.stream.Stream;

/**
 * unified iface for all scc algorithms regardless of which graph
 * and impl is used.
 *
 * @author mknblch
 */
public interface SCCAlgorithm {

    /**
     * compute scc's
     * @return
     */
    Boolean compute();

    /**
     * get number of components
     * @return
     */
    long getSetCount();

    /**
     * get minimum set size of all components
     * @return
     */
    long getMinSetSize();

    /**
     * get maximum set size of all components
     * @return
     */
    long getMaxSetSize();

    /**
     * return stream of original nodeId to component id mapping
     * @return
     */
    Stream<SCCAlgorithm.StreamResult>  resultStream();

    SCCAlgorithm withProgressLogger(ProgressLogger wrap);

    SCCAlgorithm withTerminationFlag(TerminationFlag wrap);

    /**
     * release inner data structures
     * @return
     */
    void release();

    /**
     * get nodeId to component id mapping
     * either as int[] or hugeLong array
     * @param <V>
     * @return
     */
    <V> V getConnectedComponents();

    /**
     * stream result type
     */
    class StreamResult {

        public final long nodeId;
        public final long partition;

        public StreamResult(long nodeId, long partition) {
            this.nodeId = nodeId;
            this.partition = partition;
        }
    }

    /**
     * returns a initialized SCC algorithm based on which
     * type of graph has been supplied.
     *
     * @param graph
     * @param tracker
     * @return
     */
    static SCCAlgorithm iterativeTarjan(Graph graph, AllocationTracker tracker) {
        return new SCCIterativeTarjan(graph, tracker);
    }
}
