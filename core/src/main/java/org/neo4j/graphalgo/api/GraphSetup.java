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
package org.neo4j.graphalgo.api;

import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;
import org.neo4j.logging.Log;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * DTO to ease the use of the GraphFactory-CTor.
 * Contains setup options for loading the graph from Neo4j.
 */
public class GraphSetup {

    // user name
    public final String username;
    // graph name
    public final String name;
    // start label type. null means any label.
    public final String startLabel;
    // end label type (not yet implemented).
    public final String endLabel;
    // relationtype name. null means any relation.
    public final String relationshipType;
    // direction for loading the graph.
    public final Direction direction;
    // load incoming relationships.
    public final boolean loadIncoming;
    // load outgoing relationships.
    public final boolean loadOutgoing;
    // load outgoing and incoming relationships.
    public final boolean loadBoth;
    // default property is used for relationships if a property is not set.
    @Deprecated
    public final Optional<Double> relationshipDefaultPropertyValue;

    public final Map<String, Object> params;

    public final Log log;
    public final long logMillis;
    public final AllocationTracker tracker;
    public final TerminationFlag terminationFlag;

    // the executor service for parallel execution. null means single threaded evaluation.
    public final ExecutorService executor;
    // concurrency level
    public final int concurrency;
    // batchSize for parallel computation
    public final int batchSize;

    // tells whether the underlying array should be sorted during import
    public final boolean sort;
    // in/out adjacencies are allowed to be merged into an undirected view of the graph
    public final boolean loadAsUndirected;

    public final PropertyMappings nodePropertyMappings;
    public final PropertyMappings relationshipPropertyMappings;
    public final DeduplicationStrategy deduplicationStrategy;

    /**
     * main ctor
     *
     * @param startLabel                 the start label. null means any label.
     * @param endLabel                   not implemented yet
     * @param relationshipType           the relation type identifier. null for any relationship
     * @param executor                   the executor. null means single threaded evaluation
     * @param batchSize                  batch size for parallel loading
     * @param deduplicationStrategy      strategy for handling relationship duplicates
     * @param sort                       true if relationships should stored in sorted ascending order
     */
    public GraphSetup(
            String username,
            String startLabel,
            String endLabel,
            String relationshipType,
            Direction direction,
            Map<String, Object> params,
            ExecutorService executor,
            int concurrency,
            int batchSize,
            DeduplicationStrategy deduplicationStrategy,
            Log log,
            long logMillis,
            boolean sort,
            boolean loadAsUndirected,
            AllocationTracker tracker,
            TerminationFlag terminationFlag,
            String name,
            PropertyMappings nodePropertyMappings,
            PropertyMappings relationshipPropertyMappings) {

        this.username = username;
        this.startLabel = startLabel;
        this.endLabel = endLabel;
        this.relationshipType = relationshipType;
        this.loadOutgoing = direction == Direction.OUTGOING || direction == Direction.BOTH;
        this.loadIncoming = direction == Direction.INCOMING || direction == Direction.BOTH;
        this.loadBoth = loadOutgoing && loadIncoming;
        this.direction = direction;
        this.relationshipDefaultPropertyValue = relationshipPropertyMappings.defaultWeight();
        this.params = params == null ? Collections.emptyMap() : params;
        this.executor = executor;
        this.concurrency = concurrency;
        this.batchSize = batchSize;
        this.deduplicationStrategy = deduplicationStrategy;
        this.log = log;
        this.logMillis = logMillis;
        this.sort = sort;
        this.loadAsUndirected = loadAsUndirected;
        this.tracker = tracker;
        this.terminationFlag = terminationFlag;
        this.name = name;
        this.nodePropertyMappings = nodePropertyMappings;
        this.relationshipPropertyMappings = relationshipPropertyMappings;
    }

    public boolean loadConcurrent() {
        return executor != null;
    }

    public int concurrency() {
        if (!loadConcurrent()) {
            return 1;
        }
        return concurrency;
    }

    public boolean shouldLoadRelationshipProperties() {
        return relationshipPropertyMappings.hasMappings();
    }

    public boolean loadAnyLabel() {
        return startLabel == null || startLabel.isEmpty();
    }

    public boolean loadAnyRelationshipType() {
        return relationshipType == null;
    }
}
