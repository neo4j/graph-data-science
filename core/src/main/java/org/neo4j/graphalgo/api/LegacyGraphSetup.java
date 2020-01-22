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
package org.neo4j.graphalgo.api;

import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;
import org.neo4j.logging.Log;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

public final class LegacyGraphSetup implements GraphSetup {

    // user name
    private final String username;
    // graph name
    private final String name;
    // start label type. null means any label.
    private final String nodeLabel;
    // relationtype name. null means any relation.
    private final String relationshipType;
    // direction for loading the graph.
    private final Direction direction;
    // load incoming relationships.
    private final boolean loadIncoming;
    // load outgoing relationships.
    private final boolean loadOutgoing;
    // default property is used for relationships if a property is not set.
    private final Optional<Double> relationshipDefaultPropertyValue;

    private final Map<String, Object> params;

    private final Log log;
    private final long logMillis;
    private final AllocationTracker tracker;
    private final TerminationFlag terminationFlag;

    // the executor service for parallel execution. null means single threaded evaluation.
    private final ExecutorService executor;
    // concurrency level
    private final int concurrency;
    // batchSize for parallel computation
    private final int batchSize;

    // in/out adjacencies are allowed to be merged into an undirected view of the graph
    private final boolean loadAsUndirected;

    private final PropertyMappings nodePropertyMappings;
    private final PropertyMappings relationshipPropertyMappings;
    private final DeduplicationStrategy deduplicationStrategy;

    /**
     * main ctor
     *
     * @param nodeLabel            the start label. null means any label.
     * @param relationshipType      the relation type identifier. null for any relationship
     * @param executor              the executor. null means single threaded evaluation
     * @param batchSize             batch size for parallel loading
     * @param deduplicationStrategy strategy for handling relationship duplicates
     */
    public LegacyGraphSetup(
        String username,
        String nodeLabel,
        String relationshipType,
        Direction direction,
        Map<String, Object> params,
        ExecutorService executor,
        int concurrency,
        int batchSize,
        DeduplicationStrategy deduplicationStrategy,
        Log log,
        long logMillis,
        boolean loadAsUndirected,
        AllocationTracker tracker,
        TerminationFlag terminationFlag,
        String name,
        PropertyMappings nodePropertyMappings,
        PropertyMappings relationshipPropertyMappings
    ) {

        this.username = username;
        this.nodeLabel = nodeLabel;
        this.relationshipType = relationshipType;
        this.loadOutgoing = direction == Direction.OUTGOING || direction == Direction.BOTH;
        this.loadIncoming = direction == Direction.INCOMING || direction == Direction.BOTH;
        this.direction = direction;
        this.relationshipDefaultPropertyValue = relationshipPropertyMappings.defaultWeight();
        this.params = params == null ? Collections.emptyMap() : params;
        this.executor = executor;
        this.concurrency = concurrency;
        this.batchSize = batchSize;
        this.deduplicationStrategy = deduplicationStrategy;
        this.log = log;
        this.logMillis = logMillis;
        this.loadAsUndirected = loadAsUndirected;
        this.tracker = tracker;
        this.terminationFlag = terminationFlag;
        this.name = name;
        this.nodePropertyMappings = nodePropertyMappings;
        this.relationshipPropertyMappings = relationshipPropertyMappings;
    }

    @Override
    public boolean legacyMode() {
        return true;
    }

    @Override
    public int concurrency() {
        if (!loadConcurrent()) {
            return 1;
        }
        return concurrency;
    }

    @Override
    public String username() {
        return username;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public @NotNull String nodeLabel() {
        return nodeLabel;
    }

    @Override
    public @NotNull String relationshipType() {
        return relationshipType;
    }

    @Override
    public @NotNull RelationshipProjections relationshipProjections() {
        throw new UnsupportedOperationException("Relationship projections are not available in legacy mode");
    }

    @Override
    public Optional<String> nodeQuery() {
        return Optional.of(nodeLabel());
    }

    @Override
    public Optional<String> relationshipQuery() {
        return Optional.of(relationshipType());
    }

    @Override
    public Direction direction() {
        return direction;
    }

    @Override
    public boolean loadIncoming() {
        return loadIncoming;
    }

    @Override
    public boolean loadOutgoing() {
        return loadOutgoing;
    }

    @Override
    public Optional<Double> relationshipDefaultPropertyValue() {
        return relationshipDefaultPropertyValue;
    }

    @Override
    public Map<String, Object> params() {
        return params;
    }

    @Override
    public Log log() {
        return log;
    }

    @Override
    public long logMillis() {
        return logMillis;
    }

    @Override
    public AllocationTracker tracker() {
        return tracker;
    }

    @Override
    public TerminationFlag terminationFlag() {
        return terminationFlag;
    }

    @Override
    public ExecutorService executor() {
        return executor;
    }

    @Override
    public boolean loadAsUndirected() {
        return loadAsUndirected;
    }

    @Override
    public PropertyMappings nodePropertyMappings() {
        return nodePropertyMappings;
    }

    @Override
    public PropertyMappings relationshipPropertyMappings() {
        return relationshipPropertyMappings;
    }

    @Override
    public DeduplicationStrategy deduplicationStrategy() {
        return deduplicationStrategy;
    }

    private boolean loadConcurrent() {
        return executor != null;
    }
}
