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

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphdb.Direction;
import org.neo4j.logging.Log;
import org.neo4j.stream.Streams;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * DTO to ease the use of the GraphFactory-CTor.
 * Contains setup options for loading the graph from Neo4j.
 */
public class GraphSetup {

    private final GraphCreateConfig createConfig;

    // direction for loading the graph.
    private final Direction direction;
    // load incoming relationships.
    private final boolean loadIncoming;
    // load outgoing relationships.
    private final boolean loadOutgoing;
    // default property is used for relationships if a property is not set.

    private final Map<String, Object> params;

    private final Log log;
    private final long logMillis;
    private final AllocationTracker tracker;
    private final TerminationFlag terminationFlag;

    // the executor service for parallel execution. null means single threaded evaluation.
    private final ExecutorService executor;
    // batchSize for parallel computation
    private final int batchSize;

    // in/out adjacencies are allowed to be merged into an undirected view of the graph
    private final boolean loadAsUndirected;

    private final DeduplicationStrategy deduplicationStrategy;

    /**
     * main ctor
     *  @param executor                   the executor. null means single threaded evaluation
     * @param batchSize                  batch size for parallel loading
     * @param deduplicationStrategy      strategy for handling relationship duplicates
     * @param createConfig
     */
    public GraphSetup(
        Direction direction,
        Map<String, Object> params,
        ExecutorService executor,
        int batchSize,
        DeduplicationStrategy deduplicationStrategy,
        Log log,
        long logMillis,
        boolean loadAsUndirected,
        AllocationTracker tracker,
        TerminationFlag terminationFlag,
        GraphCreateConfig createConfig
    ) {
        this.loadOutgoing = direction == Direction.OUTGOING || direction == Direction.BOTH;
        this.loadIncoming = direction == Direction.INCOMING || direction == Direction.BOTH;
        this.direction = direction;
        this.params = params == null ? Collections.emptyMap() : params;
        this.executor = executor;
        this.batchSize = batchSize;
        this.deduplicationStrategy = deduplicationStrategy;
        this.log = log;
        this.logMillis = logMillis;
        this.loadAsUndirected = loadAsUndirected;
        this.tracker = tracker;
        this.terminationFlag = terminationFlag;
        this.createConfig = createConfig;
    }

    public String username() {
        return createConfig.username();
    }

    public String name() {
        return createConfig.graphName();
    }

    public int concurrency() {
        if (!loadConcurrent()) {
            return 1;
        }
        return createConfig.concurrency();
    }

    /**
     * @deprecated This feature is going away and will mean load nothing instead
     */
    @Deprecated
    public boolean loadAnyLabel() {
        return StringUtils.isEmpty(nodeLabel());
    }

    /**
     * @deprecated This feature is going away and will mean load nothing instead
     */
    @Deprecated
    public boolean loadAnyRelationshipType() {
        return StringUtils.isEmpty(relationshipType());
    }

    public @NotNull String nodeLabel() {
        return createConfig.nodeProjection().labelFilter().orElse("");
    }

    public @NotNull String relationshipType() {
        return createConfig.relationshipProjection().typeFilter();
    }

    public NodeProjections nodeFilters() {
        return createConfig.nodeProjection();
    }

    public RelationshipProjections relationshipFilters() {
        return createConfig.relationshipProjection();
    }

    /**
     * @deprecated There is no global direction anymore
     */
    @Deprecated
    public Direction direction() {
        return direction;
    }

    /**
     * @deprecated There is no global direction anymore
     */
    @Deprecated
    public boolean loadIncoming() {
        return loadIncoming;
    }

    /**
     * @deprecated There is no global direction anymore
     */
    @Deprecated
    public boolean loadOutgoing() {
        return loadOutgoing;
    }

    /**
     * @deprecated There is no global direction anymore
     */
    @Deprecated
    public boolean loadAsUndirected() {
        return loadAsUndirected;
    }

    public boolean shouldLoadRelationshipProperties() {
        return createConfig
            .relationshipProjection()
            .allFilters()
            .stream()
            .anyMatch(RelationshipProjection::hasMappings);
    }

    /**
     * @deprecated There is no global relationship property anymore
     */
    @Deprecated
    public Optional<Double> relationshipDefaultPropertyValue() {
        return createConfig.relationshipProjection().allFilters().stream().flatMap(
            f -> Streams.ofOptional(f.properties().defaultWeight())
        ).findFirst();
    }

    /**
     * @deprecated There is no global node property configuration anymore
     */
    @Deprecated
    public PropertyMappings nodePropertyMappings() {
        PropertyMapping[] propertyMappings = createConfig.nodeProjection()
            .allFilters()
            .stream()
            .flatMap(e -> e.properties().stream())
            .toArray(PropertyMapping[]::new);
        return PropertyMappings.of(propertyMappings);
    }

    public PropertyMappings nodePropertyMappings(ElementIdentifier identifier) {
        return createConfig.nodeProjection().getFilter(identifier).properties();
    }

    /**
     * @deprecated There is no global relationship property configuration anymore
     */
    @Deprecated
    public PropertyMappings relationshipPropertyMappings() {
        PropertyMapping[] propertyMappings = createConfig.relationshipProjection()
            .allFilters()
            .stream()
            .flatMap(e -> e.properties().stream())
            .toArray(PropertyMapping[]::new);
        return PropertyMappings.of(propertyMappings);
    }

    public PropertyMappings relationshipPropertyMappings(ElementIdentifier identifier) {
        return createConfig.relationshipProjection().getFilter(identifier).properties();
    }

    /**
     * @deprecated There is no global relationship deduplication strategy anymore
     */
    @Deprecated
    public DeduplicationStrategy deduplicationStrategy() {
        return deduplicationStrategy;
    }

    public Map<String, Object> params() {
        return params;
    }

    public Log log() {
        return log;
    }

    long logMillis() {
        return logMillis;
    }

    public AllocationTracker tracker() {
        return tracker;
    }

    public TerminationFlag terminationFlag() {
        return terminationFlag;
    }

    public ExecutorService executor() {
        return executor;
    }

    public int batchSize() {
        return batchSize;
    }

    private boolean loadConcurrent() {
        return executor != null;
    }
}
