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
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.RelationshipProjection;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.core.DeduplicationStrategy;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.Log;
import org.neo4j.stream.Streams;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * DTO to ease the use of the GraphFactory-CTor.
 * Contains setup options for loading the graph from Neo4j.
 */
public class GraphSetup {

    private final GraphCreateConfig createConfig;

    private final Map<String, Object> params;

    private final Log log;
    private final long logMillis;
    private final AllocationTracker tracker;
    private final TerminationFlag terminationFlag;

    // the executor service for parallel execution. null means single threaded evaluation.
    private final ExecutorService executor;
    // batchSize for parallel computation
    private final int batchSize;

    /**
     * @param executor  the executor. null means single threaded evaluation
     * @param batchSize batch size for parallel loading
     */
    public GraphSetup(
        Map<String, Object> params,
        ExecutorService executor,
        int batchSize,
        Log log,
        long logMillis,
        AllocationTracker tracker,
        TerminationFlag terminationFlag,
        GraphCreateConfig createConfig
    ) {
        this.params = params == null ? Collections.emptyMap() : params;
        this.executor = executor;
        this.batchSize = batchSize;
        this.log = log;
        this.logMillis = logMillis;
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
        return createConfig.nodeProjection().labelProjection().orElse("");
    }

    public @NotNull String relationshipType() {
        return createConfig.relationshipProjection().typeFilter();
    }

    public NodeProjections nodeProjections() {
        return createConfig.nodeProjection();
    }

    public RelationshipProjections relationshipProjections() {
        return createConfig.relationshipProjection();
    }

    /**
     * @deprecated There is no global direction anymore
     */
    @Deprecated
    public Direction direction() {
        Direction direction = createConfig
            .relationshipProjection()
            .allFilters()
            .stream()
            .map(RelationshipProjection::projection)
            .reduce(null, this::accumulateDirections, (d1, d2) -> d1);
        return direction == null ? Direction.OUTGOING : direction;
    }

    private Direction accumulateDirections(@Nullable Direction current, Projection projection) {
        switch (projection) {
            case NATURAL:
                if (current == null) {
                    return Direction.OUTGOING;
                }
                return current == Direction.INCOMING ? Direction.BOTH : current;
            case UNDIRECTED:
                if (current == null || current == Direction.OUTGOING) {
                    return Direction.OUTGOING;
                }
                throw new IllegalArgumentException("Cannot mix undirection with " + projection);
            case REVERSE:
                if (current == null) {
                    return Direction.INCOMING;
                }
                return current == Direction.OUTGOING ? Direction.BOTH : current;
            default:
                throw new IllegalArgumentException("Unknown projection " + projection);
        }
    }

    /**
     * @deprecated There is no global direction anymore
     */
    @Deprecated
    public boolean loadIncoming() {
        return direction() == Direction.INCOMING || direction() == Direction.BOTH;
    }

    /**
     * @deprecated There is no global direction anymore
     */
    @Deprecated
    public boolean loadOutgoing() {
        return direction() == Direction.OUTGOING || direction() == Direction.BOTH;
    }

    /**
     * @deprecated There is no global direction anymore
     */
    @Deprecated
    public boolean loadAsUndirected() {
        return createConfig
            .relationshipProjection()
            .allFilters()
            .stream()
            .map(RelationshipProjection::projection)
            .anyMatch(p -> p == Projection.UNDIRECTED);
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
        Map<String, List<PropertyMapping>> groupedPropertyMappings = createConfig.nodeProjection()
            .allProjections()
            .stream()
            .flatMap(e -> e.properties().stream())
            .collect(Collectors.groupingBy(PropertyMapping::propertyKey));

        PropertyMappings.Builder builder = PropertyMappings.builder();
        groupedPropertyMappings.values().stream()
            .map(Iterables::first)
            .forEach(builder::addMapping);
        return builder.build();
    }

    public PropertyMappings nodePropertyMappings(ElementIdentifier identifier) {
        return createConfig.nodeProjection().getProjection(identifier).properties();
    }

    /**
     * @deprecated There is no global relationship property configuration anymore
     */
    @Deprecated
    public PropertyMappings relationshipPropertyMappings() {
        Map<String, List<PropertyMapping>> groupedPropertyMappings = createConfig.relationshipProjection()
            .allFilters()
            .stream()
            .flatMap(e -> e.properties().stream())
            .collect(Collectors.groupingBy(PropertyMapping::propertyKey));

        PropertyMappings.Builder builder = PropertyMappings.builder();
        groupedPropertyMappings.values().stream()
            .map(Iterables::first)
            .forEach(builder::addMapping);
        return builder.build();
    }

    public PropertyMappings relationshipPropertyMappings(ElementIdentifier identifier) {
        return createConfig.relationshipProjection().getFilter(identifier).properties();
    }

    /**
     * @deprecated There is no global relationship deduplication strategy anymore
     */
    @Deprecated
    public DeduplicationStrategy deduplicationStrategy() {
        return createConfig
            .relationshipProjection()
            .allFilters()
            .stream()
            .map(RelationshipProjection::aggregation)
            .findFirst()
            .orElse(DeduplicationStrategy.DEFAULT);
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
