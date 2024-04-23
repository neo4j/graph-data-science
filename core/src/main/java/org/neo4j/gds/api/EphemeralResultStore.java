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
package org.neo4j.gds.api;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.github.benmanes.caffeine.cache.Ticker;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.concurrency.ExecutorServiceUtil;
import org.neo4j.gds.core.utils.ClockService;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.LongUnaryOperator;
import java.util.stream.Stream;

public class EphemeralResultStore implements ResultStore {

    private static final String NO_PROPERTY_KEY = "";
    private static final List<String> NO_PROPERTIES_LIST = List.of(NO_PROPERTY_KEY);
    static final Duration CACHE_EVICTION_DURATION = Duration.of(10, ChronoUnit.MINUTES);

    private final Cache<NodeKey, NodePropertyValues> nodeProperties;
    private final Cache<String, NodeLabelEntry> nodeIdsByLabel;
    private final Cache<RelationshipKey, RelationshipEntry> relationships;
    private final Cache<RelationshipKey, RelationshipStreamEntry> relationshipStreams;
    private final Map<RelationshipKey, RelationshipIteratorEntry> relationshipIterators;

    public EphemeralResultStore() {
        var singleThreadScheduler = ExecutorServiceUtil.createSingleThreadScheduler("GDS-ResultStore");
        this.nodeProperties = createCache(singleThreadScheduler);
        this.nodeIdsByLabel = createCache(singleThreadScheduler);
        this.relationships = createCache(singleThreadScheduler);
        this.relationshipStreams = createCache(singleThreadScheduler);
        this.relationshipIterators = new HashMap<>();
    }

    @Override
    public void addNodePropertyValues(List<String> nodeLabels, String propertyKey, NodePropertyValues propertyValues) {
        this.nodeProperties.put(new NodeKey(nodeLabels, propertyKey), propertyValues);
    }

    @Override
    public NodePropertyValues getNodePropertyValues(List<String> nodeLabels, String propertyKey) {
        return this.nodeProperties.getIfPresent(new NodeKey(nodeLabels, propertyKey));
    }

    @Override
    public void removeNodePropertyValues(List<String> nodeLabels, String propertyKey) {
        this.nodeProperties.invalidate(new NodeKey(nodeLabels, propertyKey));
    }

    @Override
    public void addNodeLabel(String nodeLabel, long nodeCount, LongUnaryOperator toOriginalId) {
        this.nodeIdsByLabel.put(nodeLabel, new NodeLabelEntry(nodeCount, toOriginalId));
    }

    @Override
    public boolean hasNodeLabel(String nodeLabel) {
        return this.nodeIdsByLabel.getIfPresent(nodeLabel) != null;
    }

    @Override
    public NodeLabelEntry getNodeIdsByLabel(String nodeLabel) {
        return this.nodeIdsByLabel.getIfPresent(nodeLabel);
    }

    @Override
    public void removeNodeLabel(String nodeLabel) {
        this.nodeIdsByLabel.invalidate(nodeLabel);
    }

    @Override
    public void addRelationship(String relationshipType, Graph graph, LongUnaryOperator toOriginalId) {
        addRelationship(relationshipType, NO_PROPERTY_KEY, graph, toOriginalId);
    }

    @Override
    public void addRelationship(
        String relationshipType,
        String propertyKey,
        Graph graph,
        LongUnaryOperator toOriginalId
    ) {
        this.relationships.put(new RelationshipKey(relationshipType, List.of(propertyKey)), new RelationshipEntry(graph, toOriginalId));
    }

    @Override
    public void addRelationshipStream(
        String relationshipType,
        List<String> propertyKeys,
        List<ValueType> propertyTypes,
        Stream<ExportedRelationship> relationshipStream,
        LongUnaryOperator toOriginalId
    ) {
        this.relationshipStreams.put(
            new RelationshipKey(relationshipType, propertyKeys),
            new RelationshipStreamEntry(relationshipStream, propertyTypes, toOriginalId)
        );
    }

    @Override
    public RelationshipStreamEntry getRelationshipStream(String relationshipType, List<String> propertyKeys) {
        return this.relationshipStreams.getIfPresent(new RelationshipKey(relationshipType, propertyKeys));
    }

    @Override
    public void removeRelationshipStream(String relationshipType, List<String> propertyKeys) {
        this.relationshipStreams.invalidate(new RelationshipKey(relationshipType, propertyKeys));
    }

    @Override
    public RelationshipEntry getRelationship(String relationshipType) {
        return getRelationship(relationshipType, NO_PROPERTY_KEY);
    }

    @Override
    public RelationshipEntry getRelationship(String relationshipType, String propertyKey) {
        return this.relationships.getIfPresent(new RelationshipKey(relationshipType, List.of(propertyKey)));
    }

    @Override
    public void removeRelationship(String relationshipType) {
        removeRelationship(relationshipType, NO_PROPERTY_KEY);
    }

    @Override
    public void removeRelationship(String relationshipType, String propertyKey) {
        this.relationships.invalidate(new RelationshipKey(relationshipType, List.of(propertyKey)));
    }

    @Override
    public void addRelationshipIterator(
        String relationshipType,
        List<String> propertyKeys,
        CompositeRelationshipIterator relationshipIterator,
        LongUnaryOperator toOriginalId
    ) {
        this.relationshipIterators.put(
            new RelationshipKey(relationshipType, propertyKeys),
            new RelationshipIteratorEntry(relationshipIterator, toOriginalId)
        );
    }

    @Override
    public RelationshipIteratorEntry getRelationshipIterator(String relationshipType, List<String> propertyKeys) {
        return this.relationshipIterators.get(new RelationshipKey(relationshipType, propertyKeys));
    }

    @Override
    public void removeRelationshipIterator(String relationshipType, List<String> propertyKeys) {
        this.relationshipIterators.remove(new RelationshipKey(relationshipType, propertyKeys));
    }

    @Override
    public boolean hasRelationship(String relationshipType) {
        return hasRelationship(relationshipType, NO_PROPERTIES_LIST);
    }

    @Override
    public boolean hasRelationship(String relationshipType, List<String> propertyKeys) {
        return this.relationships.getIfPresent(new RelationshipKey(relationshipType, propertyKeys)) != null;
    }

    @Override
    public boolean hasRelationshipStream(String relationshipType, List<String> propertyKeys) {
        return this.relationshipStreams.getIfPresent(new RelationshipKey(relationshipType, propertyKeys)) != null;
    }

    private static <K, V> Cache<K, V> createCache(ScheduledExecutorService singleThreadScheduler) {
        return Caffeine.newBuilder()
            .expireAfterAccess(CACHE_EVICTION_DURATION)
            .ticker(new ClockServiceWrappingTicker())
            .executor(singleThreadScheduler)
            .scheduler(Scheduler.forScheduledExecutorService(singleThreadScheduler))
            .build();
    }

    private static class ClockServiceWrappingTicker implements Ticker {
        @Override
        public long read() {
            return ClockService.clock().millis() * 1000000;
        }
    }

    @Override
    public boolean hasRelationshipIterator(String relationshipType, List<String> propertyKeys) {
        return this.relationshipIterators.containsKey(new RelationshipKey(relationshipType, propertyKeys));
    }

    private record NodeKey(List<String> nodeLabels, String propertyKey) {}

    private record RelationshipKey(String relationshipType, Collection<String> propertyKeys) {}
}
