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
package org.neo4j.gds.core.loading.construction;

import org.neo4j.gds.api.PartialIdMap;
import org.neo4j.gds.core.compress.AdjacencyCompressor;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImportResult;
import org.neo4j.gds.utils.AutoCloseableThreadLocal;

import java.util.Optional;
import java.util.function.LongConsumer;
import java.util.stream.Stream;

public class RelationshipsBuilder {

    private final PartialIdMap idMap;
    private final SingleTypeRelationshipsBuilder singleTypeRelationshipsBuilder;
    private final AutoCloseableThreadLocal<ThreadLocalRelationshipsBuilder> threadLocalRelationshipsBuilders;

    RelationshipsBuilder(SingleTypeRelationshipsBuilder singleTypeRelationshipsBuilder) {
        this.singleTypeRelationshipsBuilder = singleTypeRelationshipsBuilder;
        this.idMap = singleTypeRelationshipsBuilder.partialIdMap();
        this.threadLocalRelationshipsBuilders = AutoCloseableThreadLocal.withInitial(singleTypeRelationshipsBuilder::threadLocalRelationshipsBuilder);
    }

    public void add(long source, long target) {
        addFromInternal(idMap.toMappedNodeId(source), idMap.toMappedNodeId(target));
    }

    public void add(long source, long target, double relationshipPropertyValue) {
        addFromInternal(
            idMap.toMappedNodeId(source),
            idMap.toMappedNodeId(target),
            relationshipPropertyValue
        );
    }

    public void add(long source, long target, double[] relationshipPropertyValues) {
        addFromInternal(
            idMap.toMappedNodeId(source),
            idMap.toMappedNodeId(target),
            relationshipPropertyValues
        );
    }

    public <T extends Relationship> void add(Stream<T> relationshipStream) {
        relationshipStream.forEach(this::add);
    }

    public <T extends Relationship> void add(T relationship) {
        add(relationship.sourceNodeId(), relationship.targetNodeId(), relationship.property());
    }

    public <T extends Relationship> void addFromInternal(Stream<T> relationshipStream) {
        relationshipStream.forEach(this::addFromInternal);
    }

    public <T extends Relationship> void addFromInternal(T relationship) {
        addFromInternal(relationship.sourceNodeId(), relationship.targetNodeId(), relationship.property());
    }

    public void addFromInternal(long source, long target) {
        this.threadLocalRelationshipsBuilders.get().addRelationship(source, target);
    }

    public void addFromInternal(long source, long target, double relationshipPropertyValue) {
        this.threadLocalRelationshipsBuilders.get().addRelationship(
            source,
            target,
            relationshipPropertyValue
        );
    }

    public void addFromInternal(long source, long target, double[] relationshipPropertyValues) {
        this.threadLocalRelationshipsBuilders.get().addRelationship(
            source,
            target,
            relationshipPropertyValues
        );
    }

    public SingleTypeRelationshipImportResult build() {
        return build(Optional.empty(), Optional.empty());
    }

    /**
     * @param mapper             A mapper to transform values before compressing them. Implementations must be thread-safe.
     * @param drainCountConsumer A consumer which is called once a {@link org.neo4j.gds.core.loading.ChunkedAdjacencyLists}
     *                           has been drained and its contents are written to the adjacency list. The consumer receives the number
     *                           of relationships that have been written. Implementations must be thread-safe.
     */
    public SingleTypeRelationshipImportResult build(
        Optional<AdjacencyCompressor.ValueMapper> mapper,
        Optional<LongConsumer> drainCountConsumer
    ) {
        this.threadLocalRelationshipsBuilders.close();
        return this.singleTypeRelationshipsBuilder.build(mapper, drainCountConsumer);
    }

    public interface Relationship {
        long sourceNodeId();

        long targetNodeId();

        double property();
    }
}
