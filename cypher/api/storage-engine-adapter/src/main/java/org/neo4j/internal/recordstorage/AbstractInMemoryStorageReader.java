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
package org.neo4j.internal.recordstorage;

import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.AllRelationshipsScan;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageSchemaReader;
import org.neo4j.token.TokenHolders;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public abstract class AbstractInMemoryStorageReader implements StorageReader {

    protected final GraphStore graphStore;
    protected final TokenHolders tokenHolders;
    protected final CountsAccessor counts;
    private final Map<Class<?>, Object> dependantState;
    private boolean closed;

    public AbstractInMemoryStorageReader(
        GraphStore graphStore,
        TokenHolders tokenHolders,
        CountsAccessor counts
    ) {
        this.graphStore = graphStore;

        this.tokenHolders = tokenHolders;
        this.counts = counts;
        this.dependantState = new ConcurrentHashMap<>();
    }

    @Override
    public Iterator<IndexDescriptor> indexGetForSchema(SchemaDescriptor descriptor) {
        return Collections.emptyIterator();
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel(int labelId) {
        return Collections.emptyIterator();
    }

    private IndexDescriptor getLabelIndexDescriptor() {
        return IndexDescriptor.NO_INDEX;
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForRelationshipType(int relationshipType) {
        return Collections.emptyIterator();
    }

    @Override
    public IndexDescriptor indexGetForName(String name) {
        return IndexDescriptor.NO_INDEX;
    }

    @Override
    public ConstraintDescriptor constraintGetForName(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean indexExists(IndexDescriptor index) {
        return false;
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll() {
        return Collections.emptyIterator();
    }

    @Override
    public Collection<IndexDescriptor> valueIndexesGetRelated(
        long[] tokens, int propertyKeyId, EntityType entityType
    ) {
        return valueIndexesGetRelated(tokens, new int[]{propertyKeyId}, entityType);
    }

    @Override
    public Collection<IndexDescriptor> valueIndexesGetRelated(
        long[] tokens, int[] propertyKeyIds, EntityType entityType
    ) {
        return Collections.emptyList();
    }

    @Override
    public Collection<IndexBackedConstraintDescriptor> uniquenessConstraintsGetRelated(
        long[] labels,
        int propertyKeyId,
        EntityType entityType
    ) {
        return Collections.emptyList();
    }

    @Override
    public Collection<IndexBackedConstraintDescriptor> uniquenessConstraintsGetRelated(
        long[] labels,
        int[] propertyKeyIds,
        EntityType entityType
    ) {
        return Collections.emptyList();
    }

    @Override
    public boolean hasRelatedSchema(long[] labels, int propertyKey, EntityType entityType) {
        return false;
    }

    @Override
    public boolean hasRelatedSchema(int label, EntityType entityType) {
        return false;
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForSchema(SchemaDescriptor descriptor) {
        return Collections.emptyIterator();
    }

    @Override
    public boolean constraintExists(ConstraintDescriptor descriptor) {
        return false;
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabel(int labelId) {
        return Collections.emptyIterator();
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType(int typeId) {
        return Collections.emptyIterator();
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAll() {
        return Collections.emptyIterator();
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId(IndexDescriptor index) {
        return null;
    }

    @Override
    public long countsForNode(int labelId, CursorContext cursorContext) {
        return counts.nodeCount(labelId, cursorContext);
    }

    @Override
    public long countsForRelationship(int startLabelId, int typeId, int endLabelId, CursorContext cursorContext) {
        return counts.relationshipCount(startLabelId, typeId, endLabelId, cursorContext);
    }

    @Override
    public long nodesGetCount(CursorContext cursorContext) {
        return graphStore.nodeCount();
    }

    @Override
    public int labelCount() {
        return graphStore.nodes().availableNodeLabels().size();
    }

    @Override
    public int propertyKeyCount() {
        int nodePropertyCount = graphStore
            .schema()
            .nodeSchema()
            .properties()
            .values()
            .stream()
            .mapToInt(map -> map.keySet().size())
            .sum();
        int relPropertyCount = graphStore
            .schema()
            .relationshipSchema()
            .properties()
            .values()
            .stream()
            .mapToInt(map -> map.keySet().size())
            .sum();
        return nodePropertyCount + relPropertyCount;
    }

    @Override
    public int relationshipTypeCount() {
        return graphStore.schema().relationshipSchema().properties().keySet().size();
    }

    @Override
    public <T> T getOrCreateSchemaDependantState(Class<T> type, Function<StorageReader, T> factory) {
        return type.cast(dependantState.computeIfAbsent(type, key -> factory.apply(this)));
    }

    @Override
    public AllNodeScan allNodeScan() {
        return new InMemoryNodeScan();
    }

    @Override
    public AllRelationshipsScan allRelationshipScan() {
        return new RecordRelationshipScan();
    }

    @Override
    public void close() {
        assert !closed;
        closed = true;
    }

    @Override
    public StorageSchemaReader schemaSnapshot() {
        return this;
    }

    @Override
    public TokenNameLookup tokenNameLookup() {
        return tokenHolders;
    }

    protected boolean nodeExists(long id) {
        var originalId = graphStore.nodes().toOriginalNodeId(id);
        return graphStore.nodes().contains(originalId);
    }
}
