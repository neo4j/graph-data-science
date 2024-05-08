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
package org.neo4j.gds.ml.core.subgraph;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongIntHashMap;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.carrotsearch.hppc.cursors.LongIntCursor;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.Estimate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class LocalIdMap {
    private final LongArrayList originalIds;
    private final LongIntHashMap originalToInternalIdMap;

    public static MemoryEstimation memoryEstimation(int numberOfClasses) {
        return MemoryEstimations.builder(LocalIdMap.class.getSimpleName())
            .fixed("original IDs", Estimate.sizeOfLongArray(numberOfClasses))
            .fixed("id mapping", Estimate.sizeOfLongArray(numberOfClasses) + Estimate.sizeOfIntArray(numberOfClasses))
            .build();
    }

    public LocalIdMap() {
        this.originalIds = new LongArrayList();
        this.originalToInternalIdMap = new LongIntHashMap();
    }

    public static LocalIdMap of(long... originalIds) {
        var idMap = new LocalIdMap();
        Arrays.stream(originalIds).forEach(idMap::toMapped);

        return idMap;
    }

    public static LocalIdMap of(Collection<Long> classes) {
        var classIdMap = new LocalIdMap();
        classes.forEach(classIdMap::toMapped);

        return classIdMap;
    }

    public static LocalIdMap ofSorted(long[] classes) {
        var classIdMap = new LocalIdMap();

        Arrays.sort(classes);

        for (long clazz :classes){
            classIdMap.toMapped(clazz);
        }

        return classIdMap;
    }

    public int toMapped(long originalId) {
        if (originalToInternalIdMap.containsKey(originalId)) {
            return originalToInternalIdMap.get(originalId);
        }
        originalToInternalIdMap.put(originalId, originalToInternalIdMap.size());
        originalIds.add(originalId);
        return originalToInternalIdMap.get(originalId);
    }

    public long toOriginal(int internalId) {
        return originalIds.get(internalId);
    }

    public long[] originalIds() {
        return originalIds.toArray();
    }

    public List<Long> originalIdsList() {
        var list = new ArrayList<Long>();
        originalIds.forEach((Consumer<LongCursor>) longCursor -> list.add(longCursor.value));
        return list;
    }

    //This method is not order preserving because internally spliterator is not
    public Stream<LongIntCursor> getMappings() {
        return StreamSupport.stream(originalToInternalIdMap.spliterator(), false);
    }

    public int size() {
        assert originalIds.size() == originalToInternalIdMap.size();
        return originalIds.size();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LocalIdMap that = (LocalIdMap) o;
        return Arrays.equals(originalIds(), that.originalIds());
    }

    @Override
    public int hashCode() {
        return Objects.hash(originalIds, originalToInternalIdMap);
    }

    @Override
    public String toString() {
        return "LocalIdMap{" +
               "originalIds=" + originalIds +
               ", originalToInternalIdMap=" + originalToInternalIdMap +
               '}';
    }
}
