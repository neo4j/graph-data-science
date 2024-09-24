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
package org.neo4j.gds.compat;

import org.jetbrains.annotations.Nullable;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.io.pagecache.context.CursorContext;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

public final class InternalReadOps {

    public static long getHighestPossibleNodeCount(@Nullable IdGeneratorFactory idGeneratorFactory) {
        return InternalReadOps.findValidIdGeneratorsStream(
                idGeneratorFactory,
                RecordIdType.NODE,
                BlockFormat.INSTANCE.nodeType,
                BlockFormat.INSTANCE.dynamicNodeType
            )
            .mapToLong(IdGenerator::getHighId)
            .max()
            .orElseThrow(InternalReadOps::unsupportedStoreFormatException);
    }

    public static void reserveNeo4jIds(
        @Nullable IdGeneratorFactory generatorFactory,
        int size,
        CursorContext cursorContext
    ) {
        var idGenerator = InternalReadOps.findValidIdGeneratorsStream(
                generatorFactory,
                RecordIdType.NODE,
                BlockFormat.INSTANCE.nodeType,
                BlockFormat.INSTANCE.dynamicNodeType
            )
            .findFirst().orElseThrow(InternalReadOps::unsupportedStoreFormatException);

        idGenerator.nextConsecutiveIdRange(size, false, cursorContext);
    }

    public static Stream<IdGenerator> findValidIdGeneratorsStream(
        @Nullable IdGeneratorFactory idGeneratorFactory,
        IdType... idTypes
    ) {
        if (idGeneratorFactory == null || idTypes.length == 0) {
            return Stream.empty();
        }
        return Arrays.stream(idTypes).mapMulti((idType, downstream) -> {
            try {
                var idGenerator = idGeneratorFactory.get(idType);
                if (idGenerator != null) {
                    downstream.accept(idGenerator);
                }
            } catch (Exception ignored) {
            }
        });
    }

    public static IllegalStateException unsupportedStoreFormatException() {
        return new IllegalStateException(
            "Unsupported store format for GDS; GDS cannot read data from this database. " +
                "Please try to use Cypher projection instead.");
    }

    private static final class BlockFormat {
        private static final BlockFormat INSTANCE = new BlockFormat();

        private org.neo4j.internal.id.IdType nodeType = null;
        private org.neo4j.internal.id.IdType dynamicNodeType = null;

        BlockFormat() {
            try {
                var blockIdType = Class.forName("com.neo4j.internal.blockformat.BlockIdType");
                var blockTypes = Objects.requireNonNull(blockIdType.getEnumConstants());
                for (Object blockType : blockTypes) {
                    var type = (Enum<?>) blockType;
                    switch (type.name()) {
                        case "NODE" -> this.nodeType = (org.neo4j.internal.id.IdType) type;
                        case "DYNAMIC_NODE" -> this.dynamicNodeType = (org.neo4j.internal.id.IdType) type;
                    }
                }
            } catch (ClassNotFoundException | NullPointerException | ClassCastException ignored) {
            }
        }
    }

    private InternalReadOps() {
        throw new UnsupportedOperationException("No instances");
    }
}
