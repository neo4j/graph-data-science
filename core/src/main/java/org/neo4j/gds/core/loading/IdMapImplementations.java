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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.core.GdsEdition;
import org.neo4j.gds.utils.GdsFeatureToggles;

public final class IdMapImplementations {

    public static boolean useBitIdMap() {
        return GdsEdition.instance().isOnEnterpriseEdition() && GdsFeatureToggles.USE_BIT_ID_MAP.isEnabled();
    }

    public static NodeMappingBuilder<InternalBitIdMappingBuilder> bitIdMapBuilder() {
        return (idMapBuilder, labelInformationBuilder, graphDimensions, concurrency, checkDuplicateIds, tracker) -> IdMapBuilder.build(
            idMapBuilder,
            labelInformationBuilder,
            tracker
        );
    }

    public static NodeMappingBuilder.Capturing bitIdMapBuilder(InternalBitIdMappingBuilder idMapBuilder) {
        return bitIdMapBuilder().capture(idMapBuilder);
    }

    public static NodeMappingBuilder<InternalSequentialBitIdMappingBuilder> sequentialBitIdMapBuilder() {
        return (idMapBuilder, labelInformationBuilder, graphDimensions, concurrency, checkDuplicateIds, tracker) -> IdMapBuilder.build(
            idMapBuilder,
            labelInformationBuilder,
            tracker
        );
    }

    public static NodeMappingBuilder.Capturing sequentialBitIdMapBuilder(InternalSequentialBitIdMappingBuilder idMapBuilder) {
        return sequentialBitIdMapBuilder().capture(idMapBuilder);
    }

    public static NodeMappingBuilder<InternalHugeIdMappingBuilder> hugeIdMapBuilder() {
        return (idMapBuilder, labelInformationBuilder, graphDimensions, concurrency, checkDuplicateIds, tracker) -> checkDuplicateIds
            ? IdMapBuilder.buildChecked(idMapBuilder, labelInformationBuilder, graphDimensions, concurrency, tracker)
            : IdMapBuilder.build(idMapBuilder, labelInformationBuilder, graphDimensions, concurrency, tracker);
    }

    public static NodeMappingBuilder.Capturing hugeIdMapBuilder(InternalHugeIdMappingBuilder idMapBuilder) {
        return hugeIdMapBuilder().capture(idMapBuilder);
    }

    private IdMapImplementations() {}
}
