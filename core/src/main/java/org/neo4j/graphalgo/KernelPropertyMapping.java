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
package org.neo4j.graphalgo;

import org.neo4j.graphalgo.core.DeduplicationStrategy;

public class KernelPropertyMapping {
    public static final KernelPropertyMapping EMPTY_WEIGHT_PROPERTY = new KernelPropertyMapping(
            "",
            -1,
            0.0,
            "",
            DeduplicationStrategy.DEFAULT);

    public final String propertyIdentifier;
    public final int propertyKeyId;
    public final double defaultValue;
    public final String neoPropertyName;
    public final DeduplicationStrategy deduplicationStrategy;

    public KernelPropertyMapping(
            String propertyIdentifier,
            int propertyKeyId,
            double defaultValue,
            String neoPropertyName,
            DeduplicationStrategy deduplicationStrategy) {
        this.propertyIdentifier = propertyIdentifier;
        this.propertyKeyId = propertyKeyId;
        this.defaultValue = defaultValue;
        this.neoPropertyName = neoPropertyName;
        this.deduplicationStrategy = deduplicationStrategy;
    }
}
