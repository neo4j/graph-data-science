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
package org.neo4j.gds.ml.pipeline.linkPipeline;

/**
 * Responsible for appending features given a specific graph.
 * Instances should not be reused between different graphs.
 */
public interface LinkFeatureAppender {
    /**
     * Adds additional features to linkFeatures
     *
     * @param linkFeatures features for the pair (source, target)
     * @param offset the start offset in each double[] where the features should be added
     */
    void appendFeatures(long source, long target, double[] linkFeatures, int offset);

    /**
     *
     * @return the number of entries to append in to the existing features
     */
    int dimension();

    default boolean isSymmetric() {
        return true;
    }
}
