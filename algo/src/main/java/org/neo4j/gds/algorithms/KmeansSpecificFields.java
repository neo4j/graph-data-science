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
package org.neo4j.gds.algorithms;

import java.util.List;
import java.util.Map;

public class KmeansSpecificFields implements CommunityStatisticsSpecificFields {
    private final Map<String, Object> communityDistribution;
    private final List<List<Double>> centroids;
    private final double averageDistanceToCentroid;
    private final double averageSilhouette;


    public static final KmeansSpecificFields EMPTY = new KmeansSpecificFields(Map.of(), List.of(), 0.0d, 0.0d);

    public KmeansSpecificFields(
        Map<String, Object> communityDistribution,
        List<List<Double>> centroids,
        double averageDistanceToCentroid,
        double averageSilhouette
    ) {
        this.communityDistribution = communityDistribution;
        this.centroids = centroids;
        this.averageDistanceToCentroid = averageDistanceToCentroid;
        this.averageSilhouette = averageSilhouette;
    }

    public double averageDistanceToCentroid() {
        return averageDistanceToCentroid;
    }

    public double averageSilhouette() {
        return averageSilhouette;
    }

    public List<List<Double>> centroids() {
        return centroids;
    }

    @Override
    public long communityCount() {
        return 0;
    }

    public Map<String, Object> communityDistribution() {
        return communityDistribution;
    }
}
