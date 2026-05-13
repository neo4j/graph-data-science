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
package org.neo4j.gds.scaling;

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.scaling.compute.L1NormComputer;
import org.neo4j.gds.scaling.compute.L2NormComputer;
import org.neo4j.gds.scaling.compute.MinMaxAverageComputer;
import org.neo4j.gds.scaling.compute.StdComputer;
import org.neo4j.gds.scaling.scale.Center;
import org.neo4j.gds.scaling.scale.L1Norm;
import org.neo4j.gds.scaling.scale.L2Norm;
import org.neo4j.gds.scaling.scale.LogScaler;
import org.neo4j.gds.scaling.scale.Max;
import org.neo4j.gds.scaling.scale.Mean;
import org.neo4j.gds.scaling.scale.MinMax;
import org.neo4j.gds.scaling.scale.NoneScaler;
import org.neo4j.gds.scaling.scale.ScalarScaler;
import org.neo4j.gds.scaling.scale.StdScore;
import org.neo4j.gds.scaling.scale.Zero;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public final class ScalerFactory {
    private static final double CLOSE_TO_ZERO = 1e-15;
    private final ScalerType type;
    private final String name;
    private final double offset;

    private ScalerFactory(ScalerType type, String name, double offset) {
        this.type = type;
        this.name = name;
        this.offset = offset;
    }

    public static ScalerFactory of(ScalerType type, String name, double offset) {
        return new ScalerFactory(type, name, offset);
    }

    public static ScalerFactory of(ScalerType type, double offset) {
        return new ScalerFactory(type, type.scalerName(), offset);
    }

    public static ScalerFactory of(ScalerType type) {
        return of(type, 0.0);
    }

    public String name() { return name; }

    public ScalerType type() { return type; }

    public static Center createCenter(NodePropertyValues properties, MinMaxAverageComputer.Result computed) {
        return new Center(properties, computed.average());
    }

    public static ScalarScaler createMean(NodePropertyValues properties, MinMaxAverageComputer.Result computed) {
        var statistics = Map.of(
            "min", List.of(computed.min()),
            "max", List.of(computed.max()),
            "avg", List.of(computed.average()));
        double maxMinDiff = computed.max() - computed.min();
        return maxMinDiff < CLOSE_TO_ZERO ? Zero.of(statistics) : new Mean(properties, statistics, computed.average(), maxMinDiff);
    }

    public static ScalarScaler createMax(NodePropertyValues properties, MinMaxAverageComputer.Result computed) {
        double absMax = Math.max(Math.abs(computed.min()), Math.abs(computed.max()));
        var statistics = Map.of("absMax", List.of(absMax));
        return absMax < CLOSE_TO_ZERO ? Zero.of(statistics) : new Max(properties, statistics, absMax);
    }

    public static ScalarScaler createMinMax(NodePropertyValues properties, MinMaxAverageComputer.Result computed) {
        var statistics = Map.of(
            "min", List.of(computed.min()),
            "max", List.of(computed.max()));
        var diff = computed.max() - computed.min();
        return diff < CLOSE_TO_ZERO ? Zero.of(statistics) : new MinMax(properties, statistics, computed.min(), diff);
    }

    public static ScalarScaler createL1Norm(NodePropertyValues properties, L1NormComputer.Result computed) {
        return computed.sum() < CLOSE_TO_ZERO ? Zero.of() : new L1Norm(properties, computed.sum());
    }

    public static ScalarScaler createL2Norm(NodePropertyValues properties, L2NormComputer.Result computed) {
        return computed.length() < CLOSE_TO_ZERO ? Zero.of() : new L2Norm(properties, computed.length());
    }

    public static ScalarScaler createStd(NodePropertyValues properties, StdComputer.Result computed) {
        var statistics = Map.of(
            "avg", List.of(computed.average()),
            "std", List.of(computed.std()));
        return computed.std() < CLOSE_TO_ZERO ? Zero.of(statistics) : new StdScore(properties, statistics, computed.average(), computed.std());
    }

    public ScalarScaler create(
        NodePropertyValues properties,
        long nodeCount,
        Concurrency concurrency,
        ProgressTracker progressTracker,
        ExecutorService executor
    ) {
        return switch (type) {
            case None -> NoneScaler.of(properties);
            case Zero -> Zero.of();
            case Log -> LogScaler.of(properties, offset);
            case Center -> createCenter(properties,
                MinMaxAverageComputer.compute(
                    properties,
                    nodeCount,
                    concurrency,
                    progressTracker,
                    executor));
            case Mean -> createMean(properties,
                MinMaxAverageComputer.compute(
                    properties,
                    nodeCount,
                    concurrency,
                    progressTracker,
                    executor));
            case Max -> createMax(properties,
                MinMaxAverageComputer.compute(
                    properties,
                    nodeCount,
                    concurrency,
                    progressTracker,
                    executor));
            case MinMax -> createMinMax(properties,
                MinMaxAverageComputer.compute(
                    properties,
                    nodeCount,
                    concurrency,
                    progressTracker,
                    executor));
            case L1Norm -> createL1Norm(properties,
                L1NormComputer.compute(
                    properties,
                    nodeCount,
                    concurrency,
                    progressTracker,
                    executor));
            case L2Norm -> createL2Norm(properties,
                L2NormComputer.compute(
                    properties,
                    nodeCount,
                    concurrency,
                    progressTracker,
                    executor));
            case Std -> createStd(properties,
                StdComputer.compute(
                    properties,
                    nodeCount,
                    concurrency,
                    progressTracker,
                    executor));
        };
    }

    public boolean workingScaler() { return type != ScalerType.None; }

    public static String toString(ScalerFactory factory) {
        return factory.name().toUpperCase(Locale.ENGLISH);
    }
}
