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
package org.neo4j.gds.metrics;

import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;
import org.neo4j.gds.metrics.projections.ProjectionMetricsService;

public class MetricsFacade {

    public static final MetricsFacade PASSTHROUGH_METRICS_FACADE = new MetricsFacade(
        new AlgorithmMetricsService(new PassthroughExecutionMetricRegistrar()),
        new ProjectionMetricsService(new PassthroughExecutionMetricRegistrar())
    );

    private  final AlgorithmMetricsService algorithmMetricsService;
    private  final ProjectionMetricsService projectionMetricsService;

    public MetricsFacade(
        AlgorithmMetricsService algorithmMetricsService,
        ProjectionMetricsService projectionMetricsService
    ){
        this.algorithmMetricsService = algorithmMetricsService;
        this.projectionMetricsService = projectionMetricsService;
    }

    public AlgorithmMetricsService algorithmMetrics(){
        return  algorithmMetricsService;
    }
    public ProjectionMetricsService projectionMetrics(){
        return  projectionMetricsService;
    }
}
