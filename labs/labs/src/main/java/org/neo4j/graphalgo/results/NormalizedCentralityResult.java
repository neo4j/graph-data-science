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
package org.neo4j.graphalgo.results;

import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.impl.results.CentralityResult;

import java.util.function.DoubleUnaryOperator;

public class NormalizedCentralityResult extends CentralityResult {
    private CentralityResult result;
    private DoubleUnaryOperator normalizationFunction;

    public NormalizedCentralityResult(CentralityResult result, DoubleUnaryOperator normalizationFunction) {
        super(result.array());
        this.result = result;
        this.normalizationFunction = normalizationFunction;
    }

    public void export(String propertyName, NodePropertyExporter exporter, DoubleUnaryOperator normalizationFunction) {
        result.export(propertyName, exporter, normalizationFunction);
    }

    public double score(int nodeId) {
        return normalizationFunction.applyAsDouble(result.score(nodeId));
    }

    public double score(long nodeId) {
        return normalizationFunction.applyAsDouble(result.score(nodeId));
    }

    public void export(String propertyName, NodePropertyExporter exporter) {
        export(propertyName, exporter, normalizationFunction);
    }
}
