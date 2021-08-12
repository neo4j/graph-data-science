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
package org.neo4j.gds.utils;

import org.neo4j.gds.ml.nodemodels.metrics.AllClassMetric;
import org.neo4j.gds.ml.nodemodels.metrics.MetricSpecification;

import java.util.LinkedList;
import java.util.List;

public final class MetricUtil {

    private MetricUtil() {}

    public static List<String> allValidMetricSpecifications() {
        var validExpressions = new LinkedList<String>();
        var allClassExpressions = AllClassMetric.values();
        for (AllClassMetric allClassExpression : allClassExpressions) {
            validExpressions.add(allClassExpression.name());
        }
        for (String singleClassMetric : MetricSpecification.SINGLE_CLASS_METRIC_FACTORIES.keySet()) {
            validExpressions.add(singleClassMetric + "(class=*)");
            validExpressions.add(singleClassMetric + "(class=0)");
        }
        return validExpressions;
    }
}
