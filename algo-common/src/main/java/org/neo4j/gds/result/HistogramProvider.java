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
package org.neo4j.gds.result;

import org.HdrHistogram.Histogram;
import org.neo4j.gds.core.ProcedureConstants;

import java.util.function.Supplier;

public class HistogramProvider implements Supplier<Histogram> {
    private long highestTrackedValue;
    private  boolean setHighestTrackedValue=false;

    void withHighestTrackedValue(long highestTrackedValue) {
        this.highestTrackedValue = highestTrackedValue;
        this.setHighestTrackedValue = true;
    }

    @Override
    public Histogram get() {

        if (setHighestTrackedValue) {
            return new Histogram(highestTrackedValue,ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT);
        }

        return new Histogram(ProcedureConstants.HISTOGRAM_PRECISION_DEFAULT);
    }
}

