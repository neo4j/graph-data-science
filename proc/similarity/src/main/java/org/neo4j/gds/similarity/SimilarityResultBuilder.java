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
package org.neo4j.gds.similarity;

import org.HdrHistogram.DoubleHistogram;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.result.HistogramUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public abstract class SimilarityResultBuilder<PROC_RESULT> extends AbstractResultBuilder<PROC_RESULT> {

    public long nodesCompared = 0L;

    public long postProcessingMillis = -1L;

    Optional<DoubleHistogram> maybeHistogram = Optional.empty();

    public Map<String, Object> distribution() {
        if (maybeHistogram.isPresent()) {
            return HistogramUtils.similaritySummary(maybeHistogram.get());
        }
        return Collections.emptyMap();
    }

    public SimilarityResultBuilder<PROC_RESULT> withNodesCompared(long nodesCompared) {
        this.nodesCompared = nodesCompared;
        return this;
    }

    public SimilarityResultBuilder<PROC_RESULT> withHistogram(DoubleHistogram histogram) {
        this.maybeHistogram = Optional.of(histogram);
        return this;
    }

    public ProgressTimer timePostProcessing() {
        return ProgressTimer.start(this::setPostProcessingMillis);
    }

    public void setPostProcessingMillis(long postProcessingMillis) {
        this.postProcessingMillis = postProcessingMillis;
    }
}
