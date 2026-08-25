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
package org.neo4j.gds.pyruntime.fastpath;

import org.neo4j.gds.ElementProjection;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.JobIdConfig;
import org.neo4j.gds.config.RandomSeedConfig;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Shared FastPath base config. FastPath is a light-weight path-embedding algorithm that runs as a
 * remote python-runtime job. These parameter definitions are mirrored from
 * {@code data_science_python_runtime.algorithms.fastpath.fp_predictor.FastPathConfig} and are shared
 * between the python-runtime compute config (flight-server) and the Cypher-surface configs (session)
 * so the two never drift.
 */
public interface FastPathBaseConfig extends RandomSeedConfig, JobIdConfig {

    String baseNodeLabel();

    String eventNodeLabel();

    Optional<String> contextNodeLabel();

    Optional<String> eventNodeTimeProperty();

    Optional<String> nextRelationshipType();

    Optional<String> firstRelationshipType();

    // relationship types to include in the projection FastPath operates on (defaults to all)
    default List<String> relationshipTypes() {
        return Collections.singletonList(ElementProjection.PROJECT_ALL);
    }

    default List<String> eventNodeCategoricalProperties() {
        return List.of();
    }

    Optional<String> eventNodeFeatureVectorProperty();

    // default inlined from data_science_python_runtime FastPathConfig.ignored_event_category
    default int eventNodeIgnoredCategory() {
        return -1;
    }

    @Configuration.IntegerRange(min = 1)
    int embeddingDimension();

    @Configuration.IntegerRange(min = 1)
    int numTimeAnchors();

    Optional<Double> observationTime();

    Optional<String> baseNodeObservationTimeProperty();

    @Configuration.IntegerRange(min = 1)
    int lookbackHorizon();

    // default inlined from data_science_python_runtime FastPathConfig.smoothing_rate
    default double smoothingRate() {
        return 0.0;
    }

    // default inlined from data_science_python_runtime FastPathConfig.decay_factor
    default double decayRate() {
        return 1.0;
    }

    // default inlined from data_science_python_runtime FastPathConfig.smoothing_window
    @Configuration.IntegerRange(min = 0)
    default int smoothingWindow() {
        return 0;
    }

    @Configuration.Check
    default void validateOutput() {
        if (observationTime().isPresent() && baseNodeObservationTimeProperty().isPresent()) {
            throw new IllegalArgumentException("Cannot specify both observationTime and baseNodeObservationTimeProperty");
        }

        if (observationTime().isEmpty() && baseNodeObservationTimeProperty().isEmpty()) {
            throw new IllegalArgumentException("Must specify either observationTime or baseNodeObservationTimeProperty");
        }
    }

    @Configuration.Check
    default void validateNextOrTimeProp() {
        if (nextRelationshipType().isEmpty() && eventNodeTimeProperty().isEmpty()) {
            throw new IllegalArgumentException("Must specify either nextRelationshipType or eventNodeTimeProperty");
        }
    }

    @Configuration.Check
    default void validateFirstAndNextOrNone() {
        if (firstRelationshipType().isPresent() != nextRelationshipType().isPresent()) {
            throw new IllegalArgumentException(
                "Must specify both firstRelationshipType and nextRelationshipType or neither"
            );
        }
    }
}
