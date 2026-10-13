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
package org.neo4j.gds.applications.algorithms.miscellaneous;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.execution.machinery.Synchroniser;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultRenderer;
import org.neo4j.gds.scaleproperties.ScalePropertiesBaseConfig;
import org.neo4j.gds.scaleproperties.ScalePropertiesResult;

import java.util.Optional;
import java.util.stream.Stream;

public class MiscellaneousApplicationsStreamModeBusinessFacade {
    private final InstrumentedMiscellaneousAlgorithms instrumentedAlgorithms;
    private final Synchroniser synchroniser;

    MiscellaneousApplicationsStreamModeBusinessFacade(
        InstrumentedMiscellaneousAlgorithms instrumentedAlgorithms,
        Synchroniser synchroniser
    ) {
        this.instrumentedAlgorithms = instrumentedAlgorithms;
        this.synchroniser = synchroniser;
    }

    public <RESULT> Stream<RESULT> scaleProperties(
        GraphName graphName,
        ScalePropertiesBaseConfig configuration,
        StreamResultBuilder<ScalePropertiesResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedAlgorithms.scaleProperties(
            graphName,
            configuration,
            Optional.empty(),
            new StreamResultRenderer<>(resultBuilder)
        ));
    }
}
