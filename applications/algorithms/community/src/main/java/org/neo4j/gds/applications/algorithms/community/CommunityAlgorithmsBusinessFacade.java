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
package org.neo4j.gds.applications.algorithms.community;

import org.neo4j.gds.CommunityAlgorithmTasks;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerManager;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.wcc.WccBaseConfig;

public class CommunityAlgorithmsBusinessFacade {
    private final ProgressTrackerManager progressTrackerManager = new ProgressTrackerManager();

    private final Log log;
    private final CommunityAlgorithms algorithms;
    private final ProgressTrackerCreator progressTrackerCreator;

    public CommunityAlgorithmsBusinessFacade(
        Log log,
        CommunityAlgorithms algorithms,
        ProgressTrackerCreator progressTrackerCreator
    ) {
        this.log = log;
        this.algorithms = algorithms;
        this.progressTrackerCreator = progressTrackerCreator;
    }

}
