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
package org.neo4j.gds.applications.algorithms.centrality;

import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.hits.HitsConfig;
import org.neo4j.gds.termination.TerminationFlag;

public class HitsHookGenerator {

    private final ProgressTrackerCreator progressTrackerCreator;
    private final TerminationFlag terminationFlag;

    public HitsHookGenerator(ProgressTrackerCreator progressTrackerCreator, TerminationFlag terminationFlag) {
        this.progressTrackerCreator = progressTrackerCreator;
        this.terminationFlag = terminationFlag;
    }

    HitsETLHook createETLHook(HitsConfig hitsConfig){
        return  new HitsETLHook(hitsConfig,progressTrackerCreator,terminationFlag);
    }
}
