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
package org.neo4j.gds.applications.algorithms.machinery;

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.Concurrency;

import java.util.Collection;

/**
 * Configurations have two halves basically: the algorithm parameters, and the request configuration.
 * This represents the latter.
 * It will help us rid the application layer of the coupling to configurations, which is unhealthy in principle,
 * and actually also not a good idea in practice.
 * Now, imagine a world where configurations were actually modelled as these two halves in the first place.
 * A developer can dream I guess...
 */
public record RequestConfiguration(
    Concurrency concurrency,
    JobId jobId,
    Collection<RelationshipType> relationshipTypeFilter,
    boolean sudo
) {

}
