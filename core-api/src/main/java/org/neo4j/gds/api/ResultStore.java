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
package org.neo4j.gds.api;

import org.neo4j.gds.core.utils.progress.JobId;

/**
 * A store for write results that are not immediately persisted in the database.
 * This is mainly used for the session architecture, where algorithms results are first
 * written into this store and then streamed via arrow to persist them in a
 * remote database.
 */
public interface ResultStore {

    /**
     * Stores a shallow entry representing result store data in this store given a {@link org.neo4j.gds.core.utils.progress.JobId}.
     */
    void add(JobId jobId, ResultStoreEntry entry);

    /**
     * Retrieves a {@link org.neo4j.gds.api.ResultStoreEntry} from this store.
     */
    ResultStoreEntry get(JobId jobId);

    /**
     * Checks if this store contains an entry for the given {@link org.neo4j.gds.core.utils.progress.JobId}.
     */
    boolean hasEntry(JobId jobId);

    /**
     * Removes a stored entry based on the given {@link org.neo4j.gds.core.utils.progress.JobId}.
     */
    void remove(JobId jobId);

    ResultStore EMPTY = new EmptyResultStore();
}
