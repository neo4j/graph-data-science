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
package org.neo4j.gds.compat.batchimport.input;

import org.neo4j.gds.compat.batchimport.InputIterable;

import java.io.IOException;

/**
 * Unifies all data input given to a {@link org.neo4j.gds.compat.batchimport.BatchImporter} to allow for more coherent implementations.
 */
public interface Input extends AutoCloseable {

    /**
     * Provides all node data for an import.
     *
     * @return an {@link org.neo4j.gds.compat.batchimport.InputIterator} which will provide all node data for the whole import.
     */
    InputIterable nodes();

    /**
     * Provides all relationship data for an import.
     *
     * @return an {@link org.neo4j.gds.compat.batchimport.InputIterator} which will provide all relationship data for the whole import.
     */
    InputIterable relationships();

    /**
     * @return {@link IdType} which matches the type of ids this {@link Input} generates.
     * Will get populated by node import and later queried by relationship import
     * to resolve potentially temporary input node ids to actual node ids in the database.
     */
    IdType idType();

    /**
     * @return accessor for id groups that this input has.
     */
    ReadableGroups groups();

    /**
     * @return {@link Estimates} for this input w/o reading through it entirely.
     * @throws IOException on I/O error.
     */
    Estimates calculateEstimates() throws IOException;

    @Override
    default void close() {}
}
