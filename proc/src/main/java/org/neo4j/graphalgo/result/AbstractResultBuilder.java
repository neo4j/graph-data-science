/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.result;

import org.neo4j.graphalgo.newapi.WriteConfig;

public abstract class AbstractResultBuilder<CONFIG extends WriteConfig, WRITE_RESULT> {

    protected final CONFIG config;
    protected final String writeProperty;

    protected long loadMillis = -1;
    protected long computeMillis = -1;
    protected long writeMillis = -1;
    protected long nodePropertiesWritten;
    protected long relationshipPropertiesWritten;

    protected AbstractResultBuilder(CONFIG config) {
        this.config = config;
        this.writeProperty = config.writeProperty();
    }

    public void withCreateMillis(long loadMillis) {
        this.loadMillis = loadMillis;
    }

    public void withComputeMillis(long computeMillis) {
        this.computeMillis = computeMillis;
    }

    public void withWriteMillis(long writeMillis) {
        this.writeMillis = writeMillis;
    }

    public void withNodePropertiesWritten(long nodePropertiesWritten) {
        this.nodePropertiesWritten = nodePropertiesWritten;
    }

    public void withRelationshipPropertiesWritten(long relationshipPropertiesWritten) {
        this.relationshipPropertiesWritten = relationshipPropertiesWritten;
    }

    public abstract WRITE_RESULT build();
}
