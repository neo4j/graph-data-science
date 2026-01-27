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
package org.neo4j.gds.procedures.algorithms;

import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;

class WriteNodePropertyMetadataBuilder {

    private NodePropertiesWritten nodePropertiesWritten;
    private long writeMillis = Long.MIN_VALUE;

    WriteNodePropertyMetadataBuilder nodePropertiesWritten(NodePropertiesWritten nodePropertiesWritten) {
        this.nodePropertiesWritten = nodePropertiesWritten;
        return this;
    }

    WriteNodePropertyMetadataBuilder writeMillis(long writeMillis) {
        this.writeMillis = writeMillis;
        return this;
    }

    WriteNodePropertyMetadata build() {
        if (nodePropertiesWritten == null) {
            throw new IllegalStateException("nodePropertiesWritten must be set");
        }

        if (writeMillis == Long.MIN_VALUE) {
            throw new IllegalStateException("writeMillis must be set");
        }

        return new WriteNodePropertyMetadata(nodePropertiesWritten, writeMillis);
    }
}
