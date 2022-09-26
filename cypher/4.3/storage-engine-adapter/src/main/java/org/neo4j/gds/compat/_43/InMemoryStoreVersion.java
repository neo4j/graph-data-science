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
package org.neo4j.gds.compat._43;

import org.neo4j.internal.recordstorage.AbstractInMemoryStoreVersion;
import org.neo4j.storageengine.api.StoreVersion;

import java.util.Optional;

public class InMemoryStoreVersion extends AbstractInMemoryStoreVersion {

    @Override
    public Optional<StoreVersion> successor() {
        return Optional.empty();
    }

    @Override
    public StoreVersion latest() {
        return this;
    }

    @Override
    public String storeVersion() {
        return STORE_VERSION;
    }

    @Override
    public boolean isCompatibleWith(StoreVersion otherVersion) {
        return otherVersion.storeVersion().equals(storeVersion());
    }
}
