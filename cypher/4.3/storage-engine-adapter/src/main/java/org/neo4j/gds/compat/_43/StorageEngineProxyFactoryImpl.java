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

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.gds.compat.Neo4jVersion;
import org.neo4j.gds.compat.StorageEngineProxyApi;
import org.neo4j.gds.compat.StorageEngineProxyFactory;
import org.neo4j.kernel.internal.Version;

import java.util.regex.Pattern;

@ServiceProvider
public class StorageEngineProxyFactoryImpl implements StorageEngineProxyFactory {

    @Override
    public boolean canLoad(Neo4jVersion version) {
        if (version != Neo4jVersion.V_4_3) {
            return false;
        }

        var patchVersion = Pattern.compile("[.-]")
            .splitAsStream(Version.getNeo4jVersion())
            .skip(2)
            .findFirst();

        // If there is no patch identifier (there should always be one),
        // we assume 4.3.0 and bail out
        if (patchVersion.isEmpty()) {
            return false;
        }

        try {
            long patchNumber = Long.parseLong(patchVersion.get(), 10);
            return patchNumber >= 4;
        } catch (NumberFormatException someWeirdVersionString) {
            // If we fail to parse we got something completely weird
            // and bail out as well
            return false;
        }
    }

    @Override
    public StorageEngineProxyApi load() {
        return new StorageEngineProxyImpl();
    }

    @Override
    public String description() {
        return "Storage Engine 4.3.4+";
    }
}
