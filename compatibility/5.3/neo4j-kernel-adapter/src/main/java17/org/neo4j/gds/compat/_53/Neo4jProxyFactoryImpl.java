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
package org.neo4j.gds.compat._53;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.gds.compat.Neo4jProxyApi;
import org.neo4j.gds.compat.Neo4jProxyFactory;
import org.neo4j.gds.compat.Neo4jVersion;

@ServiceProvider
public final class Neo4jProxyFactoryImpl implements Neo4jProxyFactory {

    @Override
    public boolean canLoad(Neo4jVersion version) {
        return version == Neo4jVersion.V_5_3;
    }

    @Override
    public Neo4jProxyApi load() {
        return new Neo4jProxyImpl();
    }

    @Override
    public String description() {
        return "Neo4j 5.3";
    }
}
