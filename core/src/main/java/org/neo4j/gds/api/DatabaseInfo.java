package org.neo4j.gds.api;

import org.neo4j.gds.annotation.ValueClass;

@ValueClass
public interface DatabaseInfo {

    enum DatabaseLocation {
        LOCAL,
        REMOTE,
        NONE
    }

    DatabaseId databaseId();

    DatabaseLocation databaseLocation();
}
