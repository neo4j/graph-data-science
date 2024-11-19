/*
Copyright (c) "Neo4j"
Neo4j Sweden AB [http://neo4j.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/


-- *========================================*
-- | Neo4j Graph Data Science for Snowflake |
-- | Basket analysis example on TPC-H data  |
-- *========================================*

-- In this example, we will use Neo4j Graph Data Science (GDS) for Snowflake to perform basket analysis on the TPC-H dataset.
-- For that we will look at parts that are often ordered together and use the node similarity algorithm to find similar parts.
-- We will then write the results back to Snowflake and query the results.
--
-- See https://docs.snowflake.com/en/user-guide/sample-data-tpch for information about the sample data, its schema.
-- NOTE: You may need to install the dataset if it's not already present in your account.
--
-- For this example to run, we make the following assumptions:
--
-- * Neo4j Graph Data Science application is installed correctly and called "Neo4j_GDS"
-- * Neo4j Graph Data Science application has been granted the CREATE COMPUTE POOL privilege
-- * Neo4j Graph Data Science application has been granted the CREATE WAREHOUSE privilege
-- * The current role can create databases and schemas
-- * The current role has granted the application role Neo4j_GDS.app_user
-- * The current role has access to the Snowflake sample data set (database "snowflake_sample_data")
--
-- The example is split into three parts:
-- 1. Data preparation,
-- 2. Application setup, and
-- 3. Graph analysis.
-- The first two parts need to be executed only once, while the third part can be executed multiple times with different parameters or algorithms.


-- ==================================================
-- 1. Data preparation
-- ==================================================

-- Create a database which we will use to prepare data for GDS.
CREATE DATABASE IF NOT EXISTS tpch_example;
CREATE SCHEMA IF NOT EXISTS tpch_example.gds;
USE SCHEMA tpch_example.gds;

-- GDS expects the data to be in a specific format: a table/view for nodes and a table/view for relationships.
-- In addition, GDS requires node identifiers to be globally unique integers.
--
-- For our analysis, the nodes will be parts and the orders in which they appeared.
-- The relationships will be the line items linking a part to an order.
--
-- We start by creating the node view for our graph.
-- First we need to map the primary keys for parts and orders to globally unique node ids.

-- We use a sequence to generate globally unique node identifiers.
CREATE OR REPLACE SEQUENCE global_id START = 0 INCREMENT = 1;

-- We create two mapping tables, one for parts and one for orders.
-- This is necessary because the primary key sets for both tables might overlap.
CREATE OR REPLACE TABLE node_mapping_parts(gdsId, p_partkey) AS
    SELECT global_id.nextval, p_partkey
    FROM snowflake_sample_data.tpch_sf1.part;
CREATE OR REPLACE TABLE node_mapping_orders(gdsId, o_orderkey) AS
    SELECT global_id.nextval, o_orderkey
    FROM snowflake_sample_data.tpch_sf1.orders;

-- Next, we can create the final node view that we use for our graph projection.
-- Note, that the view must contain a column named "nodeId" to be recognized by GDS.
-- Any additional column will be used as node property, but we don't need that for this example.
CREATE OR REPLACE VIEW nodes(nodeId) AS
    SELECT nmp.gdsId FROM node_mapping_parts nmp
    UNION
    SELECT nmo.gdsId FROM node_mapping_orders nmo;

-- Let's quickly verify the cardinality of our views.
-- As it is the union of parts and orders, we expect 1,700,000 rows.
SELECT count(*) FROM nodes;

-- We can now create the relationship view.
-- As mentioned earlier, we will use the line items to create relationships between parts and orders.
-- We join the line items with parts and orders to get the source and target nodes for our relationships.
-- We also join the mapping tables to get the globally unique node ids.
-- Note, that the view must contain columns named "sourceNodeId" and "targetNodeId" to be recognized by GDS.
-- Any additional column will be used as relationship property, but we don't need that for this example.
CREATE OR REPLACE VIEW relationships(sourceNodeId, targetNodeId) AS
    SELECT 
        nmp.gdsId AS sourceNodeId, 
        nmo.gdsId AS targetNodeId
    FROM snowflake_sample_data.tpch_sf1.part p
        -- The first two joins build the relationships between parts and orders
        JOIN snowflake_sample_data.tpch_sf1.lineitem l
          ON p.p_partkey = l.l_partkey
        JOIN snowflake_sample_data.tpch_sf1.orders o
          ON o.o_orderkey = l.l_orderkey
        -- The second two joins map the primary keys to globally unique node ids
        JOIN node_mapping_parts nmp
          ON nmp.p_partkey = p.p_partkey
        JOIN node_mapping_orders nmo
          ON nmo.o_orderkey = o.o_orderkey;

-- Let's quickly verify the cardinality of our relationship view.
-- As it is the join of parts, line items, and orders, we expect 6,001,215 rows.
SELECT count(*) FROM relationships;

-- We have now prepared the data for GDS.

-- ==================================================
-- 2. Application setup
-- ==================================================

-- We start by switching to the Neo4j_GDS application.
USE DATABASE Neo4j_GDS;

-- Next, we want to consider the warehouse that the GDS application will use to execute queries.
-- For this example a MEDIUM size warehouse, so we configure the application's warehouse accordingly
ALTER WAREHOUSE Neo4j_GDS_app_warehouse
  WAREHOUSE_SIZE='MEDIUM';
-- A highly performant warehouse will speed up graph projections but does not affect algorithm computation.
-- It can therefore be a good idea to alter the warehouse size and make other configuration changes to increase performance when projecting larger amounts of data.
-- The warehouse can then be brought back to a less expensive configuration after the projection is done.
-- ALTER WAREHOUSE Neo4j_GDS_app_warehouse
--   WAREHOUSE_SIZE='X-SMALL';

-- The following grants are necessary for the GDS application to read and write data.
-- The next queries are required to read from our prepared views.
GRANT USAGE ON DATABASE             tpch_example     TO APPLICATION Neo4j_GDS;
GRANT USAGE ON SCHEMA               tpch_example.gds TO APPLICATION Neo4j_GDS;
GRANT SELECT ON ALL VIEWS IN SCHEMA tpch_example.gds TO APPLICATION Neo4j_GDS;
-- This grant is necessary to enable write back of algorithm results.
GRANT CREATE TABLE ON SCHEMA        tpch_example.gds TO APPLICATION Neo4j_GDS;

-- We have now prepared the environment to properly run the GDS application and can start with our analysis.
-- Note, that data preparation and application setup only need to be done once.

-- Our final preparation is to select a compute pool to run the GDS service.
-- Available compute pools to select from are:
-- * CPU_X64_XS
-- * CPU_X64_M
-- * CPU_X64_L
-- * HIGHMEM_X64_S
-- * HIGHMEM_X64_M
--
-- For our example, we use a large compute pool as the node similarity algorithm is computationally intensive, but without extra memory because the graph is quite small.
-- We select: CPU_X64_L

-- ==================================================
-- 3. Graph analysis
-- ==================================================

-- The first step is to create a new GDS session.
-- Creating the session will start a container service on the selected compute pool.
-- In addition, all the service functions that allow us to interact with the GDS service are created.
-- A session can be used by many users, but only one session can be active at a time.
CALL gds.create_session('CPU_X64_L');

-- Once the session is started, we can project our node and relationship views into a GDS in-memory graph.
-- The graph will be identified by the name "parts_in_orders".
-- The mandatory parameters are the node table and the relationship table, which we point those to our prepared views.
-- We also specify the optional read concurrency to optimize building the graph projection.
-- The concurrency can be set to the number of cores available on the compute pool node.
SELECT gds.graph_project('parts_in_orders', {
    'nodeTable':         'tpch_example.gds.nodes',
    'relationshipTable': 'tpch_example.gds.relationships',
    'readConcurrency':   28
});

-- The graph we project is a so-called bipartite graph, as it contains two types of nodes and all relationships point from one type to the other.
-- The node similarity algorithm looks at all pairs of nodes of the first type and calculates the similarity for each pair based on common relationships.
-- In our case, the algorithm will calculate the similarity between two parts based on the orders in which they appear.
-- The algorithm produces new relationships between parts, the relationship property is the similarity score.
-- For further information on the node similarity algorithm, please refer to the GDS documentation:
-- https://neo4j.com/docs/graph-data-science/current/algorithms/node-similarity/
SELECT gds.node_similarity('parts_in_orders', {
    'mutateRelationshipType': 'SIMILAR_TO',
    'mutateProperty':         'similarity',
    'concurrency':            28
});

-- Once the algorithm has finished, we can write the results back to Snowflake tables for further analysis.
-- We want to write back the similarity relationships between parts.
-- The specified table will contain the globally unique source and target node ids and the similarity score.
SELECT gds.write_relationships('parts_in_orders', {
    'relationshipType':     'SIMILAR_TO',
    'relationshipProperty': 'similarity',
    'table':                'tpch_example.gds.part_similar_to_part'
});

-- After writing the table, we need to ensure that our current role is allowed to read it.
-- Alternatively, we can also grant access to all future tables created by the application.
GRANT SELECT ON tpch_example.gds.part_similar_to_part TO ROLE <your_role>; 

-- Since the results are now stored in Snowflake, we can query them and join them with our original data.
-- For example, we can find the names of the most similar parts based on the similarity score.
-- Simply speaking, this could be used as a recommendation system for parts.
SELECT DISTINCT p_source.p_name, p_target.p_name, sim.similarity
FROM snowflake_sample_data.tpch_sf1.part p_source
JOIN tpch_example.gds.node_mapping_parts nmp_source
  ON p_source.p_partkey = nmp_source.p_partkey
JOIN tpch_example.gds.part_similar_to_part sim
  ON nmp_source.gdsid = sim.sourcenodeid
JOIN tpch_example.gds.node_mapping_parts nmp_target
  ON sim.targetnodeid = nmp_target.gdsid
JOIN snowflake_sample_data.tpch_sf1.part p_target
  ON nmp_target.p_partkey = p_target.p_partkey
ORDER BY sim.similarity DESC
LIMIT 10;

-- The GDS service is a long-running service and should be stopped when not in use.
-- Once we completed our analysis, we can stop the session, which suspends the container service.
-- We can restart the session at any time to continue our analysis.
CALL Neo4j_GDS.gds.stop_session();

