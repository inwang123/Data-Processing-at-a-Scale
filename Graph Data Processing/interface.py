from neo4j import GraphDatabase


class Interface:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self._driver.verify_connectivity()

    def close(self):
        self._driver.close()

    def bfs(self, start_node, target_node):
        with self._driver.session() as session:
            result = session.run(
                """
                MATCH path = shortestPath((start:Location {name: $start_node})-[*]-(target:Location {name: $target_node}))
                RETURN [node in nodes(path) | {name: node.name}] AS path
            """,
                start_node=start_node,
                target_node=target_node,
            )

            record = result.single()
            if record:
                return [{"path": record["path"]}]
            return []

    def pagerank(self, max_iterations, weight_property):
        with self._driver.session() as session:
            # graph projection
            session.run(
                """
                CALL gds.graph.project(
                    'pagerank_graph',
                    'Location',
                    'TRIP',
                    {
                        relationshipProperties: [$weight_property]
                    }
                )
            """,
                weight_property=weight_property,
            )

            # PageRank algo
            result = session.run(
                """
                CALL gds.pageRank.stream('pagerank_graph', {
                    maxIterations: $max_iterations,
                    relationshipWeightProperty: $weight_property,
                    dampingFactor: 0.85
                })
                YIELD nodeId, score
                RETURN gds.util.asNode(nodeId).name AS name, score
                ORDER BY score DESC
            """,
                max_iterations=max_iterations,
                weight_property=weight_property,
            )

            nodes = [
                {"name": record["name"], "score": record["score"]} for record in result
            ]

            # Drop graph projection
            session.run("CALL gds.graph.drop('pagerank_graph')")

            # max and min nodes
            return [nodes[0], nodes[-1]] if nodes else []
