from neo4j import GraphDatabase

class Interface:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)
        self._driver.verify_connectivity()

    def close(self):
        self._driver.close()

    def bfs(self, start_node, target_node):
        with self._driver.session() as session:
            session.run("""
                CALL gds.graph.project(
                    'bfsgraph',
                    'Location',
                    'TRIP'
                )
            """)

            result = session.run(
                """
                MATCH (source:Location {name: $start_node})
                MATCH (target:Location {name: $target_node})
                CALL gds.bfs.stream('bfsgraph', {
                    sourceNode: source,
                    targetNodes: target
                })
                YIELD path
                RETURN [node in nodes(path) | {
                    name: node.name
                }] AS path
                LIMIT 1
                """,
                start_node=start_node,
                target_node=target_node
            )
            paths = []
            for record in result:
                paths.append({
                    "path": record["path"]
                })
            return paths

    def pagerank(self, max_iterations, weight_property):        
        with self._driver.session() as session:
            session.run("""
                CALL gds.graph.project(
                    'pagerank',
                    'Location',
                    'TRIP',
                    {
                        relationshipProperties: $weight
                    }
                )
            """, weight=weight_property)

            result = session.run(
                """
                CALL gds.pageRank.stream('pagerank', {
                    maxIterations: $max_iterations,
                    relationshipWeightProperty: $weight_property
                })
                YIELD nodeId, score
                RETURN gds.util.asNode(nodeId).name AS name, score
                ORDER BY score DESC
                """,
                max_iterations=int(max_iterations),
                weight_property=str(weight_property)
            )
            nodes = [{"name": record["name"], "score": record["score"]} for record in result]
            return [nodes[0], nodes[-1]]


