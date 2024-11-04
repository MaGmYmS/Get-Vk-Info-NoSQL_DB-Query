from py2neo import Graph

graph = Graph("bolt://localhost:7687", auth=("neo4j", "123"))

# Полное удаление всех узлов и связей
graph.run("MATCH (n) DETACH DELETE n")
print("База данных успешно очищена.")
