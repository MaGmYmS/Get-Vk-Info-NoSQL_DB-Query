import argparse

from neo4j import GraphDatabase


class Neo4jQueries:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def get_total_users(self):
        query = "MATCH (u:User) RETURN COUNT(u) AS total_users"
        with self.driver.session() as session:
            result = session.run(query)
            return result.single()["total_users"]

    def get_total_groups(self):
        query = "MATCH (g:Group) RETURN COUNT(g) AS total_groups"
        with self.driver.session() as session:
            result = session.run(query)
            return result.single()["total_groups"]

    def get_top_n_users_by_followers(self, n: int = 5):
        query = """
        MATCH (u:User)
        OPTIONAL MATCH (u)-[r]-()
        RETURN u.name AS user_name, COUNT(r) AS followers_count
        ORDER BY followers_count DESC
        LIMIT $n
        """
        with self.driver.session() as session:
            result = session.run(query, n=n)
            return [{"user_name": record["user_name"], "followers_count": record["followers_count"]} for record in
                    result]

    def get_top_n_groups_by_subscribers(self, n: int = 5):
        query = """
        MATCH (g:Group)
        OPTIONAL MATCH (g)-[r]-()
        RETURN g.name AS group_name, COUNT(r) AS subscribers_count
        ORDER BY subscribers_count DESC
        LIMIT $n
        """
        with self.driver.session() as session:
            result = session.run(query, n=n)
            return [{"group_name": record["group_name"], "subscribers_count": record["subscribers_count"]} for record in
                    result]

    def get_mutual_followers(self):
        query = """
        MATCH (u1:User)-[:Follow]->(u2:User)
        WHERE (u2)-[:Follow]->(u1)
        RETURN DISTINCT u1.name AS user1, u2.name AS user2
        """
        with self.driver.session() as session:
            result = session.run(query)
            return [{"user1": record["user1"], "user2": record["user2"]} for record in result]


def main(args):
    db = Neo4jQueries(uri="bolt://localhost:7687", user="neo4j", password="123")

    if args.query == "count_users":
        print("Total users:", db.get_total_users())
    elif args.query == "count_groups":
        print("Total groups:", db.get_total_groups())
    elif args.query == "top_users":
        top_users = db.get_top_n_users_by_followers(n=args.limit)
        print(f"Top {args.limit} users by followers:")
        for user in top_users:
            print(f"Name: {user['user_name']}, Followers: {user['followers_count']}")
    elif args.query == "top_groups":
        top_groups = db.get_top_n_groups_by_subscribers(n=args.limit)
        print(f"Top {args.limit} groups by subscribers:")
        for group in top_groups:
            print(f"Name: {group['group_name']}, Subscribers: {group['subscribers_count']}")
    elif args.query == "mutual_followers":
        mutual = db.get_mutual_followers()
        print("Users who follow each other:")
        for pair in mutual:
            print(f"User {pair['user1']} <-> User {pair['user2']}")
    else:
        print("Invalid query. Please specify a valid query type.")

    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="VK Data Collector with Neo4j")
    parser.add_argument('--query', type=str,
                        choices=['count_users', 'count_groups', 'top_users', 'top_groups', 'mutual_followers'],
                        required=True, help="Query type for Neo4j database")
    parser.add_argument('--limit', type=int, default=5, help="Number of top records to return for top queries")
    args = parser.parse_args()
    main(args)
