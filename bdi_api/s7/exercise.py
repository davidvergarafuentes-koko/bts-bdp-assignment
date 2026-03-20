from fastapi import APIRouter, HTTPException, status
from neo4j import GraphDatabase
from pydantic import BaseModel

from bdi_api.settings import Settings

settings = Settings()

s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)


class PersonCreate(BaseModel):
    name: str
    city: str
    age: int


class RelationshipCreate(BaseModel):
    from_person: str
    to_person: str
    relationship_type: str = "FRIENDS_WITH"


def get_driver():
    return GraphDatabase.driver(
        settings.neo4j_url,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )


@s7.post("/graph/person")
def create_person(person: PersonCreate) -> dict:
    """Create a person node in Neo4J."""
    driver = get_driver()
    with driver.session() as session:
        session.run(
            "MERGE (p:Person {name: $name}) SET p.city = $city, p.age = $age",
            name=person.name,
            city=person.city,
            age=person.age,
        )
    driver.close()
    return {"status": "ok", "name": person.name}


@s7.get("/graph/persons")
def list_persons() -> list[dict]:
    """List all person nodes."""
    driver = get_driver()
    with driver.session() as session:
        result = session.run("MATCH (p:Person) RETURN p.name AS name, p.city AS city, p.age AS age")
        persons = [{"name": record["name"], "city": record["city"], "age": record["age"]} for record in result]
    driver.close()
    return persons


@s7.get("/graph/person/{name}/friends")
def get_friends(name: str) -> list[dict]:
    """Get friends of a person."""
    driver = get_driver()
    with driver.session() as session:
        # Check if person exists
        exists = session.run("MATCH (p:Person {name: $name}) RETURN p", name=name).single()
        if not exists:
            driver.close()
            raise HTTPException(status_code=404, detail=f"Person '{name}' not found")

        result = session.run(
            "MATCH (p:Person {name: $name})-[:FRIENDS_WITH]-(friend:Person) "
            "RETURN friend.name AS name, friend.city AS city, friend.age AS age",
            name=name,
        )
        friends = [{"name": record["name"], "city": record["city"], "age": record["age"]} for record in result]
    driver.close()
    return friends


@s7.post("/graph/relationship")
def create_relationship(rel: RelationshipCreate) -> dict:
    """Create a relationship between two persons."""
    driver = get_driver()
    with driver.session() as session:
        # Verify both persons exist
        from_exists = session.run("MATCH (p:Person {name: $name}) RETURN p", name=rel.from_person).single()
        if not from_exists:
            driver.close()
            raise HTTPException(status_code=404, detail=f"Person '{rel.from_person}' not found")

        to_exists = session.run("MATCH (p:Person {name: $name}) RETURN p", name=rel.to_person).single()
        if not to_exists:
            driver.close()
            raise HTTPException(status_code=404, detail=f"Person '{rel.to_person}' not found")

        session.run(
            "MATCH (a:Person {name: $from_name}), (b:Person {name: $to_name}) "
            "CREATE (a)-[:FRIENDS_WITH]->(b)",
            from_name=rel.from_person,
            to_name=rel.to_person,
        )
    driver.close()
    return {"status": "ok", "from": rel.from_person, "to": rel.to_person}


@s7.get("/graph/person/{name}/recommendations")
def get_recommendations(name: str) -> list[dict]:
    """Get friend recommendations for a person."""
    driver = get_driver()
    with driver.session() as session:
        # Check if person exists
        exists = session.run("MATCH (p:Person {name: $name}) RETURN p", name=name).single()
        if not exists:
            driver.close()
            raise HTTPException(status_code=404, detail=f"Person '{name}' not found")

        result = session.run(
            "MATCH (p:Person {name: $name})-[:FRIENDS_WITH]-(friend)-[:FRIENDS_WITH]-(fof:Person) "
            "WHERE fof <> p AND NOT (p)-[:FRIENDS_WITH]-(fof) "
            "RETURN fof.name AS name, fof.city AS city, COUNT(friend) AS mutual_friends "
            "ORDER BY mutual_friends DESC",
            name=name,
        )
        recommendations = [
            {"name": record["name"], "city": record["city"], "mutual_friends": record["mutual_friends"]}
            for record in result
        ]
    driver.close()
    return recommendations
