
// Test: Social network graph with relationships and mixed CREATE/MERGE/MATCH patterns
// Focus: Query splitting, relationship handling, TAG_NODES_ with/without RETURN

/* Block comment test:
   This should be ignored by _split_combined */

// Basic nodes with explicit kg property
CREATE (alice:Person {name: "Alice", age: 30, kg: "social"})
CREATE (bob:Person {name: "Bob", age: 25, kg: "social"})
CREATE (charlie:Person {name: "Charlie", age: 35, kg: "social"});

// Single statement with multiple nodes + relationship
MATCH (alice:Person {name: "Alice", kg: "social"})
MATCH (bob:Person {name: "Bob", kg: "social"})
MATCH (charlie:Person {name: "Charlie", kg: "social"})
CREATE (alice)-[:KNOWS {since: 2020, kg: "social"}]->(bob),
       (bob)-[:KNOWS {since: 2021, kg: "social"}]->(charlie)
RETURN alice, bob, charlie;

// Query without RETURN (tests fallback tag & fetch)
CREATE (dave:Person {name: "Dave", age: 28, kg: "social"})
CREATE (dave)-[:KNOWS {kg: "social"}]->(alice);

// Mixed: MATCH + CREATE in same statement, omits 'kg' from relation, and explicitly returns partial result
MATCH (a:Person {name: "Alice", kg: "social"})
MATCH (b:Person {name: "Bob", kg: "social"})
CREATE (b)-[:FOLLOWS]->(a)
RETURN b;

// Edge case: Properties with list values
CREATE (frank:Person {
    name: "Frank",
    hobbies: ["reading", "coding"],
    scores: [85, 92, 78],
    kg: "social"
}) RETURN frank;

// Relationship properties
MATCH (alice:Person {name: "Alice", kg: "social"})
MATCH (dave:Person {name: "Dave", kg: "social"})
MERGE (alice)-[r:COLLABORATES {project: "AI", hours: 120, kg: "social"}]->(dave)
RETURN r;
