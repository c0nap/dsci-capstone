// Test: Event graph with property mutations and multi-hop traversal
// Focus: MERGE updates, relationship chains, consistent rel_type with properties

// Wave 1: Create events with minimal properties
CREATE (e1:DialogueEvent {name: "Alice speaks", speaker: "Alice", kg: "events"})
CREATE (e2:ActionEvent {name: "Alice is hired", subject: "Alice", kg: "events"})
CREATE (e3:SceneEvent {name: "Bob leaves room", subject: "Bob", kg: "events"})
CREATE (e4:DialogueEvent {name: "Bob speaks", speaker: "Bob", kg: "events"})
CREATE (e5:ActionEvent {name: "Charlie arrives", subject: "Charlie", kg: "events"})
CREATE (e6:DialogueEvent {name: "Charlie speaks", speaker: "Charlie", kg: "events"})
CREATE (e7:SceneEvent {name: "Alice enters hall", subject: "Alice", kg: "events"});

// Create event flow (DAG structure) - relations omit kg property intentionally
MERGE (e1:DialogueEvent {name: "Alice speaks", kg: "events"})
MERGE (e2:ActionEvent {name: "Alice is hired", kg: "events"})
MERGE (e3:SceneEvent {name: "Bob leaves room", kg: "events"})
MERGE (e4:DialogueEvent {name: "Bob speaks", kg: "events"})
MERGE (e5:ActionEvent {name: "Charlie arrives", kg: "events"})
MERGE (e6:DialogueEvent {name: "Charlie speaks", kg: "events"})
MERGE (e7:SceneEvent {name: "Alice enters hall", kg: "events"})
CREATE (e1)-[:followedBy {line: 42}]->(e2)
CREATE (e2)-[:followedBy {line: 58}]->(e3)
CREATE (e3)-[:followedBy {line: 61}]->(e4)
CREATE (e4)-[:followedBy {line: 75}]->(e5)
CREATE (e5)-[:followedBy {line: 89}]->(e6)
CREATE (e6)-[:followedBy {line: 102}]->(e7);

// Wave 2: Update existing events with additional properties via MERGE
MERGE (e1:DialogueEvent {name: "Alice speaks", kg: "events"})
SET e1.says = "I accept the quest", e1.audience = "Guild Master"
MERGE (e2:ActionEvent {name: "Alice is hired", kg: "events"})
SET e2.action = "hired", e2.by = "Guild Master"
MERGE (e3:SceneEvent {name: "Bob leaves room", kg: "events"})
SET e3.location_from = "guild hall", e3.location_to = "market"
MERGE (e4:DialogueEvent {name: "Bob speaks", kg: "events"})
SET e4.says = "The dragon stirs", e4.audience = "townspeople"
MERGE (e5:ActionEvent {name: "Charlie arrives", kg: "events"})
SET e5.action = "arrives", e5.location = "guild hall", e5.method = "teleportation"
MERGE (e6:DialogueEvent {name: "Charlie speaks", kg: "events"})
SET e6.says = "I bring grave news", e6.audience = "Alice"
MERGE (e7:SceneEvent {name: "Alice enters hall", kg: "events"})
SET e7.location_from = "courtyard", e7.location_to = "throne hall";

// Add text snippets to relationships via MERGE
MERGE (e1)-[r1:followedBy {line: 42}]->(e2)
SET r1.snippet = "The Guild Master nodded approvingly"
MERGE (e2)-[r2:followedBy {line: 58}]->(e3)
SET r2.snippet = "Meanwhile, across town"
MERGE (e3)-[r3:followedBy {line: 61}]->(e4)
SET r3.snippet = "Bob addressed the crowd"
MERGE (e4)-[r4:followedBy {line: 75}]->(e5)
SET r4.snippet = "A flash of light interrupted"
MERGE (e5)-[r5:followedBy {line: 89}]->(e6)
SET r5.snippet = "Charlie turned to face Alice"
MERGE (e6)-[r6:followedBy {line: 102}]->(e7)
SET r6.snippet = "The following day, Alice";

// Create alternate path (branch in DAG) - Alice speaks can also lead directly to Charlie arrives
MERGE (e1:DialogueEvent {name: "Alice speaks", kg: "events"})
MERGE (e5:ActionEvent {name: "Charlie arrives", kg: "events"})
CREATE (e1)-[:followedBy {line: 43, snippet: "Suddenly, a portal opened", alternate: true}]->(e5);
