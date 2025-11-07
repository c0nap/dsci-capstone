// Test: Multiple knowledge graphs in same database
// Focus: kg property isolation, temp_graph context manager, filter_valid correctness

// Graph 1: Scene graph with spatial relationships
CREATE (alice:Person {name: 'Alice', position: 'standing', x: 5.0, y: 3.2, kg: 'scene'})
CREATE (bob:Person {name: 'Bob', position: 'sitting', x: 2.1, y: 4.5, kg: 'scene'})
CREATE (sofa:Furniture {name: 'Sofa', type: 'seating', x: 2.0, y: 4.0, kg: 'scene'})
CREATE (table:Furniture {name: 'Table', type: 'surface', x: 5.5, y: 3.0, kg: 'scene'})
CREATE (lamp:Furniture {name: 'Lamp', type: 'lighting', x: 5.8, y: 3.1, kg: 'scene'});

// Spatial relationships
CREATE (bob)-[:SITTING_ON]->(sofa)
CREATE (alice)-[:NEAR {distance: 0.5}]->(table)
CREATE (lamp)-[:ON_TOP_OF]->(table)
RETURN alice, bob, sofa, table, lamp;

// Graph 2: Dialogue graph (references scene objects)
CREATE (d1:Dialogue {name: "Alice_10.5", speaker: 'Alice', text: 'Could you pass me the book?', timestamp: 10.5, kg: 'dialogue'})
CREATE (d2:Dialogue {name: "Bob_12.3", speaker: 'Bob', text: 'Sure, it\'s on the table', timestamp: 12.3, kg: 'dialogue'})
CREATE (d3:Dialogue {name: "Alice_15.8", speaker: 'Alice', text: 'Thanks! The lamp is nice too', timestamp: 15.8, kg: 'dialogue'})
RETURN d1, d2, d3;

// Dialogue references objects (cross-graph semantic links)
MERGE (d1_ref:DialogueRef {name: "ref_Alice_book", speaker: 'Alice', mentioned_object: 'book', kg: 'dialogue'})
MERGE (d2_ref:DialogueRef {name: "ref_Bob_table", speaker: 'Bob', mentioned_object: 'table', kg: 'dialogue'})
MERGE (d3_ref:DialogueRef {name: "ref_Alice_lamp", speaker: 'Alice', mentioned_object: 'lamp', kg: 'dialogue'})
RETURN d1_ref, d2_ref, d3_ref;

// Sequential dialogue flow
CREATE (d1)-[:FOLLOWED_BY]->(d2)
CREATE (d2)-[:FOLLOWED_BY]->(d3)
RETURN d1, d2, d3;

// Cross-graph query (should be filtered by temp_graph context)
MATCH (p:Person {kg: 'scene'})
MATCH (d:Dialogue {kg: 'dialogue'})
WHERE d.speaker = p.name
RETURN p.name, d.text;
