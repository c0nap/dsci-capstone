
// Example: make a few standalone Pet nodes
CREATE (p:Pet {name: "Milo", species: "Cat", weight: 4.2, age: 3, kg: "pets"}) RETURN p;
CREATE (p:Pet {name: "Buddy", species: "Dog", weight: 12.5, age: 5, kg: "pets"}) RETURN p;
CREATE (p:Pet {name: "Coco", species: "Parrot", weight: 0.9, age: 2, kg: "pets"}) RETURN p;
CREATE (p:Pet {name: "Luna", species: "Rabbit", weight: 2.3, age: 4, kg: "pets"}) RETURN p;
CREATE (p:Pet {name: "Nemo", species: "Fish", weight: 0.1, age: 1, kg: "pets"}) RETURN p;
