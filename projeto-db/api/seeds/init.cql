CREATE (:Person {id: '1', nome: 'Ana'});
CREATE (:Person {id: '2', nome: 'Bruno'});

MATCH (a:Person {id: '1'}), (b:Person {id: '2'})
CREATE (a)-[:FRIEND]->(b);

CREATE (:Produto {id: 1, produto: 'Notebook', valor: '3500.00'});
CREATE (:Produto {id: 2, produto: 'T\u00eanis', valor: '299.90'});