CREATE TABLE clientes (
    id SERIAL PRIMARY KEY,
    cpf VARCHAR(14),
    nome VARCHAR(100),
    endereco TEXT,
    cidade VARCHAR(50),
    uf CHAR(2),
    email VARCHAR(100)
);

CREATE TABLE produtos (
    id SERIAL PRIMARY KEY,
    produto VARCHAR(100),
    valor NUMERIC(10,2),
    quantidade INT,
    tipo VARCHAR(50)
);

CREATE TABLE compras (
    id SERIAL PRIMARY KEY,
    id_produto INT REFERENCES produtos(id),
    data DATE,
    id_cliente INT REFERENCES clientes(id)
);

INSERT INTO clientes (cpf, nome, endereco, cidade, uf, email) VALUES
('111.111.111-11','Ana','Rua A','SP','SP','ana@email.com'),
('222.222.222-22','Bruno','Rua B','RJ','RJ','bruno@email.com');

INSERT INTO produtos (produto, valor, quantidade, tipo) VALUES
('Notebook', 3500.00, 10, 'Eletrônico'),
('Tênis', 299.90, 20, 'Vestuário');

INSERT INTO compras (id_produto, data, id_cliente) VALUES
(1, '2025-12-01', 1),
(2, '2025-12-02', 2);
