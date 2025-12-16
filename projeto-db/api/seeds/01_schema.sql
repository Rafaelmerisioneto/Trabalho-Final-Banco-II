INSERT INTO
    clientes (
        cpf,
        nome,
        endereco,
        cidade,
        uf,
        email
    )
VALUES (
        '111.111.111-11',
        'Ana',
        'Rua A',
        'SP',
        'SP',
        'ana@email.com'
    ),
    (
        '222.222.222-22',
        'Bruno',
        'Rua B',
        'RJ',
        'RJ',
        'bruno@email.com'
    );

INSERT INTO
    produtos (
        produto,
        valor,
        quantidade,
        tipo
    )
VALUES (
        'Notebook',
        3500.00,
        10,
        'Eletr\u00f4nico'
    ),
    (
        'T\u00eanis',
        299.90,
        20,
        'Vestu\u00e1rio'
    );

INSERT INTO
    compras (id_produto, data, id_cliente)
VALUES (1, '2025-12-01', 1),
    (2, '2025-12-02', 2);