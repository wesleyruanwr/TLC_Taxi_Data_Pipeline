CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS silver.payment_types (   -- tabela de referencia de tipos de pagamento na camada silver
    payment_type INT PRIMARY KEY, 
    payment_description VARCHAR(50) NOT NULL,
    is_valid_payment BOOLEAN NOT NULL
);


INSERT INTO silver.payment_types (payment_type, payment_description, is_valid_payment) --  dados de referencia
VALUES 
    (1, 'Credit card', true),
    (2, 'Cash', true),
    (3, 'No charge', false),
    (4, 'Dispute', false),
    (5, 'Unknown', false),
    (6, 'Voided trip', false)
ON CONFLICT (payment_type) DO UPDATE 
SET payment_description = EXCLUDED.payment_description,
    is_valid_payment = EXCLUDED.is_valid_payment;
