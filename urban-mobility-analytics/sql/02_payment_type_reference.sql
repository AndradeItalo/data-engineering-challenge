CREATE TABLE IF NOT EXISTS bronze.payment_type_reference (
    payment_type            INTEGER PRIMARY KEY,
    payment_description     TEXT NOT NULL,
    is_valid_payment        BOOLEAN NOT NULL
);

INSERT INTO bronze.payment_type_reference (payment_type, payment_description, is_valid_payment)
VALUES
    (1, 'Credit card', TRUE),
    (2, 'Cash', TRUE),
    (3, 'No charge', FALSE),
    (4, 'Dispute', FALSE),
    (5, 'Unknown', FALSE),
    (6, 'Voided trip', FALSE)
ON CONFLICT (payment_type) DO UPDATE
    SET payment_description = EXCLUDED.payment_description,
        is_valid_payment = EXCLUDED.is_valid_payment;
