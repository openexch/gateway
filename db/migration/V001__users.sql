-- SPDX-License-Identifier: Apache-2.0
-- Demo accounts of the gateway (GATEWAY_AUTH_MODES demo): username and password registration, one opaque bearer
-- token per user. Applied by oe-gateway itself (under an advisory lock, recorded in gateway_schema_migrations with
-- this file's sha256); an applied file is never edited, a change is a new file.
--
-- user_id is minted here and becomes the user's account in the core. The gateway takes the next value of the
-- sequence, asks the core whether that account already exists, and inserts the row only when it does not (a reset of
-- this database behind a continuing core must never hand a new user an old account). After a restore or reset, set
-- the sequence above the highest userId the core knows before opening registration:
--   SELECT setval('demo_user_seq', <highest known userId>);
-- The range keeps clear of 0 and 1 (never a user) and of the simulator's 900001-900999.

CREATE SEQUENCE demo_user_seq START 100000 MINVALUE 100000 MAXVALUE 899999 NO CYCLE;

CREATE TABLE users (
    user_id BIGINT PRIMARY KEY,
    -- [a-z0-9_]{3,20}, checked by the gateway and again here
    username VARCHAR(20) NOT NULL UNIQUE CHECK (username ~ '^[a-z0-9_]{3,20}$'),
    -- pbkdf2$<iterations>$<salt base64url>$<hash base64url>, PBKDF2-HMAC-SHA256
    password_hash VARCHAR(256) NOT NULL CHECK (password_hash LIKE 'pbkdf2$%'),
    -- SHA-256 of the current bearer token (the token itself is never stored); rotated at login, cleared at logout
    token_hash BYTEA UNIQUE CHECK (token_hash IS NULL OR octet_length(token_hash) = 32),
    token_issued_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
