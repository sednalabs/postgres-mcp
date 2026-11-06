-- ONLY for a disposable, isolated PostgreSQL test instance. Never production.
-- The runner must configure TLS and provide passwords through its secret store.
CREATE ROLE remote_fixture_reader LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT;
CREATE TABLE public.remote_fixture (
    id bigint PRIMARY KEY,
    large_text text,
    nested_json jsonb,
    large_array text[]
);
INSERT INTO public.remote_fixture
SELECT id, repeat('x', 20000), jsonb_build_object('nested', repeat('x', 5000)), ARRAY[repeat('x', 5000)]
FROM generate_series(1, 150) AS id;
GRANT USAGE ON SCHEMA public TO remote_fixture_reader;
GRANT SELECT ON public.remote_fixture TO remote_fixture_reader;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
-- Verify each independent elevated flag, plus membership, fails closed.
CREATE ROLE remote_fixture_super LOGIN SUPERUSER;
CREATE ROLE remote_fixture_bypass LOGIN BYPASSRLS;
CREATE ROLE remote_fixture_createdb LOGIN CREATEDB;
CREATE ROLE remote_fixture_createrole LOGIN CREATEROLE;
CREATE ROLE remote_fixture_replication LOGIN REPLICATION;
CREATE ROLE remote_fixture_member LOGIN;
GRANT remote_fixture_reader TO remote_fixture_member;
