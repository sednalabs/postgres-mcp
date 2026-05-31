CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE EXTENSION IF NOT EXISTS hypopg;

CREATE TABLE IF NOT EXISTS public.matrix_seed (
  id integer PRIMARY KEY,
  note text NOT NULL
);

INSERT INTO public.matrix_seed (id, note)
VALUES (1, 'matrix-seed')
ON CONFLICT (id) DO NOTHING;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'matrix_limited') THEN
    CREATE ROLE matrix_limited LOGIN PASSWORD 'matrix_limited_pass';
  END IF;
END;
$$;

GRANT CONNECT ON DATABASE matrix_db TO matrix_limited;
GRANT USAGE ON SCHEMA public TO matrix_limited;
GRANT SELECT ON TABLE public.matrix_seed TO matrix_limited;
