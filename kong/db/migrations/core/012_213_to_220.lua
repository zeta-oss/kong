return {
  postgres = {
    up = [[
      CREATE TABLE IF NOT EXISTS "clustering_data_planes" (
        id             UUID PRIMARY KEY,
        hostname       TEXT NOT NULL,
        ip             TEXT NOT NULL,
        last_seen      TIMESTAMP WITH TIME ZONE DEFAULT (CURRENT_TIMESTAMP(0) AT TIME ZONE 'UTC'),
        config_hash    TEXT NOT NULL
      );




      DO $$
      BEGIN
        ALTER TABLE IF EXISTS ONLY "upstreams" ADD "service_discovery" JSONB;
      EXCEPTION WHEN DUPLICATE_COLUMN THEN
        -- Do nothing
      END;
      $$;
    ]],
  },
  cassandra = {
    up = [[
      CREATE TABLE IF NOT EXISTS clustering_data_planes(
        id uuid,
        hostname text,
        ip text,
        last_seen timestamp,
        config_hash text,
        PRIMARY KEY (id)
      );



      ALTER TABLE upstreams ADD service_discovery text;
    ]],
  }
}
