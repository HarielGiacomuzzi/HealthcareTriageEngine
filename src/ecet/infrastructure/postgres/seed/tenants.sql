-- Dev tenants. Secrets are dev HMAC keys and are worthless outside compose.
INSERT INTO tenants (id, name, webhook_url, webhook_secret, active) VALUES
  ('tenant-a', 'Northwind Health Plan', 'http://mock-client:8081/hooks/northwind', 'dev-hmac-tenant-a', true),
  ('tenant-b', 'Cascade Mutual Benefits', 'http://mock-client:8081/hooks/cascade', 'dev-hmac-tenant-b', true),
  ('tenant-empty', 'Harbor Point Onboarding', 'http://mock-client:8081/hooks/harbor', 'dev-hmac-tenant-empty', true),
  ('tenant-legacy', 'Meridian Legacy Care', 'http://mock-client:8081/hooks/meridian', 'dev-hmac-tenant-legacy', false)
ON CONFLICT (id) DO NOTHING;
