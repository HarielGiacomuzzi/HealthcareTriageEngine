-- Coverage policies. Fixed UUIDs so reruns are no-ops and demos can cite an id.
-- tenant-a: 8 rows, 5 of them effective today (v1 of the MRI policy is superseded,
-- the gene panel expired, the knee arthroscopy policy was deactivated).
INSERT INTO policies (
  id, tenant_id, name, version, covered_codes, excluded_codes,
  criteria_text, required_evidence, active, effective_from, effective_to
) VALUES
  ('1a000000-0000-4000-8000-000000000001', 'tenant-a', 'MRI lumbar spine', 1,
   '{M54.5,M51.26}', '{Z00.00}',
   'Imaging is covered after documented conservative therapy of at least six weeks.',
   '{"conservative therapy >= 6 weeks","clinical examination note"}',
   true, '2025-01-01', NULL),
  ('1a000000-0000-4000-8000-000000000002', 'tenant-a', 'MRI lumbar spine', 2,
   '{M54.5,M54.4,M54.16,M51.26}', '{Z00.00,Z13.9}',
   'Imaging is covered after six weeks of conservative therapy; red-flag findings (progressive neurological deficit, suspected malignancy) waive the waiting period.',
   '{"conservative therapy >= 6 weeks","clinical examination note","prior imaging report"}',
   true, '2026-01-01', NULL),
  ('1a000000-0000-4000-8000-000000000003', 'tenant-a', 'CT head without contrast', 1,
   '{R51.9,G43.909,S06.0X0A}', '{R42}',
   'Covered for post-traumatic headache or new neurological findings within 72 hours of onset.',
   '{"neurological examination","onset documented"}',
   true, '2025-06-01', NULL),
  ('1a000000-0000-4000-8000-000000000004', 'tenant-a', 'Bariatric surgery', 1,
   '{E66.01,E66.2}', '{F50.2}',
   'Documented supervised weight-loss program of at least six months; the member''s BMI must be >= 40, or >= 35 with an obesity-related comorbidity.',
   '{"supervised weight-loss program >= 6 months","BMI measurement","psychological evaluation"}',
   true, '2025-01-01', NULL),
  ('1a000000-0000-4000-8000-000000000005', 'tenant-a', 'Polysomnography', 1,
   '{G47.33,G47.30}', '{F51.01}',
   'In-laboratory sleep study covered when home testing is inconclusive or contraindicated.',
   '{"Epworth sleepiness score","home sleep test result"}',
   true, '2025-03-01', NULL),
  ('1a000000-0000-4000-8000-000000000006', 'tenant-a', 'Extended physical therapy course', 1,
   '{M54.5,M25.561,M17.11}', '{M79.7}',
   'Sessions beyond the initial twelve require documented functional improvement.',
   '{"functional outcome measure","therapist progress note"}',
   true, '2025-01-01', NULL),
  ('1a000000-0000-4000-8000-000000000007', 'tenant-a', 'Knee arthroscopy', 1,
   '{M23.51,S83.511A}', '{M17.11}',
   'Covered for mechanical symptoms with imaging-confirmed instability. Retired in favour of the physical therapy pathway.',
   '{"MRI report","instability examination"}',
   false, '2024-01-01', NULL),
  ('1a000000-0000-4000-8000-000000000008', 'tenant-a', 'Hereditary cancer gene panel', 1,
   '{C50.911,Z80.3}', '{Z13.9}',
   'Covered with a qualifying family history and pre-test genetic counselling.',
   '{"three-generation family history","genetic counselling note"}',
   true, '2025-01-01', '2026-03-31')
ON CONFLICT (id) DO NOTHING;

-- tenant-b: 6 rows, 5 effective today. M54.5 is excluded here and covered for
-- tenant-a, which is the point: policies are tenant data, not global truth.
INSERT INTO policies (
  id, tenant_id, name, version, covered_codes, excluded_codes,
  criteria_text, required_evidence, active, effective_from, effective_to
) VALUES
  ('1b000000-0000-4000-8000-000000000001', 'tenant-b', 'Shoulder MRI', 1,
   '{M75.100,M75.41,M25.511}', '{}',
   'Covered after six weeks of conservative management without improvement.',
   '{"range of motion assessment","conservative therapy >= 6 weeks"}',
   true, '2025-01-01', NULL),
  ('1b000000-0000-4000-8000-000000000002', 'tenant-b', 'Cardiac stress test', 1,
   '{I25.10,R07.9,I21.4}', '{Z13.6}',
   'Covered for symptomatic members; screening of asymptomatic members is excluded.',
   '{"symptom description","resting ECG"}',
   true, '2025-01-01', NULL),
  ('1b000000-0000-4000-8000-000000000003', 'tenant-b', 'Lumbar spinal fusion', 1,
   '{M43.16,M48.061}', '{M54.5}',
   'Covered for documented instability; isolated low back pain is not an indication.',
   '{"flexion-extension radiographs","conservative therapy >= 6 months"}',
   true, '2025-01-01', NULL),
  ('1b000000-0000-4000-8000-000000000004', 'tenant-b', 'Lumbar spinal fusion', 2,
   '{M43.16,M48.061,M51.26}', '{M54.5,M54.50}',
   'Covered for documented instability or stenosis with neurogenic claudication; isolated low back pain remains excluded.',
   '{"flexion-extension radiographs","conservative therapy >= 6 months","neurogenic claudication note"}',
   true, '2026-02-01', NULL),
  ('1b000000-0000-4000-8000-000000000005', 'tenant-b', 'Home oxygen therapy', 1,
   '{J44.1,J96.11,Z99.81}', '{Z87.891}',
   'Covered with a qualifying arterial blood gas or oximetry result on room air.',
   '{"oximetry on room air","pulmonary function test"}',
   true, '2025-04-01', NULL),
  ('1b000000-0000-4000-8000-000000000006', 'tenant-b', 'Continuous insulin pump', 1,
   '{E10.9,E11.65,Z79.4}', '{E11.9}',
   'Covered for members on intensive insulin therapy with documented glucose logs.',
   '{"90 days of glucose logs","HbA1c result"}',
   true, '2025-01-01', NULL)
ON CONFLICT (id) DO NOTHING;

-- tenant-legacy is inactive: ingestion fails on the tenant, never reaching this policy.
INSERT INTO policies (
  id, tenant_id, name, version, covered_codes, excluded_codes,
  criteria_text, required_evidence, active, effective_from, effective_to
) VALUES
  ('1c000000-0000-4000-8000-000000000001', 'tenant-legacy', 'Annual wellness visit', 1,
   '{Z00.00}', '{}',
   'Legacy contract, retained for historical claims only.',
   '{"visit summary"}',
   true, '2023-01-01', NULL)
ON CONFLICT (id) DO NOTHING;
