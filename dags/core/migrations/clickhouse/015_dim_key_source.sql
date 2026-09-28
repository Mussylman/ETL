-- 015: откуда справочник берёт ключи.
--
--   facts  — строки заводят факты (stub по первой ссылке) — как было;
--   source — справочник сам регистрирует каждый объект источника (1С), независимо
--            от того, встречался ли он в продажах / заказах / остатках.
--
-- Номенклатура — source: идентичность товара — guid 1С (= retail products.uid), и у
-- каждого товара должен быть стабильный nomenklatura_id, даже если он не продавался
-- (себестоимость, остатки, прайс ссылаются на любой товар). Идемпотентна.

BEGIN;

ALTER TABLE etl_meta.registers
    ADD COLUMN IF NOT EXISTS dim_key_source varchar(8) NOT NULL DEFAULT 'facts';

DO $$ BEGIN
    ALTER TABLE etl_meta.registers ADD CONSTRAINT registers_dim_key_source_chk
        CHECK (dim_key_source IN ('facts', 'source'));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

UPDATE etl_meta.registers SET dim_key_source = 'source', updated_at = now()
WHERE code = 'dim_nomenklatura' AND dim_key_source <> 'source';

COMMIT;
