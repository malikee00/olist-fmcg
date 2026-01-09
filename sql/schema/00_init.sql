-- sql/schema/00_init.sql

-- =========================
-- SCHEMAS
-- =========================
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS analytics;

-- =========================
-- DIMENSION: PRODUCT CATEGORY (PT -> EN)
-- =========================
CREATE TABLE IF NOT EXISTS analytics.dim_product_category (
  category_pt TEXT PRIMARY KEY,
  category_en TEXT NOT NULL
);

INSERT INTO analytics.dim_product_category (category_pt, category_en) VALUES
('agro_industria_e_comercio', 'Agro Industry & Commerce'),
('alimentos', 'Food'),
('alimentos_bebidas', 'Food & Beverages'),
('artes', 'Arts'),
('artes_e_artesanato', 'Arts & Crafts'),
('artigos_de_festas', 'Party Supplies'),
('artigos_de_natal', 'Christmas Supplies'),
('audio', 'Audio'),
('automotivo', 'Automotive'),
('bebes', 'Baby Products'),
('bebidas', 'Beverages'),
('beleza_saude', 'Health & Beauty'),
('brinquedos', 'Toys'),
('cama_mesa_banho', 'Home & Living'),
('casa_conforto', 'Home Comfort'),
('casa_conforto_2', 'Home Comfort (2)'),
('casa_construcao', 'Home Construction'),
('cds_dvds_musicais', 'Music CDs & DVDs'),
('cine_foto', 'Cameras & Photography'),
('climatizacao', 'Climate Control'),
('consoles_games', 'Game Consoles & Games'),
('construcao_ferramentas_construcao', 'Construction Tools'),
('construcao_ferramentas_ferramentas', 'Tools'),
('construcao_ferramentas_iluminacao', 'Lighting & Electrical Tools'),
('construcao_ferramentas_jardim', 'Garden Tools (Construction)'),
('construcao_ferramentas_seguranca', 'Security Tools'),
('cool_stuff', 'Cool Stuff'),
('dvds_blu_ray', 'DVDs & Blu-ray'),
('eletrodomesticos', 'Home Appliances'),
('eletrodomesticos_2', 'Home Appliances (2)'),
('eletronicos', 'Electronics'),
('eletroportateis', 'Portable Electronics'),
('esporte_lazer', 'Sports & Leisure'),
('fashion_bolsas_e_acessorios', 'Fashion Bags & Accessories'),
('fashion_calcados', 'Fashion Shoes'),
('fashion_esporte', 'Sports Fashion'),
('fashion_roupa_feminina', 'Women''s Clothing'),
('fashion_roupa_infanto_juvenil', 'Kids'' Clothing'),
('fashion_roupa_masculina', 'Men''s Clothing'),
('fashion_underwear_e_moda_praia', 'Underwear & Beachwear'),
('flores', 'Flowers'),
('fraldas_higiene', 'Diapers & Hygiene'),
('ferramentas_jardim', 'Garden Tools'),
('industria_comercio_e_negocios', 'Industry, Commerce & Business'),
('informatica_acessorios', 'Computers & Accessories'),
('instrumentos_musicais', 'Musical Instruments'),
('la_cuisine', 'La Cuisine'),
('livros_importados', 'Imported Books'),
('livros_interesse_geral', 'General Interest Books'),
('livros_tecnicos', 'Technical Books'),
('malas_acessorios', 'Luggage & Accessories'),
('market_place', 'Marketplace'),
('moveis_colchao_e_estofado', 'Mattresses & Upholstery'),
('moveis_cozinha_area_de_servico_jantar_e_jardim', 'Kitchen, Dining & Garden Furniture'),
('moveis_decoracao', 'Furniture & Decor'),
('moveis_escritorio', 'Office Furniture'),
('moveis_quarto', 'Bedroom Furniture'),
('moveis_sala', 'Living Room Furniture'),
('musica', 'Music'),
('papelaria', 'Stationery'),
('pc_gamer', 'Gaming PC'),
('pcs', 'PCs'),
('perfumaria', 'Perfume & Cosmetics'),
('pet_shop', 'Pet Supplies'),
('portateis_casa_forno_e_cafe', 'Portable Home, Oven & Coffee'),
('portateis_cozinha_e_preparadores_de_alimentos', 'Kitchen Appliances & Food Prep'),
('relogios_presentes', 'Watches & Gifts'),
('seguros_e_servicos', 'Insurance & Services'),
('sinalizacao_e_seguranca', 'Signage & Security'),
('tablets_impressao_imagem', 'Tablets, Printing & Imaging'),
('telefonia', 'Mobile Phones'),
('telefonia_fixa', 'Landline Phones'),
('unknown', 'Unknown'),
('utilidades_domesticas', 'Household Utilities')
ON CONFLICT (category_pt) DO UPDATE
SET category_en = EXCLUDED.category_en;

-- =========================
-- NEW FACT TABLE: PBI UNIFIED FACT (DAILY GRAIN)
-- =========================
CREATE TABLE IF NOT EXISTS analytics.pbi_fact_daily (
  date                  date NOT NULL,
  month_date            date NOT NULL,
  customer_state        text NOT NULL,
  payment_type          text NOT NULL,
  product_category_name text NOT NULL,

  revenue              double precision NOT NULL DEFAULT 0,
  orders               bigint NOT NULL DEFAULT 0,
  total_payment_value  double precision NOT NULL DEFAULT 0,
  avg_price            double precision,
  avg_freight          double precision,

  batch_id             text NOT NULL,
  updated_at           timestamptz NOT NULL DEFAULT now(),

  -- Optional stable id (can be filled by pipeline later; not required now)
  fact_id              text
);

-- Helpful indexes for slicers & performance
CREATE INDEX IF NOT EXISTS idx_pbi_fact_daily_month_date ON analytics.pbi_fact_daily (month_date);
CREATE INDEX IF NOT EXISTS idx_pbi_fact_daily_state ON analytics.pbi_fact_daily (customer_state);
CREATE INDEX IF NOT EXISTS idx_pbi_fact_daily_payment ON analytics.pbi_fact_daily (payment_type);
CREATE INDEX IF NOT EXISTS idx_pbi_fact_daily_category ON analytics.pbi_fact_daily (product_category_name);
CREATE INDEX IF NOT EXISTS idx_pbi_fact_daily_month_state_pay_cat
  ON analytics.pbi_fact_daily (month_date, customer_state, payment_type, product_category_name);

-- Enforce uniqueness at the grain (this is the real key anyway)
CREATE UNIQUE INDEX IF NOT EXISTS ux_pbi_fact_daily_grain
  ON analytics.pbi_fact_daily (date, month_date, customer_state, payment_type, product_category_name, batch_id);
