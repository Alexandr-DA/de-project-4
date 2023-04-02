-- 1. Таблицы STG слоя


CREATE TABLE IF NOT EXISTS stg.couriers (
	ts timestamp NOT NULL,
	value varchar NOT NULL,
	CONSTRAINT couriers_ts_key UNIQUE (ts)
);

CREATE TABLE IF NOT EXISTS stg.deliveries (
	ts timestamp NOT NULL,
	value varchar NOT NULL,
	CONSTRAINT deliveries_ts_key UNIQUE (ts)
);

CREATE TABLE IF NOT EXISTS stg.restaraunts (
	ts timestamp NOT NULL,
	value varchar NOT NULL,
	CONSTRAINT restaraunts_ts_key UNIQUE (ts)
);

-- 2. Таблицы DDS слоя

CREATE TABLE IF NOT EXISTS dds.hooks (
	"name" varchar NOT NULL,
	value varchar NOT NULL,
	CONSTRAINT hooks_name_key UNIQUE (name)
);

CREATE TABLE IF NOT EXISTS dds.adress (
	id serial4 NOT NULL,
	"name" varchar NOT NULL,
	CONSTRAINT adress_name_key UNIQUE (name)
);

CREATE TABLE IF NOT EXISTS dds.couriers (
	id serial4 NOT NULL,
	"_id" varchar NOT NULL,
	"name" varchar NOT NULL,
	CONSTRAINT couriers__id_key UNIQUE (_id)
);

CREATE TABLE IF NOT EXISTS dds.restaraunts (
	id serial4 NOT NULL,
	"_id" varchar NOT NULL,
	"name" varchar NOT NULL,
	CONSTRAINT restaraunts__id_key UNIQUE (_id)
);

CREATE TABLE IF NOT EXISTS dds.orders (
	id serial4 NOT NULL,
	order_ts timestamp NOT NULL,
	order_sum money NOT NULL,
	"name" varchar NOT NULL,
	CONSTRAINT orders_name_key UNIQUE (name)
);

CREATE TABLE IF NOT EXISTS dds.deliveries (
	id serial4 NOT NULL,
	courier_id int4 NOT NULL,
	adress_id int4 NOT NULL,
	"name" varchar NOT NULL,
	delivery_ts timestamp NOT NULL,
	order_id int4 NOT NULL,
	rate int4 NULL,
	tip_sum money NULL,
	CONSTRAINT deliveries_name_key UNIQUE (name)
);

-- 3. Таблицы CDM слоя
CREATE TABLE IF NOT EXISTS cdm.couriers_rate (
	id serial4 NOT NULL,
	courier_id int4 NOT NULL,
	"year" int4 NOT NULL,
	"month" int4 NOT NULL,
	average_rate numeric NOT NULL,
	CONSTRAINT couriers_rate_courier_id_year_month_key UNIQUE (courier_id, year, month)
);

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
	id serial4 NOT NULL,
	courier_id int4 NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year int4 NOT NULL,
	settlement_month int4 NOT NULL,
	orders_count int4 NOT NULL,
	orders_total_sum money NOT NULL,
	rate_avg numeric NOT NULL,
	order_processing_fee money NOT NULL,
	courier_order_sum money NOT NULL,
	courier_tips_sum money NOT NULL,
	courier_reward_sum money NOT NULL,
	CONSTRAINT dm_courier_ledger_courier_id_settlement_year_settlement_mon_key UNIQUE (courier_id, settlement_year, settlement_month)
);