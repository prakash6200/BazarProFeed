package config

import (
	"feedprovider/models"
	"fmt"
	"log"
	"sync"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	DB   *gorm.DB
	dbMu sync.RWMutex
)

func ensureEnumTypes(db *gorm.DB) error {
	createEnumsSQL := `
DO $$
BEGIN
	IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'user_role') THEN
		CREATE TYPE user_role AS ENUM ('USER', 'ADMIN');
	END IF;

	IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'instrument_status') THEN
		CREATE TYPE instrument_status AS ENUM ('ACTIVE', 'INACTIVE');
	END IF;

	IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'instrument_segment') THEN
		CREATE TYPE instrument_segment AS ENUM ('NFO-FUT', 'MCX-FUT', 'NFO-OPT', 'CDS-FUT', 'EQUITY');
	END IF;
	ALTER TYPE instrument_segment ADD VALUE IF NOT EXISTS 'NFO-FUT';
	ALTER TYPE instrument_segment ADD VALUE IF NOT EXISTS 'MCX-FUT';
	ALTER TYPE instrument_segment ADD VALUE IF NOT EXISTS 'NFO-OPT';
	ALTER TYPE instrument_segment ADD VALUE IF NOT EXISTS 'CDS-FUT';
	ALTER TYPE instrument_segment ADD VALUE IF NOT EXISTS 'EQUITY';

	IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'instrument_exchange') THEN
		CREATE TYPE instrument_exchange AS ENUM ('NSE', 'MCX', 'MCX-MINI', 'CE-PE', 'CDS', 'NSE-EQU');
	END IF;
	ALTER TYPE instrument_exchange ADD VALUE IF NOT EXISTS 'NSE';
	ALTER TYPE instrument_exchange ADD VALUE IF NOT EXISTS 'MCX';
	ALTER TYPE instrument_exchange ADD VALUE IF NOT EXISTS 'MCX-MINI';
	ALTER TYPE instrument_exchange ADD VALUE IF NOT EXISTS 'CE-PE';
	ALTER TYPE instrument_exchange ADD VALUE IF NOT EXISTS 'CDS';
	ALTER TYPE instrument_exchange ADD VALUE IF NOT EXISTS 'NSE-EQU';

	IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'global_market_segment') THEN
		CREATE TYPE global_market_segment AS ENUM ('OTHERS', 'USSTOCK', 'COMEX', 'CRYPTO', 'FOREX', 'GIFT');
	END IF;

	IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'global_market_exchange') THEN
		CREATE TYPE global_market_exchange AS ENUM ('OTHERS', 'USSTOCK', 'COMEX', 'CRYPTO', 'FOREX', 'GIFT');
	END IF;
END
$$;`

	if err := db.Exec(createEnumsSQL).Error; err != nil {
		return err
	}

	convertUsersRoleSQL := `
DO $$
DECLARE
	role_udt text;
BEGIN
	IF EXISTS (
		SELECT 1
		FROM information_schema.columns
		WHERE table_schema = 'public'
		  AND table_name = 'users'
		  AND column_name = 'role'
	) THEN
		SELECT udt_name
		INTO role_udt
		FROM information_schema.columns
		WHERE table_schema = 'public'
		  AND table_name = 'users'
		  AND column_name = 'role'
		LIMIT 1;

		IF role_udt = 'user_role' THEN
			UPDATE users
			SET role = 'USER'::user_role
			WHERE role IS NULL OR UPPER(role::text) NOT IN ('USER', 'ADMIN');
		ELSE
			UPDATE users
			SET role = 'USER'
			WHERE role IS NULL OR UPPER(TRIM(role::text)) NOT IN ('USER', 'ADMIN');

			ALTER TABLE users
				ALTER COLUMN role DROP DEFAULT;

			ALTER TABLE users
				ALTER COLUMN role TYPE user_role
				USING UPPER(TRIM(role::text))::user_role;
		END IF;

		ALTER TABLE users
			ALTER COLUMN role SET DEFAULT 'USER';

		ALTER TABLE users
			ALTER COLUMN role SET NOT NULL;
	END IF;
END
$$;`

	if err := db.Exec(convertUsersRoleSQL).Error; err != nil {
		return err
	}

	convertInstrumentStatusSQL := `
DO $$
DECLARE
	status_udt text;
BEGIN
	IF EXISTS (
		SELECT 1
		FROM information_schema.columns
		WHERE table_schema = 'public'
		  AND table_name = 'instruments'
		  AND column_name = 'status'
	) THEN
		SELECT udt_name
		INTO status_udt
		FROM information_schema.columns
		WHERE table_schema = 'public'
		  AND table_name = 'instruments'
		  AND column_name = 'status'
		LIMIT 1;

		IF status_udt = 'instrument_status' THEN
			UPDATE instruments
			SET status = 'ACTIVE'::instrument_status
			WHERE status IS NULL OR UPPER(status::text) NOT IN ('ACTIVE', 'INACTIVE');
		ELSE
			UPDATE instruments
			SET status = 'ACTIVE'
			WHERE status IS NULL OR UPPER(TRIM(status::text)) NOT IN ('ACTIVE', 'INACTIVE');

			ALTER TABLE instruments
				ALTER COLUMN status DROP DEFAULT;

			ALTER TABLE instruments
				ALTER COLUMN status TYPE instrument_status
				USING UPPER(TRIM(status::text))::instrument_status;
		END IF;

		ALTER TABLE instruments
			ALTER COLUMN status SET DEFAULT 'ACTIVE';

		ALTER TABLE instruments
			ALTER COLUMN status SET NOT NULL;
	END IF;
END
$$;`

	if err := db.Exec(convertInstrumentStatusSQL).Error; err != nil {
		return err
	}

	convertInstrumentSegmentSQL := `
DO $$
DECLARE
	segment_udt text;
BEGIN
	IF EXISTS (
		SELECT 1
		FROM information_schema.columns
		WHERE table_schema = 'public'
		  AND table_name = 'instruments'
		  AND column_name = 'segment'
	) THEN
		SELECT udt_name
		INTO segment_udt
		FROM information_schema.columns
		WHERE table_schema = 'public'
		  AND table_name = 'instruments'
		  AND column_name = 'segment'
		LIMIT 1;

		IF segment_udt = 'instrument_segment' THEN
			UPDATE instruments
			SET segment = 'EQUITY'::instrument_segment
			WHERE UPPER(TRIM(segment::text)) = 'INDICES';

			UPDATE instruments
			SET segment = 'NFO-FUT'::instrument_segment
			WHERE segment IS NULL OR UPPER(TRIM(segment::text)) NOT IN ('NFO-FUT', 'MCX-FUT', 'NFO-OPT', 'CDS-FUT', 'EQUITY');
		ELSE
			UPDATE instruments
			SET segment = 'EQUITY'
			WHERE UPPER(TRIM(segment::text)) = 'INDICES';

			UPDATE instruments
			SET segment = 'NFO-FUT'
			WHERE segment IS NULL OR UPPER(TRIM(segment::text)) NOT IN ('NFO-FUT', 'MCX-FUT', 'NFO-OPT', 'CDS-FUT', 'EQUITY');

			ALTER TABLE instruments
				ALTER COLUMN segment DROP DEFAULT;

			ALTER TABLE instruments
				ALTER COLUMN segment TYPE instrument_segment
				USING UPPER(TRIM(segment::text))::instrument_segment;
		END IF;

		ALTER TABLE instruments
			ALTER COLUMN segment SET DEFAULT 'NFO-FUT';

		ALTER TABLE instruments
			ALTER COLUMN segment SET NOT NULL;

		ALTER TABLE instruments
			DROP CONSTRAINT IF EXISTS instruments_segment_allowed_chk;

		ALTER TABLE instruments
			ADD CONSTRAINT instruments_segment_allowed_chk
			CHECK (segment IN ('NFO-FUT', 'MCX-FUT', 'NFO-OPT', 'CDS-FUT', 'EQUITY'));
	END IF;
END
$$;`

	if err := db.Exec(convertInstrumentSegmentSQL).Error; err != nil {
		return err
	}

	convertInstrumentExchangeSQL := `
DO $$
DECLARE
	exchange_udt text;
BEGIN
	IF EXISTS (
		SELECT 1
		FROM information_schema.columns
		WHERE table_schema = 'public'
		  AND table_name = 'instruments'
		  AND column_name = 'exchange'
	) THEN
		SELECT udt_name
		INTO exchange_udt
		FROM information_schema.columns
		WHERE table_schema = 'public'
		  AND table_name = 'instruments'
		  AND column_name = 'exchange'
		LIMIT 1;

		IF exchange_udt = 'instrument_exchange' THEN
			UPDATE instruments
			SET exchange = 'CE-PE'::instrument_exchange
			WHERE UPPER(TRIM(exchange::text)) = 'CEPE';

			UPDATE instruments
			SET exchange = 'NSE-EQU'::instrument_exchange
			WHERE UPPER(TRIM(exchange::text)) IN ('NFO', 'NCO');

			UPDATE instruments
			SET exchange = 'NSE'::instrument_exchange
			WHERE exchange IS NULL OR UPPER(TRIM(exchange::text)) NOT IN ('NSE', 'MCX', 'MCX-MINI', 'CE-PE', 'CDS', 'NSE-EQU');
		ELSE
			UPDATE instruments
			SET exchange = 'CE-PE'
			WHERE UPPER(TRIM(exchange::text)) = 'CEPE';

			UPDATE instruments
			SET exchange = 'NSE-EQU'
			WHERE UPPER(TRIM(exchange::text)) IN ('NFO', 'NCO');

			UPDATE instruments
			SET exchange = 'NSE'
			WHERE exchange IS NULL OR UPPER(TRIM(exchange::text)) NOT IN ('NSE', 'MCX', 'MCX-MINI', 'CE-PE', 'CDS', 'NSE-EQU');

			ALTER TABLE instruments
				ALTER COLUMN exchange DROP DEFAULT;

			ALTER TABLE instruments
				ALTER COLUMN exchange TYPE instrument_exchange
				USING UPPER(TRIM(exchange::text))::instrument_exchange;
		END IF;

		ALTER TABLE instruments
			ALTER COLUMN exchange SET DEFAULT 'NSE';

		ALTER TABLE instruments
			ALTER COLUMN exchange SET NOT NULL;

		ALTER TABLE instruments
			DROP CONSTRAINT IF EXISTS instruments_exchange_allowed_chk;

		ALTER TABLE instruments
			ADD CONSTRAINT instruments_exchange_allowed_chk
			CHECK (exchange IN ('NSE', 'MCX', 'MCX-MINI', 'CE-PE', 'CDS', 'NSE-EQU'));
	END IF;
END
$$;`

	if err := db.Exec(convertInstrumentExchangeSQL).Error; err != nil {
		return err
	}

	convertGlobalMarketEnumSQL := `
DO $$
DECLARE
	segment_udt text;
	exchange_udt text;
BEGIN
	IF EXISTS (
		SELECT 1
		FROM information_schema.columns
		WHERE table_schema = 'public'
		  AND table_name = 'global_instruments'
		  AND column_name = 'segment'
	) THEN
		SELECT udt_name
		INTO segment_udt
		FROM information_schema.columns
		WHERE table_schema = 'public'
		  AND table_name = 'global_instruments'
		  AND column_name = 'segment'
		LIMIT 1;

		IF segment_udt = 'global_market_segment' THEN
			UPDATE global_instruments
			SET segment = 'OTHERS'::global_market_segment
			WHERE segment IS NULL OR UPPER(segment::text) NOT IN ('OTHERS', 'USSTOCK', 'COMEX', 'CRYPTO', 'FOREX', 'GIFT');
		ELSE
			UPDATE global_instruments
			SET segment = 'OTHERS'
			WHERE segment IS NULL OR UPPER(TRIM(segment::text)) NOT IN ('OTHERS', 'USSTOCK', 'COMEX', 'CRYPTO', 'FOREX', 'GIFT');

			ALTER TABLE global_instruments
				ALTER COLUMN segment DROP DEFAULT;

			ALTER TABLE global_instruments
				ALTER COLUMN segment TYPE global_market_segment
				USING UPPER(TRIM(segment::text))::global_market_segment;
		END IF;

		ALTER TABLE global_instruments
			ALTER COLUMN segment SET DEFAULT 'OTHERS';

		ALTER TABLE global_instruments
			ALTER COLUMN segment SET NOT NULL;
	END IF;

	IF EXISTS (
		SELECT 1
		FROM information_schema.columns
		WHERE table_schema = 'public'
		  AND table_name = 'global_instruments'
		  AND column_name = 'exchange'
	) THEN
		SELECT udt_name
		INTO exchange_udt
		FROM information_schema.columns
		WHERE table_schema = 'public'
		  AND table_name = 'global_instruments'
		  AND column_name = 'exchange'
		LIMIT 1;

		IF exchange_udt = 'global_market_exchange' THEN
			UPDATE global_instruments
			SET exchange = 'OTHERS'::global_market_exchange
			WHERE exchange IS NULL OR UPPER(exchange::text) NOT IN ('OTHERS', 'USSTOCK', 'COMEX', 'CRYPTO', 'FOREX', 'GIFT');
		ELSE
			UPDATE global_instruments
			SET exchange = 'OTHERS'
			WHERE exchange IS NULL OR UPPER(TRIM(exchange::text)) NOT IN ('OTHERS', 'USSTOCK', 'COMEX', 'CRYPTO', 'FOREX', 'GIFT');

			ALTER TABLE global_instruments
				ALTER COLUMN exchange DROP DEFAULT;

			ALTER TABLE global_instruments
				ALTER COLUMN exchange TYPE global_market_exchange
				USING UPPER(TRIM(exchange::text))::global_market_exchange;
		END IF;

		ALTER TABLE global_instruments
			ALTER COLUMN exchange SET DEFAULT 'OTHERS';

		ALTER TABLE global_instruments
			ALTER COLUMN exchange SET NOT NULL;
	END IF;
END
$$;`

	return db.Exec(convertGlobalMarketEnumSQL).Error
}

func ConnectDatabase() {
	dbMu.Lock()
	defer dbMu.Unlock()

	if DB != nil {
		return
	}

	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%s sslmode=%s",
		App.Database.Host,
		App.Database.User,
		App.Database.Password,
		App.Database.Name,
		App.Database.Port,
		App.Database.SSLMode,
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Fatal("failed to connect to database: ", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal("failed to access sql database: ", err)
	}

	sqlDB.SetMaxOpenConns(25)
	sqlDB.SetMaxIdleConns(10)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)
	sqlDB.SetConnMaxIdleTime(5 * time.Minute)

	if err := sqlDB.Ping(); err != nil {
		_ = sqlDB.Close()
		log.Fatal("failed to ping database: ", err)
	}

	if err := ensureEnumTypes(db); err != nil {
		_ = sqlDB.Close()
		log.Fatal("failed to ensure enum types: ", err)
	}

	if err := db.AutoMigrate(&models.User{}, &models.Instrument{}, &models.GlobalInstrument{}, &models.ZerodhaSession{}); err != nil {
		_ = sqlDB.Close()
		log.Fatal("failed to run migrations: ", err)
	}

	if err := db.Exec(`
UPDATE instruments
SET is_deleted = false
WHERE is_deleted IS NULL;

ALTER TABLE instruments
	ALTER COLUMN is_deleted SET DEFAULT false;

ALTER TABLE instruments
	ALTER COLUMN is_deleted SET NOT NULL;
`).Error; err != nil {
		_ = sqlDB.Close()
		log.Fatal("failed to finalize soft delete migration: ", err)
	}

	DB = db
	log.Println("database connected successfully")
	log.Println("database migrations completed")
}

func CloseDatabase() error {
	dbMu.Lock()
	defer dbMu.Unlock()

	if DB == nil {
		return nil
	}
	sqlDB, err := DB.DB()
	if err != nil {
		return err
	}
	err = sqlDB.Close()
	DB = nil
	return err
}
