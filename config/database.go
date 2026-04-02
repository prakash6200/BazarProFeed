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
		CREATE TYPE instrument_segment AS ENUM ('MCX-FUT', 'NFO-FUT', 'NFO-OPT', 'CDS-FUT');
	END IF;
	ALTER TYPE instrument_segment ADD VALUE IF NOT EXISTS 'CDS-FUT';

	IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'instrument_exchange') THEN
		CREATE TYPE instrument_exchange AS ENUM ('CEPE', 'MCX', 'MCX-MINI', 'NSE', 'CDS');
	END IF;
	ALTER TYPE instrument_exchange ADD VALUE IF NOT EXISTS 'CDS';
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
			SET segment = 'NFO-FUT'::instrument_segment
			WHERE segment IS NULL OR UPPER(TRIM(segment::text)) NOT IN ('MCX-FUT', 'NFO-FUT', 'NFO-OPT', 'CDS-FUT');
		ELSE
			UPDATE instruments
			SET segment = 'NFO-FUT'
			WHERE segment IS NULL OR UPPER(TRIM(segment::text)) NOT IN ('MCX-FUT', 'NFO-FUT', 'NFO-OPT', 'CDS-FUT');

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
			SET exchange = 'NSE'::instrument_exchange
			WHERE exchange IS NULL OR UPPER(TRIM(exchange::text)) NOT IN ('CEPE', 'MCX', 'MCX-MINI', 'NSE', 'CDS');
		ELSE
			UPDATE instruments
			SET exchange = 'NSE'
			WHERE exchange IS NULL OR UPPER(TRIM(exchange::text)) NOT IN ('CEPE', 'MCX', 'MCX-MINI', 'NSE', 'CDS');

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
	END IF;
END
$$;`

	return db.Exec(convertInstrumentExchangeSQL).Error
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
