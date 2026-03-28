from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from sqlalchemy import text

from illdashboard.database import create_database_engine
from illdashboard.database_migrations import MIGRATIONS, list_applied_migration_versions, prepare_main_database


def _create_pre_edit_schema(db_path: Path) -> None:
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(
            """
            PRAGMA foreign_keys=ON;

            CREATE TABLE lab_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename VARCHAR NOT NULL,
                filepath VARCHAR NOT NULL UNIQUE,
                mime_type VARCHAR NOT NULL,
                page_count INTEGER NOT NULL DEFAULT 1,
                status VARCHAR NOT NULL DEFAULT 'uploaded',
                processing_error TEXT,
                source_candidate VARCHAR,
                source_candidate_key VARCHAR,
                source_name VARCHAR,
                ocr_raw TEXT,
                ocr_text_raw TEXT,
                ocr_text_english TEXT,
                ocr_summary_english TEXT,
                lab_date TIMESTAMP,
                uploaded_at TIMESTAMP,
                text_assembled_at TIMESTAMP,
                summary_generated_at TIMESTAMP,
                source_resolved_at TIMESTAMP,
                search_indexed_at TIMESTAMP,
                updated_at TIMESTAMP
            );
            CREATE INDEX ix_lab_files_status ON lab_files(status);
            CREATE INDEX ix_lab_files_source_candidate_key ON lab_files(source_candidate_key);

            CREATE TABLE marker_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR NOT NULL UNIQUE,
                display_order INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX ix_marker_groups_name ON marker_groups(name);

            CREATE TABLE measurement_types (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR NOT NULL UNIQUE,
                normalized_key VARCHAR NOT NULL UNIQUE,
                group_name VARCHAR NOT NULL DEFAULT 'Other',
                group_id INTEGER,
                canonical_unit VARCHAR,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                FOREIGN KEY(group_id) REFERENCES marker_groups(id) ON DELETE SET NULL
            );
            CREATE INDEX ix_measurement_types_name ON measurement_types(name);
            CREATE INDEX ix_measurement_types_normalized_key ON measurement_types(normalized_key);
            CREATE INDEX ix_measurement_types_group_id ON measurement_types(group_id);

            CREATE TABLE measurements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lab_file_id INTEGER NOT NULL,
                measurement_type_id INTEGER,
                raw_marker_name VARCHAR NOT NULL,
                normalized_marker_key VARCHAR NOT NULL,
                original_value FLOAT,
                original_qualitative_value VARCHAR,
                qualitative_bool BOOLEAN,
                qualitative_value VARCHAR,
                original_unit VARCHAR,
                normalized_original_unit VARCHAR,
                canonical_unit VARCHAR,
                canonical_value FLOAT,
                original_reference_low FLOAT,
                original_reference_high FLOAT,
                canonical_reference_low FLOAT,
                canonical_reference_high FLOAT,
                measured_at TIMESTAMP,
                page_number INTEGER,
                batch_key VARCHAR,
                normalization_status VARCHAR NOT NULL DEFAULT 'pending',
                normalization_error TEXT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                FOREIGN KEY(lab_file_id) REFERENCES lab_files(id) ON DELETE CASCADE,
                FOREIGN KEY(measurement_type_id) REFERENCES measurement_types(id) ON DELETE SET NULL
            );
            CREATE INDEX ix_measurements_lab_file_id ON measurements(lab_file_id);
            CREATE INDEX ix_measurements_measurement_type_id ON measurements(measurement_type_id);
            CREATE INDEX ix_measurements_normalized_marker_key ON measurements(normalized_marker_key);
            CREATE INDEX ix_measurements_normalized_original_unit ON measurements(normalized_original_unit);
            CREATE INDEX ix_measurements_batch_key ON measurements(batch_key);
            CREATE INDEX ix_measurements_normalization_status ON measurements(normalization_status);

            INSERT INTO lab_files (
                filename,
                filepath,
                mime_type,
                page_count,
                status,
                lab_date,
                uploaded_at,
                updated_at
            ) VALUES (
                'legacy-report.pdf',
                '/tmp/legacy-report.pdf',
                'application/pdf',
                1,
                'complete',
                '2024-01-02 00:00:00',
                '2024-01-03 00:00:00',
                '2024-01-03 00:00:00'
            );

            INSERT INTO measurement_types (
                name,
                normalized_key,
                group_name,
                canonical_unit,
                created_at,
                updated_at
            ) VALUES (
                'Ferritin',
                'ferritin',
                'Other',
                'ug/L',
                '2024-01-03 00:00:00',
                '2024-01-03 00:00:00'
            );

            INSERT INTO measurements (
                lab_file_id,
                measurement_type_id,
                raw_marker_name,
                normalized_marker_key,
                original_value,
                original_unit,
                normalized_original_unit,
                canonical_unit,
                canonical_value,
                canonical_reference_low,
                canonical_reference_high,
                normalization_status,
                created_at,
                updated_at
            ) VALUES (
                1,
                1,
                'Ferritin',
                'ferritin',
                42.0,
                'ug/L',
                'ug/l',
                'ug/L',
                42.0,
                30.0,
                400.0,
                'resolved',
                '2024-01-03 00:00:00',
                '2024-01-03 00:00:00'
            );
            """
        )
        connection.commit()
    finally:
        connection.close()


@pytest.mark.asyncio
async def test_prepare_main_database_records_current_version_for_fresh_database(tmp_path: Path):
    db_path = tmp_path / "fresh.db"
    engine = create_database_engine(f"sqlite+aiosqlite:///{db_path}")

    try:
        applied = await prepare_main_database(engine)
        assert [migration.version for migration in applied] == [migration.version for migration in MIGRATIONS]
        assert await list_applied_migration_versions(engine) == [migration.version for migration in MIGRATIONS]

        second_run = await prepare_main_database(engine)
        assert second_run == []
        assert await list_applied_migration_versions(engine) == [migration.version for migration in MIGRATIONS]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_prepare_main_database_migrates_pre_edit_schema(tmp_path: Path):
    db_path = tmp_path / "legacy.db"
    _create_pre_edit_schema(db_path)
    engine = create_database_engine(f"sqlite+aiosqlite:///{db_path}")

    try:
        applied = await prepare_main_database(engine)
        assert [migration.version for migration in applied] == [migration.version for migration in MIGRATIONS]
        assert await list_applied_migration_versions(engine) == [migration.version for migration in MIGRATIONS]

        async with engine.connect() as connection:
            lab_columns = {
                row[1]
                for row in (await connection.execute(text("PRAGMA table_info('lab_files')"))).all()
            }
            measurement_columns = {
                row[1]
                for row in (await connection.execute(text("PRAGMA table_info('measurements')"))).all()
            }
            file_row = (
                await connection.execute(
                    text(
                        """
                        SELECT filename, user_lab_date, user_lab_date_override
                        FROM lab_files
                        WHERE id = 1
                        """
                    )
                )
            ).one()
            measurement_row = (
                await connection.execute(
                    text(
                        """
                        SELECT
                            canonical_value,
                            user_canonical_value,
                            user_canonical_value_override,
                            user_canonical_unit,
                            user_canonical_unit_override,
                            user_original_unit,
                            user_original_unit_override,
                            user_canonical_reference_low_override,
                            user_canonical_reference_high_override
                        FROM measurements
                        WHERE id = 1
                        """
                    )
                )
            ).one()

        assert {"user_lab_date", "user_lab_date_override"}.issubset(lab_columns)
        assert {
            "user_original_unit",
            "user_original_unit_override",
            "user_canonical_unit",
            "user_canonical_unit_override",
            "user_canonical_value",
            "user_canonical_value_override",
            "user_qualitative_value",
            "user_qualitative_value_override",
            "user_qualitative_bool",
            "user_qualitative_bool_override",
            "user_canonical_reference_low",
            "user_canonical_reference_low_override",
            "user_canonical_reference_high",
            "user_canonical_reference_high_override",
            "user_measured_at",
            "user_measured_at_override",
            "user_edited_at",
        }.issubset(measurement_columns)

        assert tuple(file_row) == ("legacy-report.pdf", None, 0)
        assert tuple(measurement_row) == (42.0, None, 0, None, 0, None, 0, 0, 0)
    finally:
        await engine.dispose()
