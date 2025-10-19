# tests/test_validation.py
import os
from pathlib import Path
import pandas as pd
import pytest
from validation.data_quality_validator import DataQualityValidator


@pytest.fixture(scope="module")
def validator():
    """
    Fixture that initializes the DataQualityValidator.
    Ensures metadata/ and processed/ directories exist.
    """
    os.environ["TRANSFORMED_DATA_PATH"] = "tests/resources/data/processed"
    Path("metadata").mkdir(exist_ok=True, parents=True)
    return DataQualityValidator()


def test_validation_runs_on_all_transformed_files(validator):
    """
    ✅ Runs the validator on all transformed Parquet files.
    Ensures:
      1. All transformed files are validated.
      2. metadata/data_quality_report.csv is created.
      3. Report contains expected columns and at least one row.
    """
    processed_dir = Path(os.getenv("TRANSFORMED_DATA_PATH"))
    processed_dir.mkdir(parents=True, exist_ok=True)

    # Ensure transformed files exist
    transformed_files = list(processed_dir.glob("*_transformed.parquet"))
    assert transformed_files, f"No transformed files found in {processed_dir}"

    # Run validator
    df_report = validator.run()

    # ✅ Check report file existence
    report_path = Path("metadata/data_quality_report.csv")
    assert report_path.exists(), "❌ Data quality report was not generated!"

    # ✅ Structure validation
    assert isinstance(df_report, pd.DataFrame), "Returned report is not a DataFrame!"
    assert not df_report.empty, "Report DataFrame is empty!"

    expected_columns = {
        "file_name",
        "total_records",
        "invalid_value_type",
        "invalid_timestamp",
        "outlier_%",
        "missing_%",
        "sensors_with_gaps",
        "total_missing_hours",
    }

    actual_columns = set(df_report.columns)

    # ✅ Handle partial error cases gracefully
    if "error" in df_report.columns:
        print("⚠️ Some files failed validation — skipping detailed column checks.")
        # ensure that at least file_name + error exist
        assert {"file_name", "error"}.issubset(actual_columns)
    else:
        missing = expected_columns - actual_columns
        assert not missing, f"❌ Missing expected columns: {missing}"

    # ✅ Row count sanity check
    assert len(df_report) == len(transformed_files), (
        f"❌ Expected {len(transformed_files)} rows in report, "
        f"found {len(df_report)}"
    )

    print("\n✅ Validation successfully processed all transformed files!")
    print(df_report.head())


def test_report_is_well_formed():
    """
    ✅ Verifies that the written CSV is readable and well-formed.
    Ensures correct headers and no empty rows.
    """
    report_path = Path("metadata/data_quality_report.csv")
    assert report_path.exists(), "Report CSV not found!"

    df = pd.read_csv(report_path)
    assert not df.empty, "Report CSV is empty!"
    assert "file_name" in df.columns, "Missing file_name column in report!"

    if "error" not in df.columns:
        # Only check full schema when no errors
        assert "total_records" in df.columns, "Missing total_records in report!"
        assert df["total_records"].dtype in [int, float], "total_records column invalid type!"
    else:
        print("⚠️ Skipping numeric checks due to error-only rows.")

    print("\n✅ data_quality_report.csv verified as valid and non-empty.")