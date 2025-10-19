# src/main.py
from ingestion.ingestion_pipeline import IngestionPipeline
from transformation.transformation_pipeline import TransformationPipeline
from pathlib import Path

from validation.data_quality_validator import DataQualityValidator

def run_ingestion():
    print("🚀 Starting Ingestion Phase...")
    ingestion_pipeline = IngestionPipeline()
    dfs = ingestion_pipeline.run()
    print("✅ Ingestion Completed.\n")
    return dfs

def run_transformation():
    print("🔄 Starting Transformation Phase...")
    tp = TransformationPipeline()

    processed_dir = Path("data/processed")
    processed_files = list(processed_dir.glob("*_processed.parquet"))

    if not processed_files:
        print("⚠️ No processed files found. Run ingestion first.")
        return

    for file in processed_files:
        print(f"\n--- Transforming {file.name} ---")
        tp.run(str(file))

    print("\n✅ Transformation Completed.")

def run_validation():
    print("\n🧠 Starting Data Quality Validation Phase...")
    dqv = DataQualityValidator()
    dqv.run()
    print("✅ Data Quality Validation Completed.")

def run_storage_optimization():
    print("\n💾 Starting Storage Optimization Phase...")
    from storage.data_loader import DataLoader

    dl = DataLoader()
    dl.run()
    print("✅ Storage Optimization Completed.")

def main():
    print("====================================")
    print("   🌾 Agricultural Data Pipeline   ")
    print("====================================\n")

    run_ingestion()
    run_transformation()
    run_validation()
    run_storage_optimization()

if __name__ == "__main__":
    main()
