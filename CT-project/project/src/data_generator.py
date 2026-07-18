import os
import shutil
import argparse
from pipeline_utils import log_event

def get_paths():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    datasets_dir = os.path.join(base_dir, "datasets")
    landing_dir = os.path.join(base_dir, "raw_landing")
    return datasets_dir, landing_dir

def reset_landing():
    _, landing_dir = get_paths()
    if os.path.exists(landing_dir):
        # Delete individual files to prevent WinError 5 directory lock failures on Windows
        for filename in os.listdir(landing_dir):
            file_path = os.path.join(landing_dir, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                log_event("DATA_GENERATOR", f"Failed to delete {filename}: {str(e)}", "WARNING")
        log_event("DATA_GENERATOR", "Cleared raw landing directory contents.")
    else:
        os.makedirs(landing_dir, exist_ok=True)

def simulate_step(step_number):
    datasets_dir, landing_dir = get_paths()
    os.makedirs(landing_dir, exist_ok=True)
    
    files_to_copy = {
        1: ("Fact_Sales_1.csv", "Fact_Sales_1.csv"),
        2: ("Fact_Sales_2.csv", "Fact_Sales_2.csv"),
        3: ("2010-12-08.csv", "2010-12-08_new_source.csv") # renamed to avoid space issues
    }
    
    if step_number not in files_to_copy:
        log_event("DATA_GENERATOR", f"Invalid step number: {step_number}", "ERROR")
        return False
        
    src_filename, target_filename = files_to_copy[step_number]
    src_path = os.path.join(datasets_dir, src_filename)
    dest_path = os.path.join(landing_dir, target_filename)
    
    if not os.path.exists(src_path):
        log_event("DATA_GENERATOR", f"Source dataset file not found: {src_path}", "ERROR")
        return False
        
    shutil.copy2(src_path, dest_path)
    log_event("DATA_GENERATOR", f"Successfully copied {src_filename} to raw_landing as {target_filename}", "INFO")
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sentinel Landing Zone Simulator")
    parser.add_argument("--step", type=int, choices=[1, 2, 3], help="Execute simulation step (1: base sales, 2: incremental sales, 3: schema evolution)")
    parser.add_argument("--reset", action="store_true", help="Reset and clear raw landing zone")
    
    args = parser.parse_args()
    
    if args.reset:
        reset_landing()
    elif args.step is not None:
        simulate_step(args.step)
    else:
        parser.print_help()
