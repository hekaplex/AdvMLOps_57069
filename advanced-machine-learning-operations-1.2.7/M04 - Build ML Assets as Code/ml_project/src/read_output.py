import json

# Path to the JSON file
output_file_path = "./model_evaluation_output.json"

# Read and print the output results
try:
    with open(output_file_path, "r") as f:
        output_results = json.load(f)
    print("Model Evaluation Output Results:")
    for key, value in output_results.items():
        print(f"{key}: {value}")
except FileNotFoundError:
    print(f"Error: Output file not found at {output_file_path}")
except Exception as e:
    print(f"An error occurred: {e}")
