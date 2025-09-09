import logging
import json

def handle_failure(error_message, output_path="./failure_output.json"):
    """
    Handle task failure by logging the error, suggesting possible resolutions, and saving the output to a JSON file.

    Args:
        error_message (str): The error message associated with the failure.
        output_path (str): Path to save the failure details as a JSON file.
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    logger.error("Task Failed.")
    logger.error(f"Error Details: {error_message}")

    # Suggest troubleshooting steps
    troubleshooting_steps = []
    if "NonExistentColumn" in error_message:
        troubleshooting_steps = [
            "1. Verify that all column names used in the transformations exist in the input dataset.",
            "2. Check for typos in column names (e.g., 'NonExistentColumn').",
            "3. Use 'transformed_data.printSchema()' to inspect the dataset schema and ensure all expected columns are present.",
            "4. Debug the data loading process to verify the input dataset contains the expected schema."
        ]
    elif "Delta table" in error_message:
        troubleshooting_steps = [
            "1. Verify that the Delta table exists and is accessible.",
            "2. Check if the path or catalog/schema is correctly specified."
        ]
    elif "row count" in error_message:
        troubleshooting_steps = [
            "1. Ensure the dataset meets the minimum row count requirement.",
            "2. Verify the data integrity and filter conditions."
        ]
    else:
        troubleshooting_steps = [
            "1. Review the task logs for more details on the failure.",
            "2. Debug the notebook and inspect intermediate outputs."
        ]

    next_steps = [
        "1. Fix the identified issues in the notebook or input data.",
        "2. Re-run the pipeline after applying the fixes."
    ]

    logger.info("Possible Troubleshooting Steps:")
    for step in troubleshooting_steps:
        logger.info(step)

    logger.info("Recommended Next Steps:")
    for step in next_steps:
        logger.info(step)

    # Save error details to JSON file
    failure_output = {
        "status": "failed",
        "error_message": error_message,
        "troubleshooting_steps": troubleshooting_steps,
        "next_steps": next_steps
    }
    with open(output_path, "w") as file:
        json.dump(failure_output, file, indent=4)
    logger.info(f"Failure details saved to {output_path}")

# Example usage
error_message = "Column 'NonExistentColumn' not found in input dataset"
handle_failure(error_message)