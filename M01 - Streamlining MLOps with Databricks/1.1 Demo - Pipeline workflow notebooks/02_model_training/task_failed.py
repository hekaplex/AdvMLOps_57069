import logging
import json

def handle_failure(error_message, output_path="../failure_output.json"):
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
    if "row count" in error_message:
        troubleshooting_steps = [
            "- Ensure the dataset meets the minimum row count requirement.",
            "- Verify the data integrity and filter conditions."
        ]
    elif "Delta table" in error_message:
        troubleshooting_steps = [
            "- Verify that the Delta table exists and is accessible.",
            "- Check if the path or catalog/schema is correctly specified."
        ]
    elif "NonExistentColumn" in error_message:
        troubleshooting_steps = [
            "- Verify that all column names used in the transformations exist in the input dataset.",
            "- Check for typos in column names (e.g., 'NonExistentColumn').",
            "- Ensure the input dataset contains the expected schema."
        ]
    else:
        troubleshooting_steps = [
            "- Review the task logs for more details on the failure.",
            "- Debug the notebook and inspect intermediate outputs."
        ]

    next_steps = [
        "- Fix the identified issues in the notebook or input data.",
        "- Re-run the pipeline after applying the fixes."
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
error_message = "Sample error: NonExistentColumn not found in input dataset"
handle_failure(error_message)