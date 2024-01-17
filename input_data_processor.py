import re
import os
import argparse

def main(input_directory, output_directory):
    # Ensure the output directory exists
    os.makedirs(output_directory, exist_ok=True)

    # List all files in the input directory
    all_files = os.listdir(input_directory)

    # Filter files that match the pattern 'combined_data_*.txt'
    pattern = r'combined_data_\d+\.txt'
    files_to_process = [f for f in all_files if re.match(pattern, f)]

    # Process each file
    for file_name in files_to_process:
        print(f'Processing file {input_directory}{file_name}')

        # Open the file for reading
        with open(f'{input_directory}{file_name}', 'r') as file:
            data = file.read()

        # Write the processed data to a new file in the output directory
        with open(f'{output_directory}{file_name}', 'w') as f:
            f.write(re.sub(r"\n(?=\d+:)", "\n\n", data))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process files from input directory and save them to output directory.')
    parser.add_argument('input_directory', type=str, help='Directory where the input files are stored')
    parser.add_argument('output_directory', type=str, help='Directory where the output files should be saved')

    args = parser.parse_args()
    main(args.input_directory, args.output_directory)
