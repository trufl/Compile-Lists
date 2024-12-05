import os
import pandas as pd


def load_file(file_path):
   """
   Load a CSV or Excel file and return a DataFrame.


   Arguments:
   - file_path: The path to the file (CSV or Excel) to be loaded.


   Returns:
   - A pandas DataFrame containing the file's data.


   Steps:
   1. Extract the file extension from the provided file path.
   2. If the file is a CSV:
      - Use `pd.read_csv` to load the file.
   3. If the file is an Excel file (.xls or .xlsx):
      - Use `pd.read_excel` to load the file.
   4. If the file type is unsupported:
      - Raise a ValueError to alert the user.
   """
   file_extension = os.path.splitext(file_path)[-1].lower()  # Extract the file extension.


   if file_extension == ".csv":
       # Load the CSV file into a DataFrame.
       return pd.read_csv(file_path, dtype=object)
   elif file_extension in [".xls", ".xlsx"]:
       # Use appropriate engines for different Excel formats.
       engine = 'xlrd' if file_extension == '.xls' else 'openpyxl'


       # Load the Excel file into a DataFrame.
       return pd.read_excel(file_path, dtype=object, engine=engine)
   else:
       # Raise an error if the file type is unsupported.
       raise ValueError("Unsupported file type. Please provide a CSV or Excel file.")


def create_intermediate_csv(main_file_path, output_file_path):
   # Load the main file
   main_df = load_file(main_file_path)
  
   # Standardize column names to lowercase for easier processing
   main_df.columns = main_df.columns.str.strip().str.lower()
  
   # Identify email-related columns
   email_col = next((col for col in main_df.columns if col.lower() == 'email'), None)
   email2_col = next((col for col in main_df.columns if col.lower() == 'email2'), None)
  
   if not email_col:
       raise ValueError("The main file must contain an 'Email' column.")
  
   # Create a list to store the processed rows
   processed_rows = []
  
   for _, row in main_df.iterrows():
       # Case 1: Both 'Email' and 'Email2' have values
       if email2_col and pd.notna(row[email_col]) and pd.notna(row[email2_col]):
           # Append the original row
           processed_rows.append(row.to_dict())
          
           # Create a copy with swapped 'Email' and 'Email2' values
           row_copy = row.copy()
           row_copy[email_col], row_copy[email2_col] = row_copy[email2_col], row_copy[email_col]
           processed_rows.append(row_copy.to_dict())
      
       # Case 2: Only 'Email' has a value
       elif pd.notna(row[email_col]):
           processed_rows.append(row.to_dict())
      
       # Case 3: Only 'Email2' has a value
       elif email2_col and pd.notna(row[email2_col]):
           row[email_col] = row[email2_col]
           row[email2_col] = None
           processed_rows.append(row.to_dict())
  
   # Create a new DataFrame from the processed rows
   intermediate_df = pd.DataFrame(processed_rows)
  
   # Drop the 'Email2' column (if it exists)
   if email2_col:
       intermediate_df = intermediate_df.drop(columns=[email2_col], errors='ignore')


   # Save the intermediate output
   intermediate_df.to_csv(output_file_path, index=False)
   print(f"Intermediate output saved at: {output_file_path}")
   return intermediate_df


def create_final_csv(intermediate_file_path, secondary_file_path, output_file_path, column_mapping=None):
   # Load the intermediate and secondary files
   intermediate_df = pd.read_csv(intermediate_file_path, dtype=object)
   secondary_df = load_file(secondary_file_path)
  
   # Standardize column names to lowercase for easier matching
   intermediate_df.columns = intermediate_df.columns.str.strip().str.lower()
   secondary_df.columns = secondary_df.columns.str.strip().str.lower()
  
   # Apply column mapping to the secondary file if provided
   if column_mapping:
       secondary_df = secondary_df.rename(columns=column_mapping)


   # Filter out rows from secondary_df without an email value
   # Comment out line 103 if you want to append ALL rows that DO NOT have a matching VIN & Email from secondary file
   secondary_df = secondary_df[secondary_df["email"].notna()]
  
   # Identify unmatched rows in the secondary file
   unmatched_secondary = secondary_df[~(
       (secondary_df['vin'].isin(intermediate_df['vin'])) &
       (secondary_df['email'].isin(intermediate_df['email']))
   )]
  
   # Map data from secondary file to intermediate structure
   mapped_secondary = unmatched_secondary.reindex(columns=intermediate_df.columns)
  
   # Combine the intermediate data and the mapped secondary data
   final_df = pd.concat([intermediate_df, mapped_secondary], ignore_index=True)
  
   # Save the final output
   final_df.to_csv(output_file_path, index=False)
   print(f"Final output saved at: {output_file_path}")


   # Space out terminal prompts
   print('\n')


   return final_df


def process_files(main_folder_path, secondary_folder_path, intermediate_output_folder, final_output_folder, column_mapping=None):
   """
   Process files from two input folders in order and handle mismatched file counts.


   Arguments:
   - main_folder_path: Path to the folder containing main input files.
   - secondary_folder_path: Path to the folder containing secondary input files.
   - intermediate_output_folder: Base folder for intermediate output files.
   - final_output_folder: Base folder for final output files.
   - column_mapping: Optional mapping of column names between files.


   Steps:
   1. List and sort all files in `main_folder_path` and `secondary_folder_path`.
   2. Check if the number of files in the two folders is equal:
      - If not, prompt the user to confirm reusing the last file from the smaller folder.
      - If the user declines, abort the process.
   3. Match files in order from both folders:
      - Extend the smaller folder's file list by repeating its last file as needed.
   4. For each pair of files (main and secondary):
      - Display their names and prompt the user to confirm.
      - If confirmed, ask for a base name for the output files.
      - Generate paths for the intermediate and final output files using the base name.
      - Call `create_intermediate_csv` to process the main file and create an intermediate file.
      - Call `create_final_csv` to combine the intermediate file with the secondary file and create a final file.
   """
   # Get sorted lists of files in the main and secondary folders.
   # Excludes hidden/system files (e.g., .DS_Store)
   main_files = [f for f in sorted(os.listdir(main_folder_path)) if not f.startswith(".")]
   secondary_files = [f for f in sorted(os.listdir(secondary_folder_path)) if not f.startswith(".")]


   # Check if the number of files in both folders is the same.
   if len(main_files) != len(secondary_files):
       print("The number of files in the input folders is different.")
       small_folder = ''


       if len(main_files) < len(secondary_files):
           small_folder = 'Main'
       else:
           small_folder = 'Secondary'


       user_decision = input(
           f"The {small_folder} folder will reuse its last file. Proceed? (y/n): "
       ).strip().lower()


       if user_decision != "y":
           print("Aborting process.")
           return
      
       # Space out terminal prompts
       print('\n')




   # Determine the maximum number of files to process.
   max_files = max(len(main_files), len(secondary_files))


   # Extend the smaller folder's file list by repeating its last file.
   main_files.extend([main_files[-1]] * (max_files - len(main_files)))
   secondary_files.extend([secondary_files[-1]] * (max_files - len(secondary_files)))


   # Loop through each pair of files from the two folders.
   for i, (main_file, secondary_file) in enumerate(zip(main_files, secondary_files)):
       main_file_path = os.path.join(main_folder_path, main_file)  # Full path for the main file.
       secondary_file_path = os.path.join(secondary_folder_path, secondary_file)  # Full path for the secondary file.


       # Display the names of the files to the user for confirmation.
       print(f"Main File: {main_file}")
       print(f"Secondary File: {secondary_file}")


       # Prompt the user to confirm the file pair.
       user_confirmation = input("Are these files correct? (y/n): ").strip().lower()
       if user_confirmation != "y":
           print("Aborting process.")
           return


       # Prompt the user to provide a base name for the output files.
       base_name = input("Enter a base name for output files: ").strip()
       intermediate_output_file = os.path.join(intermediate_output_folder, f"{base_name}-intermediate.csv")
       final_output_file = os.path.join(final_output_folder, f"{base_name}-final.csv")


       # Process the main file to create an intermediate output.
       create_intermediate_csv(main_file_path, intermediate_output_file)


       # Combine the intermediate file with the secondary file to create the final output.
       create_final_csv(intermediate_output_file, secondary_file_path, final_output_file, column_mapping)
          
# File paths
main_folder_path = "/input/main"
secondary_folder_path = "/input/secondary"
intermediate_output_folder = "/input/intermediate"
final_output_folder = "/output"


# Column mapping for secondary file (optional)
column_mapping = {
   'postal address': 'address',
   'email address': 'email',
   'model year': 'year',
   'name': 'first name',
   'customer name': 'first name'
}


process_files(main_folder_path, secondary_folder_path, intermediate_output_folder, final_output_folder, column_mapping)


















"""
   Uncomment to run process for each file manually.
   In order to use different files change the file name at the end of the path string.
"""


# File paths
# main_file_path = '/input/main/CBT-All.csv'                                             # Main file path
# secondary_file_path = '/input/secondary/CBT-TLE-TARGET-12-2.csv'                       # Secondary file path
# intermediate_output_path = '/input/intermediate/intermediate_output.csv'               # Desired intermediate output path
# final_output_path = '/output/compiled-unique-emails-CBT-TLE-TARGET-12-2.csv'           # Desired final output path


# Column mapping for secondary file (optional)
# Default configuration for test files
# column_mapping = {
   # 'firstname': 'first',
   # 'last name': 'last',
   # 'address': 'street'
# }


# Part 1: Create Corrected Intermediate Output
# create_intermediate_csv(main_file_path, intermediate_output_path)


# Part 2: Create Corrected Final Output with column mapping
# create_final_csv(intermediate_output_path, secondary_file_path, final_output_path, column_mapping=column_mapping)