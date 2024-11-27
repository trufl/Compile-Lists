import pandas as pd

def create_intermediate_csv(main_file_path, output_file_path):
   # Load the main file
   main_df = pd.read_csv(main_file_path, dtype=object)
  
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
   secondary_df = pd.read_csv(secondary_file_path, dtype=object)
  
   # Standardize column names to lowercase for easier matching
   intermediate_df.columns = intermediate_df.columns.str.strip().str.lower()
   secondary_df.columns = secondary_df.columns.str.strip().str.lower()
  
   # Apply column mapping to the secondary file if provided
   if column_mapping:
       secondary_df = secondary_df.rename(columns=column_mapping)
  
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
   return final_df

# File paths
main_file_path = '/Users/tristansaragosa/Desktop/TempPythonScript/input/listTwo/example-file-two.csv'  # Replace with actual path
secondary_file_path = '/Users/tristansaragosa/Desktop/TempPythonScript/input/listOne/example-file-one.csv'  # Replace with actual path
corrected_intermediate_output_path = '/Users/tristansaragosa/Desktop/TempPythonScript/input/intermediate/intermediate_output.csv'  # Desired intermediate output path
corrected_final_output_path = '/Users/tristansaragosa/Desktop/TempPythonScript/output/compiled-unique-emails.csv'  # Desired final output path

# Column mapping for secondary file (optional)
column_mapping = {
   'firstname': 'first',
   'last name': 'last',
   'address': 'street',
}

# Part 1: Create Corrected Intermediate Output
create_intermediate_csv(main_file_path, corrected_intermediate_output_path)

# Part 2: Create Corrected Final Output with column mapping
create_final_csv(corrected_intermediate_output_path, secondary_file_path, corrected_final_output_path, column_mapping=column_mapping)