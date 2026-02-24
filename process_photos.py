import os
import csv
import subprocess
import argparse
import sys
from pathlib import Path

def process_image_with_gemini(image_path):
    """
    Invokes the gemini CLI to extract student names from the given image.
    """
    prompt = f"Extract the student names from this image in CSV format (Header: Name). Only output the CSV, nothing else. Do not use blockquotes. Image: {image_path}"
    
    # Run the gemini CLI
    try:
        result = subprocess.run(
            ["gemini", "-p", prompt],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error processing {image_path}: {e}", file=sys.stderr)
        print(f"Stderr: {e.stderr}", file=sys.stderr)
        return None

def clean_csv_output(raw_output):
    """
    Cleans up the output from Gemini CLI to ensure it only contains CSV data.
    Strips markdown blockquotes and unwanted prefix lines.
    """
    lines = raw_output.strip().split('\n')
    cleaned_lines = []
    
    in_csv_block = False
    
    for line in lines:
        line = line.strip()
        # Handle cases where output might have 'Loaded cached credentials.' or other CLI logs
        if 'Loaded cached credentials' in line:
            continue
            
        if line.startswith('```'):
            in_csv_block = not in_csv_block
            continue
            
        if line:
            cleaned_lines.append(line)
            
    # Sometimes it doesn't output code blocks, just raw CSV. 
    # Let's ensure 'Name' header is the start if it exists, otherwise just return cleaned lines
    
    # Find where the actual data starts (header 'Name')
    start_idx = 0
    for i, line in enumerate(cleaned_lines):
        if line.lower() == 'name' or line.lower() == 'name,':
            start_idx = i
            break
            
    return cleaned_lines[start_idx:]

def main():
    parser = argparse.ArgumentParser(description="Process student list photos into a single CSV")
    parser.add_argument("input_dir", help="Directory containing the student photos")
    parser.add_argument("output_csv", help="Path to the output consolidated CSV file")
    args = parser.parse_args()
    
    input_path = Path(args.input_dir)
    if not input_path.is_dir():
        print(f"Error: {args.input_dir} is not a valid directory.", file=sys.stderr)
        sys.exit(1)
        
    csv_data = []
    headers = ["Name", "Folder", "File"]
    
    image_extensions = {'.jpg', '.jpeg', '.png'}
    
    # Recursively find images
    for img_file in input_path.rglob('*'):
        if img_file.is_file() and img_file.suffix.lower() in image_extensions:
            print(f"Processing: {img_file}")
            
            # Extract folder name
            folder_name = img_file.parent.name
            
            # Call Gemini
            raw_output = process_image_with_gemini(str(img_file))
            if raw_output:
                cleaned_lines = clean_csv_output(raw_output)
                
                # Skip header if it exists in output
                if cleaned_lines and cleaned_lines[0].lower().startswith('name'):
                    cleaned_lines = cleaned_lines[1:]
                
                # Append to our data structure
                for line in cleaned_lines:
                    # Clean up any potential leftover quotes
                    name = line.replace('"', '').replace(',', '').strip()
                    if name:
                        csv_data.append([name, folder_name, img_file.name])
                        
    # Write consolidated CSV
    with open(args.output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(csv_data)
        
    print(f"Successfully wrote {len(csv_data)} records to {args.output_csv}")

if __name__ == "__main__":
    main()
