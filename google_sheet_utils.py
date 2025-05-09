# google_sheet_utils.py
import gspread
from google.oauth2.service_account import Credentials
import os
from datetime import datetime, timedelta
import traceback # For detailed error logging

SCOPE = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file']

def get_sheet():
    """Authenticates with Google Sheets and returns the specific worksheet."""
    try:
        google_service_account_file = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE')
        google_sheet_name = os.getenv('GOOGLE_SHEET_NAME')
        google_sheet_id = os.getenv('GOOGLE_SPREADSHEET_ID') # For opening by ID
        google_sheet_worksheet_name = os.getenv('GOOGLE_SHEET_WORKSHEET_NAME')

        if not google_service_account_file:
            print("DEBUG_GSU_ERROR: GOOGLE_SERVICE_ACCOUNT_FILE not set in .env")
            return None
        if not google_sheet_worksheet_name:
            print(f"DEBUG_GSU_ERROR: GOOGLE_SHEET_WORKSHEET_NAME ('{google_sheet_worksheet_name}') not set in .env or is invalid.")
            return None
        if not google_sheet_id and not google_sheet_name:
            print("DEBUG_GSU_ERROR: Neither GOOGLE_SPREADSHEET_ID nor GOOGLE_SHEET_NAME is set in .env")
            return None

        creds = Credentials.from_service_account_file(google_service_account_file, scopes=SCOPE)
        client = gspread.authorize(creds)

        spreadsheet = None
        if google_sheet_id:
            try:
                print(f"DEBUG_GSU: Attempting to open spreadsheet by ID: {google_sheet_id}")
                spreadsheet = client.open_by_key(google_sheet_id)
                print(f"DEBUG_GSU: Successfully opened spreadsheet by ID. Title: '{spreadsheet.title}'")
            except gspread.exceptions.APIError as e:
                print(f"DEBUG_GSU_ERROR: API error opening spreadsheet by ID '{google_sheet_id}': {e}. Falling back to name if available.")
                if not google_sheet_name:
                    return None
            except Exception as e:
                print(f"DEBUG_GSU_ERROR: Error opening spreadsheet by ID '{google_sheet_id}': {e}. Falling back to name if available.")
                if not google_sheet_name:
                    return None
        
        if not spreadsheet and google_sheet_name:
            try:
                print(f"DEBUG_GSU: Attempting to open spreadsheet by name: {google_sheet_name}")
                spreadsheet = client.open(google_sheet_name)
                print(f"DEBUG_GSU: Successfully opened spreadsheet by name. Title: '{spreadsheet.title}'")
            except gspread.exceptions.SpreadsheetNotFound:
                print(f"DEBUG_GSU_ERROR: Spreadsheet named '{google_sheet_name}' not found.")
                return None
            except Exception as e:
                print(f"DEBUG_GSU_ERROR: Error opening spreadsheet by name '{google_sheet_name}': {e}")
                return None
        
        if not spreadsheet:
            print("DEBUG_GSU_ERROR: Could not open spreadsheet by ID or name.")
            return None

        try:
            sheet = spreadsheet.worksheet(google_sheet_worksheet_name)
            print(f"DEBUG_GSU: Successfully opened worksheet: '{sheet.title}'")
            return sheet
        except gspread.exceptions.WorksheetNotFound:
            print(f"DEBUG_GSU_ERROR: Worksheet named '{google_sheet_worksheet_name}' not found in spreadsheet '{spreadsheet.title}'.")
            return None

    except FileNotFoundError:
        print(f"DEBUG_GSU_ERROR: Service account JSON file not found at path: {os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE')}")
        return None
    except Exception as e:
        print("DEBUG_GSU_ERROR: An unexpected error occurred in get_sheet:")
        traceback.print_exc()
        return None

def get_all_sales_data(sheet):
    """Fetches all records from the sheet using get_all_records for dictionary format."""
    if not sheet:
        print("DEBUG_GSU: get_all_sales_data received no sheet object.")
        return []
    try:
        records = sheet.get_all_records() 
        print(f"DEBUG_GSU: Fetched {len(records)} records using get_all_records().")
        if records:
            print(f"DEBUG_GSU_DETAIL: Example of first record fetched: {records[0]}")
        return records
    except Exception as e:
        print(f"DEBUG_GSU_ERROR: Error in get_all_sales_data: {e}")
        traceback.print_exc()
        return []

def get_weekly_leaderboard_data(sheet):
    """Fetches and processes sales data for the current week's leaderboard."""
    timestamp_column = os.getenv("TIMESTAMP_COLUMN")
    first_name_column = os.getenv("FIRST_NAME_COLUMN")
    premium_column = os.getenv("PREMIUM_COLUMN")

    if not all([timestamp_column, first_name_column, premium_column]):
        print("DEBUG_GSU_ERROR: One or more column names (TIMESTAMP_COLUMN, FIRST_NAME_COLUMN, PREMIUM_COLUMN) not set in .env")
        return {}

    all_sales = get_all_sales_data(sheet)
    if not all_sales:
        print("DEBUG_GSU: No sales data returned from get_all_sales_data for leaderboard.")
        return {}

    leaderboard = {}
    today = datetime.now()
    start_of_week = today - timedelta(days=today.weekday())
    start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_week = start_of_week + timedelta(days=6, hours=23, minutes=59, seconds=59)
    
    print(f"DEBUG_GSU: Calculating leaderboard for week: {start_of_week.strftime('%Y-%m-%d %H:%M:%S')} to {end_of_week.strftime('%Y-%m-%d %H:%M:%S')}")

    sales_in_week_count = 0
    for i, sale_record in enumerate(all_sales):
        sale_date = None # Reset for each record
        try:
            timestamp_value = sale_record.get(timestamp_column) # This is the raw value from sheet
            print(f"DEBUG_GSU_DETAIL: Row {i+1}: Raw timestamp_value from sheet for column '{timestamp_column}': '{timestamp_value}', Type: {type(timestamp_value)}")
            
            first_name = sale_record.get(first_name_column)
            
            premium_raw = sale_record.get(premium_column)
            if isinstance(premium_raw, (int, float)):
                premium_str = str(premium_raw)
            elif isinstance(premium_raw, str):
                premium_str = premium_raw.replace('$', '').replace(',', '')
            else: 
                premium_str = "0"

            if timestamp_value is None or timestamp_value == '': # Check if timestamp_value is empty or None
                print(f"DEBUG_GSU_DETAIL: Row {i+1} skipped - timestamp_value is None or empty for column '{timestamp_column}'. Record: {sale_record}")
                continue
            if not first_name:
                # print(f"DEBUG_GSU_DETAIL: Row {i+1} skipped - missing name for column '{first_name_column}'. Record: {sale_record}")
                continue

            ts_to_parse = str(timestamp_value).strip() # Ensure it's a string for strptime
            print(f"DEBUG_GSU_DETAIL: Row {i+1}: Attempting to parse ts_to_parse: '{ts_to_parse}' (original type: {type(timestamp_value)})")

            common_formats = [
                '%Y-%m-%d %H:%M:%S',    # Primary expected format
                '%Y-%m-%dT%H:%M:%S%z', 
                '%m/%d/%Y %I:%M:%S %p', 
                '%m/%d/%Y %H:%M',       
                '%Y-%m-%d',             
                '%m/%d/%Y'              
            ]
            
            # Clean timezone for strptime if necessary
            if '+' in ts_to_parse and not any('%z' in fmt for fmt in common_formats if fmt == '%Y-%m-%dT%H:%M:%S%z'): # Avoid double handling
                 ts_to_parse_cleaned = ts_to_parse.split('+')[0].strip()
            elif ts_to_parse.upper().endswith('Z') and not any('%z' in fmt for fmt in common_formats if fmt == '%Y-%m-%dT%H:%M:%S%z'):
                 ts_to_parse_cleaned = ts_to_parse[:-1].strip()
            else:
                 ts_to_parse_cleaned = ts_to_parse

            for fmt in common_formats:
                try:
                    sale_date = datetime.strptime(ts_to_parse_cleaned, fmt)
                    print(f"DEBUG_GSU_DETAIL: Row {i+1}: SUCCESS - Parsed '{ts_to_parse_cleaned}' with format '{fmt}' -> {sale_date}")
                    break 
                except ValueError:
                    # print(f"DEBUG_GSU_DETAIL: Row {i+1}: FAILED - Parsing '{ts_to_parse_cleaned}' with format '{fmt}'") # Verbose
                    continue
            
            if sale_date is None:
                print(f"DEBUG_GSU_WARNING: Row {i+1}: COULD NOT PARSE timestamp. Original value: '{timestamp_value}', String for parsing: '{ts_to_parse_cleaned}'. Raw record: {sale_record}. Skipping.")
                continue

            print(f"DEBUG_GSU_DETAIL: Row {i+1}: Comparing sale_date: {sale_date} (Type: {type(sale_date)}) with start_of_week: {start_of_week} and end_of_week: {end_of_week}")
            if start_of_week <= sale_date <= end_of_week:
                sales_in_week_count += 1
                print(f"DEBUG_GSU_DETAIL: Row {i+1}: Sale from {first_name} on {sale_date.strftime('%Y-%m-%d')} IS IN CURRENT WEEK.")
                try:
                    premium_value = float(premium_str) if premium_str else 0.0
                except ValueError:
                    print(f"DEBUG_GSU_WARNING: Could not convert premium '{premium_str}' to float for {first_name}. Using 0.0. Record: {sale_record}")
                    premium_value = 0.0
                leaderboard[str(first_name)] = leaderboard.get(str(first_name), 0.0) + premium_value
            else:
                print(f"DEBUG_GSU_DETAIL: Row {i+1}: Sale from {first_name} on {sale_date.strftime('%Y-%m-%d')} is NOT in current week (Start: {start_of_week}, End: {end_of_week}, Sale Date: {sale_date}).")

        except Exception as ex:
            print(f"DEBUG_GSU_ERROR: Unexpected error processing sale record #{i+1}: {ex}")
            print(f"Problematic sale_record: {sale_record}")
            traceback.print_exc()
            
    print(f"DEBUG_GSU: Processed {len(all_sales)} sales. Found {sales_in_week_count} sales in the current week.")
    print(f"DEBUG_GSU: Final leaderboard data before sorting: {leaderboard}")
    
    if not leaderboard and len(all_sales) > 0 and sales_in_week_count == 0 : # Added more conditions for this message
        print("DEBUG_GSU_INFO: Leaderboard is empty because no sales from the sheet fell into the current week's date range after parsing.")
    elif not leaderboard:
         print("DEBUG_GSU_INFO: Leaderboard dictionary is empty after processing all sales (either no sales at all or none qualified).")


    sorted_leaderboard = dict(sorted(leaderboard.items(), key=lambda item: item[1], reverse=True))
    return sorted_leaderboard
