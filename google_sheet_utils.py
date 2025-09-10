import gspread_asyncio
from google.oauth2.service_account import Credentials
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import traceback


SCOPE = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file']

def get_creds():
    """Gets the Google credentials from the service account file."""
    google_service_account_file = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE')
    if not google_service_account_file:
        print("DEBUG_GSU_ERROR: GOOGLE_SERVICE_ACCOUNT_FILE not set in .env")
        return None
    
    return Credentials.from_service_account_file(google_service_account_file, scopes=SCOPE)

async def get_sheet():
    """Asynchronously authenticates with Google Sheets and returns the specific worksheet."""
    try:
        google_sheet_name = os.getenv('GOOGLE_SHEET_NAME')
        google_sheet_id = os.getenv('GOOGLE_SPREADSHEET_ID')
        google_sheet_worksheet_name = os.getenv('GOOGLE_SHEET_WORKSHEET_NAME')

        if not google_sheet_worksheet_name:
            print(f"DEBUG_GSU_ERROR: GOOGLE_SHEET_WORKSHEET_NAME ('{google_sheet_worksheet_name}') not set in .env or is invalid.")
            return None
        if not google_sheet_id and not google_sheet_name:
            print("DEBUG_GSU_ERROR: Neither GOOGLE_SPREADSHEET_ID nor GOOGLE_SHEET_NAME is set in .env")
            return None

        agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)
        client = await agcm.authorize()

        spreadsheet = None
        if google_sheet_id:
            try:
                # print(f"DEBUG_GSU: Attempting to open spreadsheet by ID: {google_sheet_id}")
                spreadsheet = await client.open_by_key(google_sheet_id)
                # print(f"DEBUG_GSU: Successfully opened spreadsheet by ID. Title: '{spreadsheet.title}'")
                # commented out print statements to reduce noise in logs
            except gspread_asyncio.gspread.exceptions.APIError as e:
                print(f"DEBUG_GSU_ERROR: API error opening spreadsheet by ID '{google_sheet_id}': {e}. Falling back to name if available.")
                if not google_sheet_name:
                    return None
            except Exception:
                print(f"DEBUG_GSU_ERROR: Error opening spreadsheet by ID '{google_sheet_id}'. Falling back to name if available.")
                if not google_sheet_name:
                    return None
        
        if not spreadsheet and google_sheet_name:
            try:
                print(f"DEBUG_GSU: Attempting to open spreadsheet by name: {google_sheet_name}")
                spreadsheet = await client.open(google_sheet_name)
                print(f"DEBUG_GSU: Successfully opened spreadsheet by name. Title: '{spreadsheet.title}'")
            except gspread_asyncio.gspread.exceptions.SpreadsheetNotFound:
                print(f"DEBUG_GSU_ERROR: Spreadsheet named '{google_sheet_name}' not found.")
                return None
            except Exception as e:
                print(f"DEBUG_GSU_ERROR: Error opening spreadsheet by name '{google_sheet_name}': {e}")
                return None
        
        if not spreadsheet:
            print("DEBUG_GSU_ERROR: Could not open spreadsheet by ID or name.")
            return None

        try:
            sheet = await spreadsheet.worksheet(google_sheet_worksheet_name)
            print(f"DEBUG_GSU: Successfully opened worksheet: '{sheet.title}'")
            return sheet
        except gspread_asyncio.gspread.exceptions.WorksheetNotFound:
            print(f"DEBUG_GSU_ERROR: Worksheet named '{google_sheet_worksheet_name}' not found in spreadsheet '{spreadsheet.title}'.")
            return None

    except FileNotFoundError:
        print(f"DEBUG_GSU_ERROR: Service account JSON file not found at path: {os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE')}")
        return None
    except Exception:
        print("DEBUG_GSU_ERROR: An unexpected error occurred in get_sheet:")
        traceback.print_exc()
        return None

async def get_all_sales_data(sheet):
    """Fetches all records from the sheet using get_all_records for dictionary format."""
    if not sheet:
        print("DEBUG_GSU: get_all_sales_data received no sheet object.")
        return []
    try:
        records = await sheet.get_all_records() 
        print(f"DEBUG_GSU: Fetched {len(records)} records using get_all_records().")
        if records:
            print(f"DEBUG_GSU_DETAIL: Example of first record fetched: {records[0]}")
        return records
    except Exception as e:
        print(f"DEBUG_GSU_ERROR: Error in get_all_sales_data: {e}")
        traceback.print_exc()
        return []

async def get_weekly_leaderboard_data(sheet):
    """
    Fetches and processes sales data for the current week's leaderboard.
    Fills remaining slots with salespeople who have had activity in the last two weeks.
    """
    timestamp_column = os.getenv("TIMESTAMP_COLUMN")
    first_name_column = os.getenv("FIRST_NAME_COLUMN")
    premium_column = os.getenv("PREMIUM_COLUMN")

    if not all([timestamp_column, first_name_column, premium_column]):
        print("DEBUG_GSU_ERROR: One or more column names (TIMESTAMP_COLUMN, FIRST_NAME_COLUMN, PREMIUM_COLUMN) not set in .env")
        return {}

    all_sales = await get_all_sales_data(sheet)
    if not all_sales:
        print("DEBUG_GSU: No sales data returned from get_all_sales_data for leaderboard.")
        return {}

    leaderboard = {}
    recently_active_names = set()

    today = datetime.now(ZoneInfo("America/New_York"))
    print(today)
    start_of_week = today - timedelta(days=today.weekday())
    start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    print(start_of_week)
    end_of_week = start_of_week + timedelta(days=6, hours=23, minutes=59, seconds=59)
    print(end_of_week)
    
    two_weeks_ago = today - timedelta(days=14)

    print(f"DEBUG_GSU: Calculating leaderboard for week: {start_of_week.strftime('%Y-%m-%d')} to {end_of_week.strftime('%Y-%m-%d')}")
    print(f"DEBUG_GSU: Checking for recent activity since: {two_weeks_ago.strftime('%Y-%m-%d')}")

    for i, sale_record in enumerate(all_sales):
        sale_date = None
        try:
            timestamp_value = sale_record.get(timestamp_column)
            first_name = sale_record.get(first_name_column)
            premium_raw = sale_record.get(premium_column, "0")

            if not timestamp_value or not first_name:
                continue

            ts_to_parse = str(timestamp_value).strip()
            common_formats = [
                '%Y-%m-%d %H:%M:%S', '%m/%d/%Y %I:%M:%S %p', '%m/%d/%Y %H:%M', 
                '%Y-%m-%d', '%m/%d/%Y'
            ]
            for fmt in common_formats:
                try:
                    sale_date = datetime.strptime(ts_to_parse, fmt)
                    break 
                except ValueError:
                    continue
            
            if sale_date is None:
                print(f"DEBUG_GSU_WARNING: Row {i+1}: COULD NOT PARSE timestamp '{ts_to_parse}'. Skipping.")
                continue

            salesperson_name = str(first_name)

            if sale_date >= two_weeks_ago:
                recently_active_names.add(salesperson_name)

            if start_of_week <= sale_date <= end_of_week:
                premium_str = str(premium_raw).replace('$', '').replace(',', '')
                try:
                    premium_value = float(premium_str) if premium_str else 0.0
                except ValueError:
                    print(f"DEBUG_GSU_WARNING: Could not convert premium '{premium_str}' to float for {salesperson_name}. Using 0.0.")
                    premium_value = 0.0
                
                if salesperson_name not in leaderboard:
                    leaderboard[salesperson_name] = {"premium": 0.0, "apps": 0}
                
                leaderboard[salesperson_name]["premium"] += premium_value
                leaderboard[salesperson_name]["apps"] += 1

        except Exception as ex:
            print(f"DEBUG_GSU_ERROR: Unexpected error processing sale record #{i+1}: {ex}")
            traceback.print_exc()

    print(f"DEBUG_GSU: Found {len(leaderboard)} people with sales this week.")
    print(f"DEBUG_GSU: Found {len(recently_active_names)} people with sales in the last two weeks.")
    
    leaderboard_names = set(leaderboard.keys())
    filler_candidates = [name for name in recently_active_names if name not in leaderboard_names]
    
    for name in filler_candidates:
        if len(leaderboard) > 20:
            break
        leaderboard[name] = {"premium": 0.0, "apps": 0}

    sorted_leaderboard = dict(sorted(leaderboard.items(), key=lambda item: item[1]['premium'], reverse=True))
    sorted_leaderboard = dict(list(sorted_leaderboard.items())[:20])
    
    print(f"DEBUG_GSU: Final leaderboard data after filling and sorting: {sorted_leaderboard}")
    return sorted_leaderboard