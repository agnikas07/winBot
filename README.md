You will need your Google Service Account JSON in the same directory that the rest of the bot runs in.

Additionally, you will need a .env file with the following values:

DISCORD_BOT_TOKEN = [your bot token]\n
GOOGLE_SHEET_NAME = [your Google Sheet name]\n
GOOGLE_SPREADSHEET_ID = [your Google Spreadsheet ID]\n
GOOGLE_SHEET_WORKSHEET_NAME = [your Google Sheets Worksheet Name]\n
GOOGLE_SERVICE_ACCOUNT_FILE = [your filepath for your Google Service Account file]\n

NOTIFICATION_CHANNEL_ID = [your Discord channel ID for posting new sale notifications]
COMMAND_CHANNEL_ID = [your Discord channel ID for commands]
AUTOMATED_LEADERBOARD_CHANNEL_ID = [your Discord channel ID for posting automated leaderboard updates]

FIRST_NAME_COLUMN = "Name"
SALE_TYPE_COLUMN = "Sale Type"
PREMIUM_COLUMN = "Premium"
TIMESTAMP_COLUMN = "Date"
#Replace with your actual Google Sheets column names
