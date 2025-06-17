# main.py
from datetime import datetime as dt, time, timedelta
import discord
from discord.ext import commands, tasks
import gspread
from dotenv import load_dotenv
import google_sheet_utils as gsu
import asyncio
import os
import traceback

load_dotenv()

# --- Bot Setup ---
intents = discord.Intents.default()
intents.message_content = True
intents.messages = True
bot = commands.Bot(command_prefix="!", intents=intents)

# --- Global state for polling ---
last_known_row_count_g = 1
initial_check_done = False

# --- Helper to initialize last_known_row_count ---
async def initialize_row_count():
    global last_known_row_count_g, initial_check_done
    sheet = gsu.get_sheet()
    if sheet:
        try:
            all_values = sheet.get_all_values()
            last_known_row_count_g = len(all_values)
            print(f"Initial row count set to: {last_known_row_count_g}")
        except Exception as e:
            print(f"Error initializing row count: {e}")
            last_known_row_count_g = 1
    else:
        print("Sheet not available during initial row count check.")
    initial_check_done = True

# --- Reusable Leaderboard Function ---
async def generate_and_post_leaderboard(destination: discord.abc.Messageable):
    """
    Fetches, formats, and posts the weekly sales leaderboard to the given destination.
    'destination' can be a TextChannel or a commands.Context object.
    """
    sheet = gsu.get_sheet()
    if not sheet:
        try:
            await destination.send("Sorry, I couldn't connect to the sales data sheet right now for the leaderboard. Please try again later.")
        except discord.errors.Forbidden:
            print(f"Error: Bot does not have permission to send messages in {destination}")
        except Exception as e:
            print(f"Error sending sheet connection error message: {e}")
        return

    if isinstance(destination, commands.Context):
        await destination.send("Generating weekly leaderboard... 📊", delete_after=15)

    try:
        leaderboard_data = gsu.get_weekly_leaderboard_data(sheet)

        if not leaderboard_data:
            await destination.send("No sales recorded yet this week.")
            return

        today = dt.now()
        start_of_week = today - timedelta(days=today.weekday())
        end_of_week = start_of_week + timedelta(days=6)

        team_total = sum(
            v for v in leaderboard_data.values() if isinstance(v, (int, float))
        )

        embed = discord.Embed(
            title="🏆 Weekly Sales Leaderboard 🏆",
            description=f"Sales from {start_of_week.strftime('%b %d, %Y')} to {end_of_week.strftime('%b %d, %Y')}",
            color=discord.Color.gold()
        )
        embed.set_footer(text=f"Total Production: ${team_total:,.2f}\nLast updated: {dt.now().strftime('%Y-%m-%d %I:%M %p %Z')}")

        position = 1
        for name, total_premium in leaderboard_data.items():
            if position > 10:
                break
            
            if position == 1:
                prefix = "🥇"
            elif position == 2:
                prefix = "🥈"
            elif position == 3:
                prefix = "🥉"
            else:
                prefix = f"#{position}."

            formatted_premium = f"${total_premium:,.2f}" if isinstance(total_premium, (int, float)) else str(total_premium)
            embed.add_field(name=f"{prefix} {name}", value=f"Total Premium: **{formatted_premium}**", inline=False)
            position += 1

        if not embed.fields:
            await destination.send("No sales data found for the current week to display on the leaderboard.")
            return

        await destination.send(embed=embed)

    except gspread.exceptions.APIError as e:
        await destination.send("There was an API error trying to fetch leaderboard data from Google Sheets. Please try again later.")
        print(f"Google Sheets API Error during leaderboard generation: {e}")
    except discord.errors.Forbidden:
        print(f"Error: Bot does not have permission to send leaderboard message in {destination}")
    except Exception as e:
        await destination.send("An unexpected error occurred while generating the leaderboard.")
        print(f"Error in generate_and_post_leaderboard: {e}")
        traceback.print_exc()

# --- Event: Bot Ready ---
@bot.event
async def on_ready():
    print(f'{bot.user.name} has connected to Discord!')
    print(f"Bot ID: {bot.user.id}")
    await initialize_row_count()
    if not check_for_new_sales.is_running():
        check_for_new_sales.start()
    if not automated_leaderboard_poster.is_running():
        automated_leaderboard_poster.start()

# --- Task: Check for New Sales (Polling) ---
@tasks.loop(seconds=60)
async def check_for_new_sales():
    global last_known_row_count_g, initial_check_done

    custom_alarm_emoji = os.getenv("ALARM_EMOJI_TAG", "<a:AlarmreminderUrgence:1370133606856392816>") 
    custom_gsd_emoji = os.getenv("GSD_EMOJI_TAG", "<:GSD:1369689499592036364>") 


    if not initial_check_done:
        print("Waiting for initial row count check to complete...")
        return

    sheet = gsu.get_sheet()
    if not sheet:
        print("Sheet not available for polling new sales.")
        return

    try:
        current_total_rows = len(sheet.get_all_values())

        if current_total_rows > last_known_row_count_g:
            print(f"Change detected! Old rows: {last_known_row_count_g}, New rows: {current_total_rows}")
            all_values_from_sheet = sheet.get_all_values()
            headers = all_values_from_sheet[0] if len(all_values_from_sheet) > 0 else []
            new_sales_data = []

            for i in range(last_known_row_count_g, current_total_rows):
                if i < len(all_values_from_sheet):
                    row_values = all_values_from_sheet[i]
                    sale_data = {}
                    for col_idx, header in enumerate(headers):
                        if col_idx < len(row_values):
                            sale_data[header] = row_values[col_idx]
                        else:
                            sale_data[header] = None
                    new_sales_data.append(sale_data)

            notification_channel_id_str = os.getenv("NOTIFICATION_CHANNEL_ID")
            first_name_column = os.getenv("FIRST_NAME_COLUMN", "Name") 
            sale_type_column = os.getenv("SALE_TYPE_COLUMN", "Sale Type")   
            premium_column = os.getenv("PREMIUM_COLUMN", "Premium") 

            if not notification_channel_id_str:
                print("Error: NOTIFICATION_CHANNEL_ID is not set in .env")
                last_known_row_count_g = current_total_rows 
                return
            
            try:
                notification_channel_id = int(notification_channel_id_str)
            except ValueError:
                print(f"Error: NOTIFICATION_CHANNEL_ID '{notification_channel_id_str}' is not a valid integer.")
                last_known_row_count_g = current_total_rows 
                return

            notification_channel = bot.get_channel(notification_channel_id)
            if not notification_channel:
                print(f"Error: Notification channel ID {notification_channel_id} not found.")
                last_known_row_count_g = current_total_rows
                return

            for sale in new_sales_data:
                first_name = sale.get(first_name_column, "N/A")
                sale_type = sale.get(sale_type_column, "N/A")
                premium = sale.get(premium_column, "N/A")

                if first_name != "N/A":
                    message = f"{custom_alarm_emoji} **New Sale!** {custom_alarm_emoji}\n\n{first_name} just made a sale!\n**Sale Type:** {sale_type}\n**Annual Premium:** ${premium}\n\n{custom_gsd_emoji}"
                    await notification_channel.send(message)
                else:
                    print(f"Skipping notification for incomplete sale data: {sale}")

            last_known_row_count_g = current_total_rows


    except gspread.exceptions.APIError as e:
        print(f"Google Sheets API error during polling: {e}")
        if hasattr(e, 'response') and e.response.status_code == 429:
            print("Rate limit hit. Pausing polling for a bit.")
            check_for_new_sales.change_interval(seconds=300)
            await asyncio.sleep(10)
            check_for_new_sales.change_interval(seconds=60)
    except Exception as e:
        print(f"An error occurred in check_for_new_sales: {e}")
        traceback.print_exc()

# --- Command: Weekly Leaderboard ---
@bot.command(name='leaderboard', help='Displays the weekly sales leaderboard.')
async def leaderboard_command(ctx): 
    await generate_and_post_leaderboard(ctx)

@bot.event
async def on_message(message):
    if message.author.bot:
        await bot.process_commands(message)
    else:
        await bot.process_commands(message)

# --- Task: Automated Weekly Leaderboard Post ---
@tasks.loop(time=time(23, 0))
async def automated_leaderboard_poster():
    automated_leaderboard_channel_id_str = os.getenv("AUTOMATED_LEADERBOARD_CHANNEL_ID")
    if not automated_leaderboard_channel_id_str:
        print("Error: AUTOMATED_LEADERBOARD_CHANNEL_ID is not set in .env. Automated leaderboard will not be posted.")
        return
    
    try:
        automated_leaderboard_channel_id = int(automated_leaderboard_channel_id_str)
    except ValueError:
        print(f"Error: AUTOMATED_LEADERBOARD_CHANNEL_ID '{automated_leaderboard_channel_id_str}' is not a valid integer.")
        return

    channel = bot.get_channel(automated_leaderboard_channel_id)
    if channel:
        print(f"Posting automated leaderboard to channel: {channel.name} ({channel.id})")
        await generate_and_post_leaderboard(channel)
    else:
        print(f"Error: Automated leaderboard channel ID {automated_leaderboard_channel_id} not found or bot cannot access it.")

@automated_leaderboard_poster.before_loop
async def before_automated_leaderboard_poster():
    print('Waiting for bot to be ready before starting automated leaderboard poster...')
    await bot.wait_until_ready()
    print('Bot is ready, starting automated leaderboard poster.')

if __name__ == "__main__":
    discord_bot_token = os.getenv("DISCORD_BOT_TOKEN")
    google_service_account_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")

    if not discord_bot_token:
        print("Error: DISCORD_BOT_TOKEN is not set in .env")
    elif not google_service_account_file: 
        print("Error: GOOGLE_SERVICE_ACCOUNT_FILE is not set in .env (needed for Google Sheets connection)")
    else:
        bot.run(discord_bot_token)

