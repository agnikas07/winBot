from datetime import datetime as dt, time, timedelta
import discord
from discord.ext import commands, tasks
import gspread
from dotenv import load_dotenv
import google_sheet_utils as gsu
import asyncio
import os
import traceback
from zoneinfo import ZoneInfo


load_dotenv()

# --- Bot Setup ---
intents = discord.Intents.default()
intents.message_content = True
intents.messages = True
bot = commands.Bot(command_prefix="!", intents=intents)

# --- Global state for polling ---
last_known_row_count_g = 1
initial_check_done = False


# -- Helper to check for first sale ---
def is_first_sale(salesperson_name: str, all_sales_data: list, headers: list, first_name_column: str, current_sale_row_index: int) -> bool:
    """
    Checks if this is the first sale for a given salesperson by looking at historical sales data.
    """
    name_col_idx = -1
    try:
        name_col_idx = headers.index(first_name_column)
    except ValueError:
        return False

    for i in range(1, current_sale_row_index):
        if i < len(all_sales_data):
            previous_sale_row = all_sales_data[i]
            if len(previous_sale_row) > name_col_idx:
                if previous_sale_row[name_col_idx] == salesperson_name:
                    return False
    return True


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

        now_utc = dt.now(tz=ZoneInfo("UTC"))
        now_est = now_utc.astimezone(ZoneInfo("America/New_York"))

        team_total = sum(data['premium'] for data in leaderboard_data.values())

        embed = discord.Embed(
            title="🏆 Weekly Sales Leaderboard 🏆",
            description=f"Sales from {start_of_week.strftime('%b %d, %Y')} to {end_of_week.strftime('%b %d, %Y')}",
            color=discord.Color.gold()
        )
        embed.set_footer(text=f"Total Production: ${team_total:,.2f}\nLast updated: {now_est.strftime('%Y-%m-%d %I:%M %p %Z')}")

        ten_k_club =[]
        five_k_club = []
        main_board = []
        zero_board = []

        for name, data in leaderboard_data.items():
            premium = data["premium"]
            if premium >= 10000:
                ten_k_club.append((name, data))
            elif premium >= 5000:
                five_k_club.append((name, data))
            elif premium > 0:
                main_board.append((name, data))
            else:
                zero_board.append((name, data))

        position = 1
        total_people_added = 0

        custom_dbab_emoji = "<:DBAB:1369689466708557896>"
        custom_domore_emoji = "<:DOMOREGSD:1387049213686452245>"

        def add_person_to_embed(name, data, rank):
            total_premium = data['premium']
            num_apps = data['apps']
            suffix = ""

            if position == 1:
                prefix = "🥇"
            elif position == 2:
                prefix = "🥈"
            elif position == 3:
                prefix = "🥉"
            else:
                prefix = f"#{rank}"

            if total_premium >= 10000:
                suffix = "🏆"
            elif total_premium >= 5000:
                suffix = "🤑"
            elif total_premium >= 2500:
                suffix = custom_domore_emoji
            elif total_premium >= 1000:
                suffix = custom_dbab_emoji
            elif total_premium > 0:
                suffix = "🤡"
            else:
                suffix = "💤"

            apps_text = "App" if num_apps == 1 else "Apps"
            formatted_premium = f"${total_premium:,.2f}" if isinstance(total_premium, (int, float)) else str(total_premium)
            embed.add_field(name=f"{prefix} {name} {suffix}", value=f"Total Premium: **{formatted_premium}** | **{num_apps}** {apps_text}", inline=False)

        if ten_k_club:
            embed.add_field(name="\n--- 👑 10K CLUB 👑 ---", value="", inline=False)
            for name, data in ten_k_club:
                if total_people_added >= 15:
                    break
                add_person_to_embed(name, data, position)
                position += 1
                total_people_added += 1

        if five_k_club and total_people_added <15:
            embed.add_field(name="\n--- ⭐ 5K CLUB ⭐ ---", value="", inline=False)
            for name, data in five_k_club:
                if total_people_added >= 15:
                    break
                add_person_to_embed(name, data, position)
                position += 1
                total_people_added += 1

        if main_board and total_people_added < 15:
            if ten_k_club or five_k_club:
                embed.add_field(name=f"\n--- {custom_dbab_emoji} DBAB {custom_dbab_emoji} ---", value="", inline=False)
            for name, data in main_board:
                if total_people_added >= 15:
                    break
                add_person_to_embed(name, data, position)
                position += 1
                total_people_added += 1

        if zero_board and total_people_added < 15:
            if ten_k_club or five_k_club or main_board:
                embed.add_field(name="\n--- 😴 SLACKERS 😴 ---", value="", inline=False)
            for name, data in zero_board:
                if total_people_added >= 15:
                    break
                add_person_to_embed(name, data, position)
                position += 1
                total_people_added += 1

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
        all_values_from_sheet = sheet.get_all_values()
        current_total_rows = len(all_values_from_sheet)

        if current_total_rows > last_known_row_count_g:
            print(f"Change detected! Old rows: {last_known_row_count_g}, New rows: {current_total_rows}")
            headers = all_values_from_sheet[0] if len(all_values_from_sheet) > 0 else []

            leaderboard_data = gsu.get_weekly_leaderboard_data(sheet)
            
            notification_channel_id_str = os.getenv("NOTIFICATION_CHANNEL_ID")
            first_name_column = os.getenv("FIRST_NAME_COLUMN", "Name")
            sale_type_column = os.getenv("SALE_TYPE_COLUMN", "Sale Type")
            premium_column = os.getenv("PREMIUM_COLUMN", "Premium")
            appointments_left_column = os.getenv("APPOINTMENTS_LEFT_COLUMN", "Appointments Left")
            carrier_column = os.getenv("CARRIER_COLUMN", "Carrier")
            lead_age_column = os.getenv("LEAD_AGE_COLUMN", "Lead Age")
            lead_type_column = os.getenv("LEAD_TYPE_COLUMN", "Lead Type")

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

            for i in range(last_known_row_count_g, current_total_rows):
                if i < len(all_values_from_sheet):
                    row_values = all_values_from_sheet[i]
                    sale_data = {header: row_values[col_idx] if col_idx < len(row_values) else None for col_idx, header in enumerate(headers)}

                    first_name = sale_data.get(first_name_column, "N/A")
                    sale_type = sale_data.get(sale_type_column, "N/A")
                    premium = sale_data.get(premium_column, "N/A")
                    appointments_left = sale_data.get(appointments_left_column, "N/A")
                    carrier = sale_data.get(carrier_column, "N/A")
                    lead_age = sale_data.get(lead_age_column, "N/A")
                    lead_type = sale_data.get(lead_type_column, "N/A")

                    wtd_premium = leaderboard_data.get(first_name, {}).get("premium", 0.0)

                    if first_name != "N/A":
                        if is_first_sale(first_name, all_values_from_sheet, headers, first_name_column, i):
                            message = (f"🎉🎉{custom_alarm_emoji} **First Sale Alert!** {custom_alarm_emoji}🎉🎉\n\n"
                                       f"Congratulations to **{first_name}** on making their very first sale!\n"
                                       f"**Sale Type:** {sale_type}\n"
                                       f"**Annual Premium:** ${premium}\n"
                                       f"**Carrier:** {carrier}\n"
                                       f"**Lead Type:** {lead_type}\n"
                                       f"**Lead Age:** {lead_age}\n"
                                       f"**Appointments Left ➔** {appointments_left}\n"
                                       f"**Week to Date Sales:** ${wtd_premium:,.2f}\n\n"
                                       f"Welcome to the scoreboard! {custom_gsd_emoji}")
                        else:
                            message = (f"{custom_alarm_emoji} **New Sale!** {custom_alarm_emoji}\n\n"
                                       f"{first_name} just made a sale!\n"
                                       f"**Sale Type:** {sale_type}\n"
                                       f"**Annual Premium:** ${premium}\n"
                                       f"**Carrier:** {carrier}\n"
                                       f"**Lead Type:** {lead_type}\n"
                                       f"**Lead Age:** {lead_age}\n"
                                       f"**Appointments Left ➔** {appointments_left}\n"
                                       f"**Week to Date Sales:** ${wtd_premium:,.2f}\n\n"
                                       f"{custom_gsd_emoji}")
                        
                        await notification_channel.send(message)
                    else:
                        print(f"Skipping notification for incomplete sale data: {sale_data}")

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
@tasks.loop(time=time(19, 0, tzinfo=ZoneInfo("America/New_York")))
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


@tasks.loop(time=time(13,30, tzinfo=ZoneInfo("America/New_York")))
async def post_tuesday_motivation_gif():
    if dt.now(tz=ZoneInfo("America/New_York")).weekday() != 1:
        return
    
    sheet = gsu.get_sheet()
    if not sheet:
        print("Sheet not available for Tuesday GIF check.")

    leaderboard_data = gsu.get_weekly_leaderboard_data(sheet)

    if not leaderboard_data:
        gif_url = os.getenv("TUESDAY_NOON_GIF_URL")
        channel_id_str = os.getenv("NOTIFICATION_CHANNEL_ID")

        if not gif_url or not channel_id_str:
            print("Error: TUESDAY_NOON_GIF_URL or NOTIFICATION_CHANNEL_ID is not set in .env")
            return
        
        try:
            channel_id = int(channel_id_str)
        except ValueError:
            print(f"Error: NOTIFICATION_CHANNEL_ID '{channel_id_str}' is not a valid integer.")
            return
        
        channel = bot.get_channel(channel_id)
        if channel:
            print("No sales by Tuesday noon, posting motivation GIF.")
            await channel.send(gif_url)
        else:
            print(f"Error: Notification channel ID {channel_id} not found or bot cannot access it.")

@post_tuesday_motivation_gif.before_loop
async def before_post_tuesday_motivation_gif():
    print('Waiting for bot to be ready before posting Tuesday motivation GIF...')
    await bot.wait_until_ready()
    print('Bot is ready, starting Tuesday motivation GIF poster.')


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

