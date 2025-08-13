# telegram_weather_bot.py

from keep_alive import keep_alive
import logging
from logging.handlers import RotatingFileHandler
import os
import requests
import html
import json
import traceback
from datetime import datetime, timedelta
import pytz
import re
from collections import deque
from typing import Dict, Tuple, Optional, List

from telegram import (
    Update, 
    ReplyKeyboardMarkup, 
    KeyboardButton, 
    InlineKeyboardButton, 
    InlineKeyboardMarkup,
    Message,
    ChatMember
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
    CallbackQueryHandler,
    CallbackContext
)
from telegram.constants import ParseMode, ChatMemberStatus
from telegram.error import BadRequest
from apscheduler.schedulers.background import BackgroundScheduler

# --- Configuration ---
TELEGRAM_TOKEN = "7826068822:AAEjwtMeuK6pQvPfIx6t7RsrQ_IvVqaEy4g"
DEVELOPER_CHAT_ID = 7191595289  # Replace with your chat ID to receive error notifications
MAIN_CHANNEL_ID = "@Unix_Bots"  # The public username of the channel to check for membership

# --- Rate Limiting & Cleanup Configuration ---
RATE_LIMIT = 5  # Requests per minute
RATE_LIMIT_PERIOD = 60  # Seconds
DATA_EXPIRY_DAYS = 30  # Days to keep inactive user data
MEMBERSHIP_CACHE_EXPIRY = timedelta(hours=1)  # Cache membership status for 1 hour

# --- Setup Logging with Rotation ---
log_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
log_file = "weather_bot.log"

# Set up a rotating file handler
rotating_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=5, encoding='utf-8')
rotating_handler.setFormatter(log_formatter)

# Set up a stream handler for console output
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(log_formatter)

logging.basicConfig(
    level=logging.INFO,
    handlers=[rotating_handler, stream_handler]
)

logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# --- Cache & State Setup ---
WEATHER_CACHE: Dict[Tuple[float, float], Tuple[dict, datetime]] = {}
CACHE_EXPIRY = timedelta(minutes=10)
user_states: Dict[int, Dict] = {}
user_rate_limits: Dict[int, deque] = {}
membership_cache: Dict[int, Tuple[bool, datetime]] = {}  # user_id: (is_member, expiry_time)

# =============================================================================
# 0. DATA CLEANUP MODULE
# =============================================================================

def cleanup_old_data():
    """Periodically cleans up old user data and expired cache to save memory."""
    now = datetime.now()
    expiry_threshold = timedelta(days=DATA_EXPIRY_DAYS)
    
    inactive_users = []
    for user_id, state in list(user_states.items()):
        last_seen = state.get("last_seen")
        if last_seen and (now - last_seen > expiry_threshold):
            inactive_users.append(user_id)
            
    for user_id in inactive_users:
        if user_id in user_states:
            del user_states[user_id]
        if user_id in user_rate_limits:
            del user_rate_limits[user_id]
        if user_id in membership_cache:
            del membership_cache[user_id]
        logger.info(f"Cleaned up data for inactive user: {user_id}")

    expired_cache_keys = []
    for key, (data, timestamp) in list(WEATHER_CACHE.items()):
        if now - timestamp >= CACHE_EXPIRY:
            expired_cache_keys.append(key)

    for key in expired_cache_keys:
        if key in WEATHER_CACHE:
            del WEATHER_CACHE[key]
    
    # Clean expired membership cache
    expired_members = [user_id for user_id, (_, expiry) in membership_cache.items() if now > expiry]
    for user_id in expired_members:
        if user_id in membership_cache:
            del membership_cache[user_id]
        logger.info(f"Cleaned expired membership cache for user: {user_id}")
    
    if inactive_users or expired_cache_keys or expired_members:
        logger.info(f"Cleanup complete. Removed {len(inactive_users)} users, {len(expired_cache_keys)} cache entries, and {len(expired_members)} membership entries.")
    else:
        logger.info("Periodic cleanup ran. No old data to remove.")

def update_user_activity(user_id: int):
    """Updates the last_seen timestamp for a user."""
    if user_id not in user_states:
        user_states[user_id] = {}
    user_states[user_id]['last_seen'] = datetime.now()

# =============================================================================
# 1. ENHANCED MEMBERSHIP CHECK MODULE
# =============================================================================

async def check_channel_membership(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Checks if the user is a member of the main channel. 
    Uses cache to reduce API calls. Returns True if they are, False otherwise.
    """
    if not MAIN_CHANNEL_ID:
        return True  # Skip check if no channel is configured
    
    user_id = update.effective_user.id
    now = datetime.now()
    
    # Check cache first
    if user_id in membership_cache:
        is_member, expiry_time = membership_cache[user_id]
        if now < expiry_time:
            return is_member
    
    try:
        member = await context.bot.get_chat_member(chat_id=MAIN_CHANNEL_ID, user_id=user_id)
        is_member = member.status in [ChatMemberStatus.OWNER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.MEMBER]
        
        # Update cache
        expiry_time = now + MEMBERSHIP_CACHE_EXPIRY
        membership_cache[user_id] = (is_member, expiry_time)
        
        if not is_member:
            logger.info(f"User {user_id} is not a member of {MAIN_CHANNEL_ID} (status: {member.status})")
        return is_member
    except BadRequest:
        # User is not in the chat
        membership_cache[user_id] = (False, now + MEMBERSHIP_CACHE_EXPIRY)
        logger.info(f"User {user_id} is not a member of {MAIN_CHANNEL_ID} (BadRequest)")
        return False
    except Exception as e:
        logger.error(f"Could not verify membership for user {user_id} in {MAIN_CHANNEL_ID}: {e}")
        # Fail closed for security
        return False

async def verify_membership_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Command to manually verify channel membership and proceed."""
    user_id = update.effective_user.id
    update_user_activity(user_id)
    
    # Clear any cached membership status
    if user_id in membership_cache:
        del membership_cache[user_id]
    
    if await check_channel_membership(update, context):
        await update.message.reply_text("✅ Membership verified! Loading main menu...")
        await start_command(update, context)  # Automatically show the welcome message
    else:
        await reply_with_join_message(update)

async def reply_with_join_message(update: Update):
    """Sends a consistent 'join channel' message with verification button."""
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Join Channel", url=f"https://t.me/{MAIN_CHANNEL_ID.lstrip('@')}")],
        [InlineKeyboardButton("✅ I've Joined", callback_data="verify_membership")]
    ])
    text = (
        "🔒 To use this bot, you must be a member of our main channel.\n\n"
        "1. Tap 'Join Channel' below to join\n"
        "2. After joining, tap '✅ I've Joined' to verify\n"
    )
    
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.message.reply_text(text, reply_markup=keyboard)
    elif update.message:
        await update.message.reply_text(text, reply_markup=keyboard)

# =============================================================================
# 2. ENHANCED API INTERACTION MODULE
# =============================================================================

def rate_limit_user(user_id: int) -> bool:
    """Enforce rate limiting for users"""
    now = datetime.now()
    if user_id not in user_rate_limits:
        user_rate_limits[user_id] = deque(maxlen=RATE_LIMIT)
    
    while user_rate_limits[user_id] and (now - user_rate_limits[user_id][0]).total_seconds() > RATE_LIMIT_PERIOD:
        user_rate_limits[user_id].popleft()
    
    if len(user_rate_limits[user_id]) >= RATE_LIMIT:
        return False
    
    user_rate_limits[user_id].append(now)
    return True

def get_location_from_name(city_name: str) -> Optional[dict]:
    """Geocodes a city name with enhanced error handling"""
    try:
        if re.match(r'^[\U0001F300-\U0001F6FF\s]+$', city_name):
            return None
        url = f"https://geocoding-api.open-meteo.com/v1/search?name={city_name}&count=5&language=en&format=json"
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        if "results" in data and data["results"]:
            return data["results"][0]
        logger.warning(f"No results found for city: {city_name}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Geocoding API request failed for '{city_name}': {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in geocoding: {str(e)}")
        return None

def get_location_from_coords(lat: float, lon: float) -> Optional[dict]:
    """Reverse geocodes coordinates with enhanced error handling"""
    try:
        headers = {'User-Agent': 'TelegramWeatherBot/2.0'}
        url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lon}&zoom=10"
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        address = data.get('address', {})
        name_parts = [
            address.get('city') or address.get('town') or address.get('village'),
            address.get('state'),
            address.get('country')
        ]
        name = ", ".join(p for p in name_parts if p) or "Your Current Location"
        return {"name": name, "raw": data}
    except requests.exceptions.RequestException as e:
        logger.error(f"Reverse geocoding request failed: {str(e)}")
        return {"name": "Your Current Location"}
    except Exception as e:
        logger.error(f"Unexpected error in reverse geocoding: {str(e)}")
        return {"name": "Your Current Location"}

def get_weather_and_forecast(latitude: float, longitude: float) -> Optional[dict]:
    """Fetches weather data with caching and enhanced error handling"""
    cache_key = (round(latitude, 2), round(longitude, 2))
    now = datetime.now()
    
    if cache_key in WEATHER_CACHE:
        cached_data, timestamp = WEATHER_CACHE[cache_key]
        if now - timestamp < CACHE_EXPIRY:
            logger.info(f"Using cached weather data for {cache_key}")
            return cached_data
    
    try:
        params = {
            "latitude": latitude, "longitude": longitude,
            "current": "temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,weather_code,wind_speed_10m,uv_index,pressure_msl",
            "hourly": "temperature_2m,precipitation_probability,weather_code,visibility",
            "daily": "weather_code,temperature_2m_max,temperature_2m_min,sunrise,sunset,uv_index_max,precipitation_sum,precipitation_probability_max,wind_speed_10m_max",
            "timezone": "auto"
        }
        url = "https://api.open-meteo.com/v1/forecast"
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        
        try:
            aqi_params = {"latitude": latitude, "longitude": longitude, "hourly": "european_aqi"}
            aqi_url = "https://air-quality-api.open-meteo.com/v1/air-quality"
            aqi_response = requests.get(aqi_url, params=aqi_params, timeout=10)
            if aqi_response.status_code == 200:
                aqi_data = aqi_response.json()
                if "hourly" in aqi_data and "european_aqi" in aqi_data["hourly"]:
                    now_utc = datetime.now(pytz.utc)
                    current_time_str = now_utc.strftime('%Y-%m-%dT%H:00')
                    try:
                        time_index = aqi_data['hourly']['time'].index(current_time_str)
                        data["current"]["european_aqi"] = aqi_data["hourly"]["european_aqi"][time_index]
                    except (ValueError, IndexError):
                        data["current"]["european_aqi"] = aqi_data["hourly"]["european_aqi"][0]
        except Exception as e:
            logger.warning(f"Couldn't fetch air quality data: {str(e)}")
        
        WEATHER_CACHE[cache_key] = (data, now)
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Weather API request failed: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in weather API: {str(e)}")
        return None

# =============================================================================
# 3. ENHANCED DATA FORMATTING MODULE
# =============================================================================

WEATHER_DESCRIPTIONS = {
    0: ("☀️", "Clear sky"), 1: ("🌤️", "Mainly clear"), 2: ("⛅", "Partly cloudy"), 3: ("☁️", "Overcast"),
    45: ("🌫️", "Fog"), 48: ("🌫️", "Depositing rime fog"), 51: ("🌧️", "Light drizzle"), 53: ("🌧️", "Moderate drizzle"),
    55: ("🌧️", "Dense drizzle"), 56: ("🌧️🥶", "Light freezing drizzle"), 57: ("🌧️🥶", "Dense freezing drizzle"),
    61: ("🌧️", "Slight rain"), 63: ("🌧️", "Moderate rain"), 65: ("🌧️", "Heavy rain"), 66: ("🌧️🥶", "Light freezing rain"),
    67: ("🌧️🥶", "Heavy freezing rain"), 71: ("❄️", "Slight snow fall"), 73: ("❄️", "Moderate snow fall"),
    75: ("❄️", "Heavy snow fall"), 77: ("❄️", "Snow grains"), 80: ("🌦️", "Slight rain showers"),
    81: ("🌦️", "Moderate rain showers"), 82: ("⛈️", "Violent rain showers"), 85: ("🌨️", "Slight snow showers"),
    86: ("🌨️", "Heavy snow showers"), 95: ("⛈️", "Thunderstorm"), 96: ("⛈️🧊", "Thunderstorm with slight hail"),
    99: ("⛈️🧊", "Thunderstorm with heavy hail")
}

def get_weather_description(code: int) -> Tuple[str, str]:
    return WEATHER_DESCRIPTIONS.get(code, ("❓", "Unknown"))

def get_uv_description(uv_index: float) -> str:
    if uv_index is None: return "N/A"
    if uv_index <= 2: return f"{uv_index} (Low)"
    if uv_index <= 5: return f"{uv_index} (Moderate)"
    if uv_index <= 7: return f"{uv_index} (High)"
    if uv_index <= 10: return f"{uv_index} (Very High)"
    return f"{uv_index} (Extreme)"

def get_aqi_description(aqi: int) -> str:
    if aqi is None: return "N/A"
    if aqi <= 20: return f"{aqi} (Good)"
    if aqi <= 40: return f"{aqi} (Fair)"
    if aqi <= 60: return f"{aqi} (Moderate)"
    if aqi <= 80: return f"{aqi} (Poor)"
    if aqi <= 100: return f"{aqi} (Very Poor)"
    return f"{aqi} (Extremely Poor)"

def format_full_weather_report(weather_data: dict, location_name: str) -> str:
    if not weather_data:
        return "❌ Sorry, weather service is currently unavailable. Please try again later."
    try:
        current = weather_data.get('current', {})
        daily = weather_data.get('daily', {})
        hourly = weather_data.get('hourly', {})
        timezone_str = weather_data.get('timezone', 'UTC')
        local_tz = pytz.timezone(timezone_str)
        
        report = [f"📍 *{html.escape(location_name)}*"]
        emoji, weather_desc = get_weather_description(current.get('weather_code'))
        report.append("\n🌤️ *Current Conditions*")
        report.append(f"{emoji} {weather_desc}")
        report.append(f"🌡️ Temp: {current.get('temperature_2m', 'N/A')}°C (Feels like: {current.get('apparent_temperature', 'N/A')}°C)")
        report.append(f"💧 Humidity: {current.get('relative_humidity_2m', 'N/A')}%")
        report.append(f"💨 Wind: {current.get('wind_speed_10m', 'N/A')} km/h")
        report.append(f"☀️ UV Index: {get_uv_description(current.get('uv_index', 'N/A'))}")
        report.append(f"🌫️ Air Quality: {get_aqi_description(current.get('european_aqi', None))}")
        
        report.append("\n📅 *Today's Forecast*")
        report.append(f"🌡️ High/Low: {daily.get('temperature_2m_max', [None])[0]}°C / {daily.get('temperature_2m_min', [None])[0]}°C")
        report.append(f"💧 Precip: {daily.get('precipitation_probability_max', [None])[0]}% chance, {daily.get('precipitation_sum', [None])[0]} mm total")
        if daily.get('sunrise', [None])[0]:
            sunrise = datetime.fromisoformat(daily['sunrise'][0]).astimezone(local_tz).strftime('%H:%M')
            report.append(f"🌅 Sunrise: {sunrise}")
        if daily.get('sunset', [None])[0]:
            sunset = datetime.fromisoformat(daily['sunset'][0]).astimezone(local_tz).strftime('%H:%M')
            report.append(f"🌇 Sunset: {sunset}")
        
        hourly_times = hourly.get('time', [])
        if hourly_times:
            now_local = datetime.now(local_tz)
            current_hour_index = next((i for i, t in enumerate(hourly_times) if datetime.fromisoformat(t).astimezone(local_tz) >= now_local), -1)
            if current_hour_index != -1:
                report.append("\n⏱️ *Hourly Forecast*")
                for i in range(current_hour_index, min(current_hour_index + 13, len(hourly_times)), 3):
                    hour_str = datetime.fromisoformat(hourly['time'][i]).astimezone(local_tz).strftime('%H:%M')
                    emoji, _ = get_weather_description(hourly['weather_code'][i])
                    report.append(f"• `{hour_str}`: {emoji} {hourly['temperature_2m'][i]}°C, 💧{hourly['precipitation_probability'][i]}%")
        
        daily_times = daily.get('time', [])
        if daily_times and len(daily_times) > 1:
            report.append("\n📆 *7-Day Forecast*")
            for i in range(1, min(8, len(daily_times))):
                date_str = datetime.fromisoformat(daily['time'][i]).strftime('%a, %b %d')
                day_emoji, _ = get_weather_description(daily['weather_code'][i])
                report.append(f"• `{date_str}`: {day_emoji} {daily['temperature_2m_max'][i]}°/{daily['temperature_2m_min'][i]}° 💨{daily['wind_speed_10m_max'][i]}km/h")
        
        return "\n".join(report)
    except Exception as e:
        logger.error(f"Critical error formatting weather: {str(e)}", exc_info=True)
        return "❌ An error occurred while processing weather data."

# =============================================================================
# 4. ENHANCED BOT HANDLERS & MAIN LOGIC
# =============================================================================

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the /start command, checking membership and showing the welcome message."""
    user_id = update.effective_user.id
    update_user_activity(user_id)
    
    # Determine the message object to reply to.
    # This is the key fix for the AttributeError.
    message = update.message if update.message else update.callback_query.message

    if not await check_channel_membership(update, context):
        await reply_with_join_message(update)
        return

    keyboard = ReplyKeyboardMarkup([
        [KeyboardButton(text="📍 Send Location", request_location=True), KeyboardButton(text="🌤️ Get Forecast")]
    ], resize_keyboard=True, one_time_keyboard=True)
    
    await message.reply_html(
        rf"👋 Hello {update.effective_user.mention_html()}! I'm your Advanced Weather Assistant 🌦️",
        reply_markup=keyboard,
    )
    await message.reply_text(
        "You can:\n- Type a city name 🌆\n- Share your location 📍\n- Use commands for weather info\n\n"
        "Try me now! Where would you like weather for?",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Help Guide", callback_data="help_guide")]])
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await check_channel_membership(update, context):
        await reply_with_join_message(update)
        return
        
    update_user_activity(update.effective_user.id)
    help_text = (
        "📖 *Weather Bot Guide*\n\n"
        "1. *Get Weather*\n"
        "   - Type a city name (e.g., `Paris`)\n"
        "   - Share your location\n\n"
        "2. *Location Management*\n"
        "   - /setlocation `city` - Save a default location\n"
        "   - /mylocation - Show saved location\n"
        "   - /current - Get weather for saved location\n\n"
        "3. *Membership Verification*\n"
        "   - /verify - Re-check channel membership\n\n"
        "4. *Other Commands*\n"
        "   - /start - Welcome message\n"
        "   - /help - This guide\n"
        "   - /feedback `your message` - Send feedback"
    )
    if update.message:
        await update.message.reply_markdown(help_text)
    elif update.callback_query:
        await update.callback_query.message.reply_markdown(help_text)

async def feedback_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await check_channel_membership(update, context):
        await reply_with_join_message(update)
        return

    user_id = update.effective_user.id
    update_user_activity(user_id)
    feedback_text = " ".join(context.args)
    if not feedback_text:
        await update.message.reply_text("Please include your feedback after the command, e.g., `/feedback I love this bot!`")
        return
    
    with open("feedback.txt", "a", encoding="utf-8") as f:
        f.write(f"{datetime.now()} - User {user_id}: {feedback_text}\n")
    if DEVELOPER_CHAT_ID:
        try:
            await context.bot.send_message(chat_id=DEVELOPER_CHAT_ID, text=f"📝 Feedback from user {user_id}:\n{feedback_text}")
        except Exception as e:
            logger.error(f"Failed to send feedback: {str(e)}")
    await update.message.reply_text("✅ Thank you for your feedback!")

async def set_location_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await check_channel_membership(update, context):
        await reply_with_join_message(update)
        return

    user_id = update.effective_user.id
    update_user_activity(user_id)
    location = " ".join(context.args)
    if not location:
        await update.message.reply_text("Please specify a location. Example:\n/setlocation New York")
        return
    user_states[user_id]["default_location"] = location
    await update.message.reply_text(f"✅ Default location set to: {location}")

async def my_location_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await check_channel_membership(update, context):
        await reply_with_join_message(update)
        return

    user_id = update.effective_user.id
    update_user_activity(user_id)
    location = user_states.get(user_id, {}).get("default_location")
    if location:
        await update.message.reply_text(f"📍 Your saved location is: {location}")
    else:
        await update.message.reply_text("You haven't set a default location. Use /setlocation to save one.")

async def current_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await check_channel_membership(update, context):
        await reply_with_join_message(update)
        return

    user_id = update.effective_user.id
    update_user_activity(user_id)
    location = user_states.get(user_id, {}).get("default_location")
    if location:
        await handle_text_message(update, context, custom_text=location)
    else:
        await update.message.reply_text("You haven't set a default location. Please share a location or use /setlocation first.")

async def handle_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    update_user_activity(user_id)
    
    if query.data == "help_guide":
        await help_command(update, context)
    elif query.data == "verify_membership":
        # Clear any cached membership status
        if user_id in membership_cache:
            del membership_cache[user_id]
            
        if await check_channel_membership(update, context):
            await query.message.edit_text("✅ Membership verified! Loading main menu...")
            await start_command(update, context) # Automatically show the welcome message
        else:
            await query.message.edit_text("❌ You still haven't joined the channel. Please join and try again.")
            await reply_with_join_message(update)


async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE, custom_text: str = None) -> None:
    if not await check_channel_membership(update, context):
        await reply_with_join_message(update)
        return

    user_id = update.effective_user.id
    update_user_activity(user_id)
    text = custom_text or update.message.text
    
    if text == "🌤️ Get Forecast":
        await update.message.reply_text("Please type a city name or share your location 📍")
        return
    if not rate_limit_user(user_id):
        await update.message.reply_text("⏳ You're making requests too quickly. Please wait a moment.")
        return
    
    sent_message = await update.message.reply_text(f"🔍 Searching for *{text}*...", parse_mode=ParseMode.MARKDOWN)
    location_data = get_location_from_name(text)
    
    if location_data:
        full_display_name = ", ".join(filter(None, [location_data.get("name"), location_data.get("admin1"), location_data.get("country")]))
        await process_location_request(update, context, location_data["latitude"], location_data["longitude"], full_display_name, message_to_edit=sent_message)
    else:
        await sent_message.edit_text(f"❌ Couldn't find '{text}'. Please check the spelling.")

async def handle_location_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await check_channel_membership(update, context):
        await reply_with_join_message(update)
        return

    user_id = update.effective_user.id
    update_user_activity(user_id)
    if not rate_limit_user(user_id):
        await update.message.reply_text("⏳ You're making requests too quickly. Please wait a moment.")
        return
    
    location = update.message.location
    sent_message = await update.message.reply_text("📍 Getting weather for your location...")
    await process_location_request(update, context, location.latitude, location.longitude, "gps", message_to_edit=sent_message)

async def process_location_request(update: Update, context: ContextTypes.DEFAULT_TYPE, latitude: float, longitude: float, source_name: str, message_to_edit: Optional[Message] = None) -> None:
    try:
        display_name = source_name
        if source_name == "gps":
            location_info = get_location_from_coords(latitude, longitude)
            display_name = location_info.get("name", "Current Location")
        
        weather_data = get_weather_and_forecast(latitude, longitude)
        if not weather_data:
            error_msg = "❌ Weather service is currently unavailable. Please try again later."
            if message_to_edit: await message_to_edit.edit_text(error_msg)
            else: await safe_reply(update, error_msg)
            return
        
        weather_message = format_full_weather_report(weather_data, display_name)
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh", callback_data=f"refresh_{latitude}_{longitude}")]])
        
        if message_to_edit:
            await message_to_edit.edit_text(weather_message, parse_mode=ParseMode.MARKDOWN, reply_markup=keyboard, disable_web_page_preview=True)
        else:
            await safe_reply_markdown(update, weather_message, keyboard)
        
        user_id = update.effective_user.id
        if user_id not in user_states:
            user_states[user_id] = {}
        user_states[user_id]["last_location"] = (latitude, longitude, display_name)
    except Exception as e:
        logger.error(f"Error in location processing: {str(e)}", exc_info=True)
        await safe_reply(update, "❌ An unexpected error occurred. Our team has been notified.")

async def handle_refresh(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await check_channel_membership(update, context):
        await reply_with_join_message(update)
        return

    query = update.callback_query
    user_id = query.from_user.id
    update_user_activity(user_id)
    
    try:
        _, lat_str, lon_str = query.data.split('_')
        latitude, longitude = float(lat_str), float(lon_str)
    except (ValueError, IndexError) as e:
        logger.error(f"Could not parse refresh callback data: {query.data}, error: {e}")
        await query.answer()
        await query.edit_message_text("❌ Error refreshing. Please send a new location.")
        return

    display_name = "Refreshed Location"
    if user_id in user_states and "last_location" in user_states[user_id]:
        saved_lat, saved_lon, saved_name = user_states[user_id]["last_location"]
        if round(saved_lat, 4) == round(latitude, 4) and round(saved_lon, 4) == round(longitude, 4):
            display_name = saved_name
    
    try:
        await query.answer("Refreshing...")
        await process_location_request(update, context, latitude, longitude, display_name, message_to_edit=query.message)
    except Exception as e:
        logger.warning(f"Couldn't edit message for refresh: {str(e)}")
        await process_location_request(update, context, latitude, longitude, display_name)

async def safe_reply(update: Update, text: str) -> None:
    try:
        target = update.callback_query.message if update.callback_query else update.message
        await target.reply_text(text)
    except Exception as e:
        logger.error(f"Failed to send reply: {str(e)}")

async def safe_reply_markdown(update: Update, text: str, keyboard: InlineKeyboardMarkup = None) -> None:
    try:
        target = update.callback_query.message if update.callback_query else update.message
        await target.reply_markdown(text, reply_markup=keyboard, disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"Failed to send markdown reply: {str(e)}")
        await safe_reply(update, text)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Exception while handling update:", exc_info=context.error)
    tb_string = "".join(traceback.format_exception(None, context.error, context.error.__traceback__))
    
    if DEVELOPER_CHAT_ID:
        try:
            update_str = str(update) if isinstance(update, Update) else json.dumps(update, default=str)
            dev_message = (f"⚠️ Error: {context.error}\n\n"
                           f"Traceback:\n`{html.escape(tb_string[:3500])}`\n\n"
                           f"Update:\n`{html.escape(update_str[:200])}`")
            await context.bot.send_message(chat_id=DEVELOPER_CHAT_ID, text=dev_message, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.error(f"Failed to send error notification: {str(e)}")
    
    if isinstance(update, Update) and update.effective_message:
        await safe_reply(update, "⚠️ An unexpected error occurred. Our team has been notified.")

def main() -> None:
    if not TELEGRAM_TOKEN or TELEGRAM_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        logger.critical("FATAL: TELEGRAM_TOKEN is not set!")
        return
    
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    
    application.add_error_handler(error_handler)
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("feedback", feedback_command))
    application.add_handler(CommandHandler("setlocation", set_location_command))
    application.add_handler(CommandHandler("mylocation", my_location_command))
    application.add_handler(CommandHandler("current", current_command))
    application.add_handler(CommandHandler("verify", verify_membership_command))  # New command
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))
    application.add_handler(MessageHandler(filters.LOCATION, handle_location_message))
    
    # Combined handler for buttons
    application.add_handler(CallbackQueryHandler(handle_button, pattern="^(help_guide|verify_membership)$"))
    application.add_handler(CallbackQueryHandler(handle_refresh, pattern="^refresh_"))
    
    # --- Set up and start the background cleanup task ---
    scheduler = BackgroundScheduler(timezone=str(pytz.utc))
    scheduler.add_job(cleanup_old_data, 'interval', days=1)
    scheduler.start()
    
    logger.info("Starting bot with enhanced membership verification...")
    
    try:
        application.run_polling(allowed_updates=Update.ALL_TYPES)
    finally:
        scheduler.shutdown()
        logger.info("Bot stopped and scheduler shut down.")

if __name__ == "__main__":
    keep_alive()
    main()
