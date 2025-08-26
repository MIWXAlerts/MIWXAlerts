import requests
import re
import time
import yaml
import os
import json
from datetime import datetime, timedelta
import pytz
import random
from flask import Flask, jsonify, request, send_file
import threading
import signal
import sys

CONFIG_FILE = "config.yml"

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    else:
        raise FileNotFoundError("config.yml not found. Please create it with WEBHOOKS, EMBED_COLORS, ALERT_ICONS, SAFETY_TIPS, and WINTER_ALERTS_ENABLED.")

config = load_config()
WEBHOOKS = config.get("WEBHOOKS", {})
EMBED_COLORS = config.get("EMBED_COLORS", {})
ALERT_ICONS = config.get("ALERT_ICONS", {})
SAFETY_TIPS = config.get("SAFETY_TIPS", {})
WINTER_ALERTS_ENABLED = config.get("WINTER_ALERTS_ENABLED", False)

ROLE_IDS = {
    "Severe Thunderstorm Warning": "1376030659642134538",
    "Severe Thunderstorm Watch": "1376030704936423605",
    "Tornado Warning": "1376031037888663624",
    "Tornado Watch": "1376031111553220751",
    "PDS Tornado Warning": "1376030729493807134",
    "Tornado Emergency": "1376030798662078566",
    "Tornado Observed": "1376030826252472413",
    "Extreme Heat Warning": "1376030876789375078",
    "Heat Advisory": "1376030897530474507",
    "Special Weather Statement": "1388611436850446342",
    "Winter Storm Warning": "1398035517743825016",
    "Winter Storm Watch": "1398035579765260358",
    "Winter Weather Advisory": "1398035614313611475",
    "Snow Squall Warning": "1398035669456257054",
    "Blizzard Warning": "1398035713332613254"
}

ERROR_WEBHOOK_URL = ""
DAILY_SUMMARY_WEBHOOK_URL = ""
NWS_BASE_URL = "https://api.weather.gov/alerts/active?area=MI"

sent_alerts = {}
SENT_ALERTS_FILE = "sent_alerts.json"
ALERT_CACHE_FILE = "alert_cache.json"
ALERT_LOG_FILE = "alert_logs.yml"
ALERTS_TXT_FILE = f"alerts_{datetime.now().strftime('%Y-%m-%d')}.txt"
ALERT_COUNTER_FILE = "alert_counter.json"

local_tz = pytz.timezone('America/New_York')

last_error_time = None
ERROR_RATE_LIMIT_SECONDS = 60 

app = Flask(__name__)
start_time = datetime.now()

STATE_TIMEZONES = {
    "AL": "America/Chicago",
    "AK": "America/Anchorage",
    "AZ": "America/Phoenix",
    "AR": "America/Chicago",
    "CA": "America/Los_Angeles",
    "CO": "America/Denver",
    "CT": "America/New_York",
    "DE": "America/New_York",
    "FL": "America/New_York",
    "GA": "America/New_York",
    "HI": "Pacific/Honolulu",
    "ID": "America/Boise", 
    "IL": "America/Chicago",
    "IN": "America/New_York",
    "IA": "America/Chicago",
    "KS": "America/Chicago",
    "KY": "America/New_York",
    "LA": "America/Chicago",
    "ME": "America/New_York",
    "MD": "America/New_York",
    "MA": "America/New_York",
    "MI": "America/New_York",
    "MN": "America/Chicago",
    "MS": "America/Chicago",
    "MO": "America/Chicago",
    "MT": "America/Denver",
    "NE": "America/Chicago",
    "NV": "America/Los_Angeles",
    "NH": "America/New_York",
    "NJ": "America/New_York",
    "NM": "America/Denver",
    "NY": "America/New_York",
    "NC": "America/New_York",
    "ND": "America/Chicago",
    "OH": "America/New_York",
    "OK": "America/Chicago",
    "OR": "America/Los_Angeles",
    "PA": "America/New_York",
    "RI": "America/New_York",
    "SC": "America/New_York",
    "SD": "America/Chicago",
    "TN": "America/Chicago",
    "TX": "America/Chicago",
    "UT": "America/Denver",
    "VT": "America/New_York",
    "VA": "America/New_York",
    "WA": "America/Los_Angeles",
    "WV": "America/New_York",
    "WI": "America/Chicago",
    "WY": "America/Denver"
}

STATE_ABBREVS = set(STATE_TIMEZONES.keys())

def load_alert_counter():
    if os.path.exists(ALERT_COUNTER_FILE):
        with open(ALERT_COUNTER_FILE, "r") as f:
            return json.load(f)
    return {
        "watch": 0,
        "warning": 0,
        "pds_emergency": 0,
        "heat": 0,
        "special_weather": 0,
        "winter": 0
    }

def save_alert_counter(counter):
    with open(ALERT_COUNTER_FILE, "w") as f:
        json.dump(counter, f)

alert_counter = load_alert_counter()

def get_alert_number(event_type):
    """Generate a unique alert number based on event type."""
    global alert_counter
    if event_type in ["Severe Thunderstorm Watch", "Tornado Watch"]:
        alert_counter["watch"] += 1
        number = f"1-{str(alert_counter['watch']).zfill(5)}"
    elif event_type in ["Severe Thunderstorm Warning", "Tornado Warning", "Tornado Observed"]:
        alert_counter["warning"] += 1
        number = f"2-{str(alert_counter['warning']).zfill(6)}"
    elif event_type in ["PDS Tornado Warning", "Tornado Emergency"]:
        alert_counter["pds_emergency"] += 1
        number = f"3-{str(alert_counter['pds_emergency']).zfill(6)}"
    elif event_type in ["Extreme Heat Warning", "Heat Advisory"]:
        alert_counter["heat"] += 1
        number = f"9-{str(alert_counter['heat']).zfill(8)}"
    elif event_type in ["Special Weather Statement"]:
        alert_counter["special_weather"] = alert_counter.get("special_weather", 0) + 1
        number = f"4-{str(alert_counter['special_weather']).zfill(6)}"
    elif event_type in ["Winter Storm Warning", "Winter Storm Watch", "Winter Weather Advisory", "Snow Squall Warning", "Blizzard Warning"]:
        alert_counter["winter"] = alert_counter.get("winter", 0) + 1
        number = f"8-{str(alert_counter['winter']).zfill(6)}"
    else:
        number = "0-UNKNOWN"
    save_alert_counter(alert_counter)
    return number

@app.route("/ping", methods=["GET"])
def ping():
    uptime = str(datetime.now() - start_time)
    return jsonify({"status": "OK", "message": "The server is running!", "uptime": uptime})

@app.route("/status", methods=["GET"])
def status():
    webhook_status = {}
    for event_type, webhook_url in WEBHOOKS.items():
        try:
            response = requests.get(webhook_url, timeout=5)
            webhook_status[event_type] = "Healthy" if response.status_code == 200 else f"Failed ({response.status_code})"
        except requests.exceptions.RequestException as e:
            webhook_status[event_type] = f"Error: {str(e)}"
    return jsonify({
        "active_alerts": len(sent_alerts),
        "webhook_status": webhook_status,
        "last_nws_fetch": fetch_nws_alerts.__last_fetch__ if hasattr(fetch_nws_alerts, "__last_fetch__") else "Never",
        "uptime": str(datetime.now() - start_time)
    })

@app.route("/alerts", methods=["GET"])
def get_alerts():
    date_filter = request.args.get("date", default=datetime.now().strftime('%Y-%m-%d'))
    if os.path.exists(ALERT_LOG_FILE):
        with open(ALERT_LOG_FILE, "r", encoding="utf-8") as f:
            logs = yaml.safe_load(f) or []
        filtered_logs = [log for log in logs if log["timestamp"].startswith(date_filter)]
        return jsonify({"alerts": filtered_logs, "count": len(filtered_logs)})
    return jsonify({"alerts": [], "count": 0, "message": "No logs available"})

@app.route("/logs", methods=["GET"])
def download_logs():
    today = datetime.now().strftime('%Y-%m-%d')
    log_file = f"alerts_{today}.txt"
    if os.path.exists(log_file):
        return send_file(log_file, as_attachment=True, download_name=f"alerts_{today}.txt")
    return jsonify({"status": "Error", "message": f"No log file found for {today}"}), 404

@app.route("/reload_config", methods=["POST"])
def reload_config():
    global WEBHOOKS, EMBED_COLORS, ALERT_ICONS, SAFETY_TIPS, WINTER_ALERTS_ENABLED
    try:
        new_config = load_config()
        WEBHOOKS = new_config.get("WEBHOOKS", {})
        EMBED_COLORS = new_config.get("EMBED_COLORS", {})
        ALERT_ICONS = new_config.get("ALERT_ICONS", {})
        SAFETY_TIPS = new_config.get("SAFETY_TIPS", {})
        WINTER_ALERTS_ENABLED = new_config.get("WINTER_ALERTS_ENABLED", False)
        send_error_log("Configuration reloaded successfully via /reload_config endpoint")
        return jsonify({"status": "Success", "message": "Configuration reloaded"})
    except Exception as e:
        send_error_log(f"Failed to reload configuration: {str(e)}")
        return jsonify({"status": "Error", "message": str(e)}), 500

def load_sent_data():
    global sent_alerts
    if os.path.exists(SENT_ALERTS_FILE):
        with open(SENT_ALERTS_FILE, "r") as f:
            data = json.load(f)
            sent_alerts = data.get("alerts", {})
    print(f"Loaded {len(sent_alerts)} alerts")

def save_sent_data():
    with open(SENT_ALERTS_FILE, "w") as f:
        json.dump({"alerts": sent_alerts}, f)

def load_alert_cache():
    if os.path.exists(ALERT_CACHE_FILE):
        with open(ALERT_CACHE_FILE, "r") as f:
            return json.load(f)
    return []

def save_alert_cache(cache):
    with open(ALERT_CACHE_FILE, "w") as f:
        json.dump(cache, f)

def fetch_nws_alerts():
    target_events = [
        "Severe Thunderstorm Watch", "Severe Thunderstorm Warning",
        "Tornado Watch", "Tornado Warning",
        "Extreme Heat Warning", "Heat Advisory",
        "Special Weather Statement"
    ]
    if WINTER_ALERTS_ENABLED:
        target_events.extend([
            "Winter Storm Warning",
            "Winter Storm Watch",
            "Winter Weather Advisory",
            "Snow Squall Warning",
            "Blizzard Warning"
        ])
    url = NWS_BASE_URL
    headers = {"User-Agent": "MIWXAlerts/1.0 (stroussdevon@gmail.com)"}
    params = {"event": ",".join(target_events)}
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        alerts = response.json().get("features", [])
        print(f"Fetched {len(alerts)} active alerts for targeted events")
        fetch_nws_alerts.__last_fetch__ = datetime.now().isoformat()
        return alerts
    except requests.exceptions.RequestException as e:
        send_error_log(f"Error fetching targeted alerts: {str(e)}")
        return []

def extract_states_and_timezone(area_desc):
    """Extract states and determine the dominant timezone from areaDesc."""
    areas = [area.strip() for area in area_desc.split(";")]
    states = set()
    
    for area in areas:
        for abbr in STATE_ABBREVS:
            if f" {abbr}" in area or area.endswith(abbr):
                states.add(abbr)
                break
    
    if len(states) > 1:
        states_list = ", ".join(sorted(states))
        dominant_state = sorted(states)[0]
        timezone_name = STATE_TIMEZONES.get(dominant_state, "America/New_York")
        timezone = pytz.timezone(timezone_name)
        return f"PARTS OF {states_list.upper()}", timezone
    return None, local_tz

def format_time_with_tz(utc_time_str, target_tz):
    """Convert UTC time to target timezone and format with abbreviation."""
    if not utc_time_str:
        return "Unknown Time"
    try:
        utc_time = datetime.fromisoformat(utc_time_str.replace("Z", "+00:00"))
        if utc_time.tzinfo is None:
            utc_time = pytz.utc.localize(utc_time)
        local_time = utc_time.astimezone(target_tz)
        tz_abbr = local_time.strftime('%Z')
        hour = local_time.strftime('%I').lstrip('0')
        period = local_time.strftime('%p').lower()
        return f"{hour} {period} {tz_abbr}".upper()
    except (ValueError, TypeError) as e:
        print(f"Error formatting time: {utc_time_str} - {str(e)}")
        return "Unknown Time"

def get_cities_for_counties(area_desc, description):
    """Extract cities from areaDesc and description, return formatted text and flag for cities."""
    text = f"{area_desc} {description}".lower()
    city_pattern = r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b'
    matches = re.findall(city_pattern, text)
    cities = set(match for match in matches if len(match) > 2 and match not in STATE_ABBREVS and not match.startswith('county'))
    
    if not cities:
        return "No specific cities identified.", False
    
    city_list = sorted(cities)
    location_text = ", ".join(city_list)
    
    if len(location_text) > 1024:
        location_text = location_text[:1000] + "... (see NWS link for full list)"
    
    return location_text, True

def send_discord_alert(event_type, alert, tornado_possible=False, is_update=False):
    if event_type not in WEBHOOKS:
        print(f"Skipping {event_type}: No webhook defined")
        return

    webhook_url = WEBHOOKS[event_type]
    embed_color = EMBED_COLORS.get(event_type, 0x000000)
    
    title = alert["properties"]["event"]
    if tornado_possible:
        title += " [Tornado Possible]"
    if is_update:
        title = f"UPDATED: {title}"
    
    description = alert["properties"].get("description", "No description available.")
    area = alert["properties"].get("areaDesc", "Unknown Area")
    nws_url = alert["properties"]["@id"]
    sent_raw = alert["properties"].get("sent", None)
    expires_raw = alert["properties"].get("expires", None)
    sender_name = alert["properties"].get("senderName", "National Weather Service")
    timestamp = convert_to_local_time(sent_raw) if sent_raw else "Unknown Time"

    multi_state_text, alert_tz = extract_states_and_timezone(area)
    location_text = multi_state_text if multi_state_text else area
    expires_time = format_time_with_tz(expires_raw, alert_tz) if expires_raw else "Unknown Time"
    
    alert_type = "WATCH" if "Watch" in event_type else "WARNING"
    formatted_description = (
        f"THE NATIONAL WEATHER SERVICE HAS ISSUED {alert_type} IN EFFECT UNTIL "
        f"{expires_time} THIS EVENING FOR THE FOLLOWING AREAS\n\n{location_text}\n\n{description}"
    )

    alert_number = get_alert_number(event_type)
    
    warning_types_with_details = [
        "Severe Thunderstorm Warning", "Tornado Warning", 
        "PDS Tornado Warning", "Tornado Emergency", "Tornado Observed",
        "Extreme Heat Warning", "Heat Advisory", "Special Weather Statement",
        "Winter Storm Warning", "Winter Storm Watch", "Winter Weather Advisory",
        "Snow Squall Warning", "Blizzard Warning"
    ]

    fields = [
        {"name": "📍 Location", "value": location_text, "inline": True},
        {"name": "📡 Issued By", "value": sender_name, "inline": True},
        {"name": "💡 Safety Tip", "value": random.choice(SAFETY_TIPS.get(event_type, ["Stay safe and follow local guidance."])), "inline": True},
        {"name": "🔗 More Info", "value": f"[NWS Link]({nws_url})", "inline": False}
    ]

    if event_type in warning_types_with_details:
        wind_speed = "N/A"
        movement = "N/A"
        gusts = "N/A"
        hail_size = "N/A"
        desc_lower = description.lower()

        speed_match = re.search(r"(\d+)\s*mph(?!\s*gust)", desc_lower)
        if speed_match:
            wind_speed = f"{speed_match.group(1)} MPH"

        direction_keywords = {
            "north": "North", "south": "South", "east": "East", "west": "West",
            "northeast": "Northeast", "northwest": "Northwest", "southeast": "Southeast", "southwest": "Southwest"
        }
        for keyword, direction in direction_keywords.items():
            if f"moving {keyword}" in desc_lower or f"heading {keyword}" in desc_lower:
                movement = direction
                break

        gust_match = re.search(r"gusts?\s*(?:up)?\s*to\s*(\d+)\s*mph", desc_lower)
        if gust_match:
            gusts = f"{gust_match.group(1)} MPH"

        if event_type in ["Severe Thunderstorm Warning", "Tornado Warning", "PDS Tornado Warning", "Tornado Emergency", "Tornado Observed"]:
            hail_match = re.search(r"(\d+(\.\d+)?)\s*inch(?:es)?\s*hail", desc_lower)
            if hail_match:
                hail_size = f"{hail_match.group(1)} inches"

        fields.insert(1, {"name": "💨 Wind Speed", "value": wind_speed, "inline": True})
        fields.insert(2, {"name": "🧭 Movement", "value": movement, "inline": True})
        fields.insert(3, {"name": "🌬️ Gusts", "value": gusts, "inline": True})
        if hail_size != "N/A":
            fields.insert(4, {"name": "❄️ Hail Size", "value": hail_size, "inline": True})

    city_text, has_cities = get_cities_for_counties(area, description)
    embeds = []
    
    embed_1 = {
        "title": f"{ALERT_ICONS.get(event_type, '🚨')} {title} [{alert_number}{', 1/2' if has_cities else ''}]",
        "description": formatted_description,
        "color": embed_color,
        "fields": fields,
        "timestamp": datetime.now(local_tz).isoformat()
    }
    embeds.append(embed_1)

    if has_cities:
        if len(city_text) <= 1024:
            embed_2 = {
                "title": f"{ALERT_ICONS.get(event_type, '🚨')} {title} [{alert_number}, 2/2]",
                "description": "Affected cities:",
                "color": embed_color,
                "fields": [{"name": "🏙️ Cities", "value": city_text, "inline": False}],
                "timestamp": datetime.now(local_tz).isoformat()
            }
            embeds.append(embed_2)
        else:
            cities = city_text.split(", ")
            first_batch = []
            second_batch = []
            current_length = 0
            for city in cities:
                if current_length + len(city) + 2 <= 1024:
                    first_batch.append(city)
                    current_length += len(city) + 2
                else:
                    second_batch.append(city)
            
            embed_2 = {
                "title": f"{ALERT_ICONS.get(event_type, '🚨')} {title} [{alert_number}, 2/3]",
                "description": "Affected cities (part 1):",
                "color": embed_color,
                "fields": [{"name": "🏙️ Cities", "value": ", ".join(first_batch), "inline": False}],
                "timestamp": datetime.now(local_tz).isoformat()
            }
            embeds.append(embed_2)
            
            embed_3 = {
                "title": f"{ALERT_ICONS.get(event_type, '🚨')} {title} [{alert_number}, 3/3]",
                "description": "Affected cities (part 2):",
                "color": embed_color,
                "fields": [{"name": "🏙️ Cities", "value": ", ".join(second_batch) if second_batch else "Continued list unavailable.", "inline": False}],
                "timestamp": datetime.now(local_tz).isoformat()
            }
            embeds.append(embed_3)

    critical_alerts = {"Tornado Emergency", "PDS Tornado Warning", "Tornado Observed"}
    if event_type in critical_alerts:
        role_id = ROLE_IDS.get(event_type, "")
        payload = {"content": f"<@&{role_id}>", "embeds": embeds}
    elif "Watch" in event_type:
        role_id = ROLE_IDS.get(event_type, "")
        payload = {"embeds": embeds}
    else:
        role_id = ROLE_IDS.get(event_type, "")
        payload = {"content": f"<@&{role_id}>", "embeds": embeds}

    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print(f"Sent alert: {title} [{alert_number}] to {event_type} channel{' (update)' if is_update else ''}")
        log_alert(event_type, event_type, area, description, nws_url, timestamp)
        alert_id = alert["id"]
        sent_alerts[alert_id] = {"sent": timestamp, "event_type": event_type}
        save_sent_data()
    except requests.exceptions.RequestException as e:
        send_error_log(f"Error sending alert for {event_type}: {str(e)}")
        cache = load_alert_cache()
        cache.append({"event_type": event_type, "alert": alert, "tornado_possible": tornado_possible, "timestamp": datetime.now(local_tz).isoformat()})
        save_alert_cache(cache)
        print(f"Cached alert {alert['id']} for retry")

def retry_cached_alerts():
    cache = load_alert_cache()
    if not cache:
        return
    new_cache = []
    for entry in cache:
        try:
            send_discord_alert(entry["event_type"], entry["alert"], entry["tornado_possible"])
        except Exception as e:
            send_error_log(f"Retry failed for cached alert {entry['alert']['id']}: {str(e)}")
            new_cache.append(entry)
    save_alert_cache(new_cache)

def send_error_log(message):
    global last_error_time
    now = time.time()
    if last_error_time is None or (now - last_error_time) >= ERROR_RATE_LIMIT_SECONDS:
        payload = {"content": f"Error: {message}"}
        try:
            response = requests.post(ERROR_WEBHOOK_URL, json=payload)
            response.raise_for_status()
            last_error_time = now
        except requests.exceptions.RequestException as e:
            print(f"Failed to send error log: {str(e)}")
    else:
        print(f"Error log rate-limited: {message}")

def send_daily_summary():
    while True:
        now = datetime.now(local_tz)
        target_time = now.replace(hour=23, minute=59, second=0, microsecond=0)
        if now >= target_time and now < target_time.replace(second=59):
            if os.path.exists(ALERT_LOG_FILE):
                with open(ALERT_LOG_FILE, "r") as file:
                    try:
                        logs = yaml.safe_load(file) or []
                    except yaml.YAMLError:
                        logs = []
            else:
                logs = []

            today = now.strftime('%Y-%m-%d')
            today_alerts = [log for log in logs if log["timestamp"].startswith(today)]
            alert_count = len(today_alerts)

            summary_text = f"**MIWXAlerts Daily Summary - {today}**\nTotal Alerts: {alert_count}\n"
            if alert_count > 0:
                event_counts = {}
                hour_counts = {}
                area_counts = {}
                critical_alerts = {"Tornado Emergency", "PDS Tornado Warning"}
                critical_count = 0

                for log in today_alerts:
                    event = log["event"]
                    event_counts[event] = event_counts.get(event, 0) + 1
                    timestamp = datetime.strptime(log["timestamp"], '%Y-%m-%d %H:%M:%S')
                    hour = timestamp.strftime('%H:00')
                    hour_counts[hour] = hour_counts.get(hour, 0) + 1
                    area = log["location"]
                    area_counts[area] = area_counts.get(area, 0) + 1
                    if any(critical in event for critical in critical_alerts):
                        critical_count += 1

                total = sum(event_counts.values())
                breakdown = "\n".join([f"{event}: {count} ({count/total:.1%})" for event, count in event_counts.items()])
                summary_text += f"**Alert Frequency:**\n{breakdown}\n"
                peak_hour = max(hour_counts, key=hour_counts.get, default="N/A")
                peak_count = hour_counts.get(peak_hour, 0)
                summary_text += f"**Peak Hour:** {peak_hour} ({peak_count} alerts)\n"
                most_active_area = max(area_counts, key=area_counts.get, default="N/A")
                most_active_count = area_counts.get(most_active_area, 0)
                summary_text += f"**Most Active Region:** {most_active_area} ({most_active_count} alerts)\n"
                summary_text += f"**Critical Alerts:** {critical_count} (Tornado Emergency, PDS, Observed)\n"
            else:
                summary_text += "No alerts recorded today.\n"

            embed = {"title": "🌩️ Daily Weather Alert Summary", "description": summary_text, "color": 0x00b7eb, "timestamp": now.isoformat()}
            payload = {"embeds": [embed]}
            try:
                response = requests.post(DAILY_SUMMARY_WEBHOOK_URL, json=payload)
                response.raise_for_status()
                print(f"Sent daily summary for {today} with {alert_count} alerts")
            except requests.exceptions.RequestException as e:
                send_error_log(f"Error sending daily summary: {str(e)}")

            time.sleep(60)
        else:
            seconds_to_target = (target_time - now).total_seconds()
            if seconds_to_target > 0:
                time.sleep(min(seconds_to_target, 60))

def send_health_ping():
    health_messages = [
        "All systems nominal.",
        "MIWXAlerts is running smoothly!",
        "Everything's looking good here.",
        "No issues detected, all clear!",
        "Systems are green across the board.",
        "MIWXAlerts is fully operational.",
        "Health check passed with flying colors!",
    ]
    while True:
        now = datetime.now(local_tz)
        message = random.choice(health_messages)
        embed = {"title": "🟢 MIWXAlerts Health Check", "description": message, "color": 0x00ff00, "timestamp": now.isoformat()}
        payload = {"embeds": [embed]}
        try:
            response = requests.post(ERROR_WEBHOOK_URL, json=payload)
            response.raise_for_status()
            print(f"Sent health ping to #errors: {message}")
        except requests.exceptions.RequestException as e:
            print(f"Failed to send health ping: {str(e)}")
        time.sleep(6 * 3600)

def convert_to_local_time(utc_time_str):
    if not utc_time_str:
        return "Unknown Time"
    try:
        utc_time = datetime.fromisoformat(utc_time_str.replace("Z", "+00:00"))
        if utc_time.tzinfo is None:
            utc_time = pytz.utc.localize(utc_time)
        local_time = utc_time.astimezone(local_tz)
        return local_time.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError) as e:
        print(f"Error converting time: {utc_time_str} - {str(e)}")
        return "Unknown Time"

def check_for_pds_tornado_warning(alert):
    headline = alert["properties"].get("headline", "").lower()
    description = alert["properties"].get("description", "").lower()
    return "particularly dangerous situation" in headline or "particularly dangerous situation" in description

def check_for_tornado_emergency(alert):
    headline = alert["properties"].get("headline", "").lower()
    description = alert["properties"].get("description", "").lower()
    return "tornado emergency" in headline or "tornado emergency" in description

def check_for_tornado_observed(alert):
    description = alert["properties"].get("description", "").lower()
    return "observed" in description or "confirmed" in description

def check_for_tornado_possible(alert):
    headline = alert["properties"].get("headline", "").lower()
    description = alert["properties"].get("description", "").lower()
    tornado_phrases = ["tornado possible", "possible tornado", "radar indicated tornado"]
    is_tornado_possible = any(phrase in headline or phrase in description for phrase in tornado_phrases)
    if is_tornado_possible:
        print(f"Tornado Possible detected for alert {alert['id']}")
    return is_tornado_possible

def log_alert(event_type, event, area, description, nws_url, timestamp):
    alert_data = {
        "timestamp": timestamp,
        "event": event,
        "location": area,
        "details": description,
        "url": nws_url
    }

    if os.path.exists(ALERT_LOG_FILE):
        with open(ALERT_LOG_FILE, "r") as file:
            try:
                logs = yaml.safe_load(file) or []
            except yaml.YAMLError:
                logs = []
    else:
        logs = []

    logs.append(alert_data)

    with open(ALERT_LOG_FILE, "w") as file:
        yaml.dump(logs, file, default_flow_style=False)

    global ALERTS_TXT_FILE
    today = datetime.now().strftime('%Y-%m-%d')
    ALERTS_TXT_FILE = f"alerts_{today}.txt"

    with open(ALERTS_TXT_FILE, "a") as file:
        file.write(f"{timestamp} - {event} [{area}]\n")
        file.write(f"Details: {description}\n")
        file.write("="*50 + "\n")

def check_for_alerts():
    global sent_alerts
    alerts = fetch_nws_alerts()
    target_events = set(WEBHOOKS.keys()) - {"PDS Tornado Warning", "Tornado Observed", "Tornado Emergency"}
    if not WINTER_ALERTS_ENABLED:
        target_events -= {"Winter Storm Warning", "Winter Storm Watch", "Winter Weather Advisory", "Snow Squall Warning", "Blizzard Warning"}

    for alert in alerts:
        alert_id = alert["id"]
        event_type = alert["properties"]["event"]
        status = alert["properties"].get("status", "Actual").lower()
        message_type = alert["properties"].get("messageType", "Alert").lower()

        if status != "actual" or message_type == "cancel":
            print(f"Skipping {alert_id}: Status={status}, MessageType={message_type}")
            continue

        if event_type not in target_events:
            continue

        tornado_possible = False
        if event_type == "Severe Thunderstorm Warning":
            tornado_possible = check_for_tornado_possible(alert)

        if event_type == "Tornado Warning":
            if check_for_tornado_emergency(alert):
                event_type = "Tornado Emergency"
            elif check_for_pds_tornado_warning(alert):
                event_type = "PDS Tornado Warning"
            elif check_for_tornado_observed(alert):
                event_type = "Tornado Observed"

        if event_type in WEBHOOKS:
            if alert_id not in sent_alerts:
                send_discord_alert(event_type, alert, tornado_possible)
        elif message_type == "update":
            original_event = sent_alerts[alert_id].get("event_type", "")
            if original_event == "Tornado Warning" and event_type in ["PDS Tornado Warning", "Tornado Emergency", "Tornado Observed"]:
                send_discord_alert(event_type, alert, tornado_possible, is_update=True)
            else:
                print(f"Skipping update for {alert_id}: No escalation to PDS/Emergency (from {original_event} to {event_type})")
                continue

def run_flask():
    app.run(host="0.0.0.0", port=5000)

def signal_handler(sig, frame):
    print("Received SIGINT, shutting down gracefully...")
    save_sent_data()
    save_alert_cache(load_alert_cache())
    save_alert_counter(alert_counter)
    send_error_log("Shutting down gracefully.")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)

    print(f"Starting alert monitoring at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    load_sent_data()

    failed_webhooks = []
    for event_type, webhook_url in WEBHOOKS.items():
        try:
            response = requests.get(webhook_url, timeout=5)
            response.raise_for_status()
            print(f"Webhook for {event_type} is valid")
        except requests.exceptions.RequestException as e:
            failed_webhooks.append(event_type)
            send_error_log(f"Invalid webhook detected for {event_type}: {str(e)}")
            print(f"Webhook validation failed for {event_type}: {str(e)}")

    if not failed_webhooks:
        send_error_log("All webhooks are valid and started successfully!")
    else:
        print(f"Some webhooks failed validation: {', '.join(failed_webhooks)}")

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    summary_thread = threading.Thread(target=send_daily_summary, daemon=True)
    summary_thread.start()

    health_thread = threading.Thread(target=send_health_ping, daemon=True)
    health_thread.start()

    while True:
        try:
            check_for_alerts()
            retry_cached_alerts()
            time.sleep(random.randint(1, 2))
        except Exception as e:
            send_error_log(f"Main loop error: {str(e)}")
            time.sleep(1)
