import requests
from datetime import datetime, timedelta
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from collections import Counter
from twilio.rest import Client
import time
from apscheduler.schedulers.background import BackgroundScheduler
from pytz import timezone
import atexit

# Twilio configuration
account_sid = ''
auth_token = ''
twilio_number = '+18885655470'
to_number = '+17208991490'

client = Client(account_sid, auth_token)
geolocator = Nominatim(user_agent="hail_report_locator", timeout=10)

def reverse_geocode(lat, lon, retries=3, backoff_factor=0.5):
    for attempt in range(retries):
        try:
            location = geolocator.reverse(f"{lat},{lon}", timeout=10)
            if location and location.raw.get('address'):
                address = location.raw['address']
                state = address.get('state')
                zip_code = address.get('postcode')
                if state == "Colorado" and zip_code:
                    return zip_code
        except (GeocoderTimedOut, GeocoderServiceError):
            time.sleep(backoff_factor * (2 ** attempt))
    return None

def generate_zip_code_data():
    # Calculate the date range for the last week
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')

    # Define the URL for the desired date range
    url = f"https://www.spc.noaa.gov/exper/reports/v3/src/getAllReports.php?combine&start={start_date}&end={end_date}&json"

    # Make the request
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()

        # Filter reports for hail ('HA') reports in Colorado ('CO')
        colorado_reports = [report for report in data if report.get('Type') == 'HA' and report.get('St') == 'CO']

        # Collect zip codes
        zip_codes = []
        for report in colorado_reports:
            lat = float(report.get('Lat')) / 100.0  # Convert to decimal degrees
            lon = float(report.get('Lon')) / -100.0  # Convert to decimal degrees
            
            zip_code = reverse_geocode(lat, lon)
            if zip_code:
                zip_codes.append(zip_code)
        
        # Count zip codes and return the most common one
        if zip_codes:
            zip_code_counts = Counter(zip_codes)
            most_common_zip_code = zip_code_counts.most_common(1)[0][0]
            return most_common_zip_code, zip_code_counts
    return None, None

def send_sms(most_common_zip_code, zip_code_counts):
    message_body = f"Latest hail report in Colorado:\nMost common zip code: {most_common_zip_code}\n\nHail Reports by Zip Code:\n"
    for zip_code, count in zip_code_counts.most_common():
        message_body += f"{zip_code}: {count} reports\n"

    message = client.messages.create(
        body=message_body,
        from_=twilio_number,
        to=to_number
    )
    print(f"Alert sent: {message.sid}")

def check_latest_hail_report():
    most_common_zip_code, zip_code_counts = generate_zip_code_data()
    if most_common_zip_code:
        send_sms(most_common_zip_code, zip_code_counts)
    else:
        print("No hail reports found in Colorado within the specified date range.")

# Schedule the update to run daily
scheduler = BackgroundScheduler(timezone=timezone('America/Denver'))
scheduler.add_job(func=check_latest_hail_report, trigger='interval', days=1)
scheduler.start()

# Shut down the scheduler when exiting the app
atexit.register(lambda: scheduler.shutdown())

# Initial check
check_latest_hail_report()
