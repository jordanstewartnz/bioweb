import pandas as pd
from flask import Flask, request, render_template_string, send_file
import webbrowser
from threading import Timer
from geopy.distance import geodesic
import math
from datetime import datetime
import os # Import the os module
import io # Import io for in-memory file handling

# --- Global DataFrames and Initial Preprocessing ---
bat_data_df = None
herp_data_df = None
initial_data_load_error = None

# Define the expected filenames
BAT_FILENAME = "DOC_Bat_bioweb_data_2023.csv"
HERP_FILENAME = "DOC_Bioweb_Herpetofauna_data_2023.csv"

def load_and_preprocess_data():
    global bat_data_df, herp_data_df, initial_data_load_error
    script_dir = os.path.dirname(os.path.abspath(__file__))
    bat_filepath = os.path.join(script_dir, BAT_FILENAME)
    herp_filepath = os.path.join(script_dir, HERP_FILENAME)

    missing_files = []
    if not os.path.exists(bat_filepath):
        missing_files.append(BAT_FILENAME)
    if not os.path.exists(herp_filepath):
        missing_files.append(HERP_FILENAME)

    if missing_files:
        initial_data_load_error = f"Missing file(s): {', '.join(missing_files)}.<br><br>Recommendation: Please ensure '{BAT_FILENAME}' and '{HERP_FILENAME}' are in the same directory as 'bioweb.py'."
        return

    try:
        # Load and preprocess Bat data
        temp_bat_df = pd.read_csv(bat_filepath, low_memory=False)
        # For bat data, 'x' and 'y' are consistently used and renamed to Latitude/Longitude
        temp_bat_df.rename(columns={'x': 'Longitude', 'y': 'Latitude'}, inplace=True)
        temp_bat_df['date'] = pd.to_datetime(temp_bat_df['date'], errors='coerce', dayfirst=True)
        temp_bat_df.dropna(subset=['Latitude', 'Longitude', 'date', 'batspecies', 'roost'], inplace=True)
        bat_data_df = temp_bat_df

        # Load and preprocess Herpetofauna data
        temp_herp_df = pd.read_csv(herp_filepath, encoding='latin1', low_memory=False)

        # Explicitly use 'x' and 'y' for Herpetofauna data and rename them
        if 'x' in temp_herp_df.columns and 'y' in temp_herp_df.columns:
            temp_herp_df.rename(columns={'x': 'Longitude', 'y': 'Latitude'}, inplace=True)
            # Drop any other coordinate columns (lowercase or capitalized) to avoid ambiguity
            for col in ['latitude', 'longitude', 'Latitude', 'Longitude']:
                # Ensure not to drop the ones we just renamed to, which are now 'Latitude', 'Longitude'
                if col in temp_herp_df.columns and col not in ['Latitude', 'Longitude']:
                    temp_herp_df.drop(columns=[col], inplace=True)
        else:
            raise ValueError("Required 'x' and 'y' coordinate columns not found in Herpetofauna data.")

        # Continue with other renames and cleaning
        temp_herp_df.rename(columns={'observat_2': 'date', 'scientific': 'scientific_name', 'commonname': 'common_name'}, inplace=True)
        temp_herp_df['date'] = pd.to_datetime(temp_herp_df['date'], errors='coerce', dayfirst=True)
        temp_herp_df.dropna(subset=['Latitude', 'Longitude', 'date', 'scientific_name', 'common_name', 'sightingty'], inplace=True)
        temp_herp_df['scientific_name'] = temp_herp_df['scientific_name'].str.replace('"', '').str.strip()
        temp_herp_df['common_name'] = temp_herp_df['common_name'].str.replace('"', '').str.strip()
        temp_herp_df['sightingty'] = temp_herp_df['sightingty'].str.replace('"', '').str.strip()
        herp_data_df = temp_herp_df

    except Exception as e:
        initial_data_load_error = f"An error occurred during data loading or preprocessing: {e}"

# Call the data loading function once when the script starts
load_and_preprocess_data()


# --- HTML and CSS Template ---
def render_html_page(bat_results=None, herp_results=None, submitted_coords="", submitted_radius=25, error=None):
    """
    Generates the full HTML for the web page, embedding the results dynamically.
    """
    # Start of the HTML document with CSS styles
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>BioWeb Summary App</title>
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
                margin: 0;
                padding: 2em;
                background-color: #f4f4f9;
                color: #333;
            }}
            .container {{
                max-width: 900px;
                margin: auto;
                background: #fff;
                padding: 2em;
                border-radius: 8px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }}
            h1, h2 {{
                color: #2c3e50;
                border-bottom: 2px solid #e0e0e0;
                padding-bottom: 0.5em;
            }}
            form {{
                margin-bottom: 2em;
            }}
            label {{
                display: block;
                margin-bottom: 0.5em;
                font-weight: bold;
            }}
            input[type="text"], input[type="number"] {{
                width: 100%;
                padding: 10px;
                margin-bottom: 1em;
                border-radius: 4px;
                border: 1px solid #ccc;
                box-sizing: border-box;
            }}
            .button-group {{
                display: flex;
                gap: 10px;
                margin-top: 1em;
                flex-wrap: wrap;
            }}
            button[type="submit"] {{
                background-color: #3498db;
                color: white;
                padding: 12px 20px;
                border: none;
                border-radius: 4px;
                cursor: pointer;
                font-size: 16px;
                display: flex;
                align-items: center;
                justify-content: center;
                flex-grow: 1;
            }}
            button[type="submit"].download-button {{
                background-color: #27ae60; /* Green for download buttons */
                padding: 8px 15px; /* Smaller padding */
                font-size: 14px; /* Smaller font size */
                flex-grow: 0; /* Don't force them to grow */
                width: auto; /* Allow width to shrink to content */
            }}
            button[type="submit"]:hover {{
                background-color: #2980b9;
            }}
            button[type="submit"].download-button:hover {{
                background-color: #229954;
            }}
            button[type="submit"]:disabled {{
                background-color: #a0cbed;
                cursor: not-allowed;
            }}
            .results-container {{
                margin-top: 2em;
                border-top: 1px solid #ccc;
                padding-top: 1em;
            }}
            table {{
                border-collapse: separate;
                border-spacing: 0;
                width: 100%;
                margin-top: 1em;
            }}
            th, td {{
                border: 1px solid #ddd;
                padding: 12px;
                text-align: left;
            }}
            thead {{
                position: sticky;
                top: 0;
                z-index: 9999;
                box-shadow: 0 2px 5px rgba(0,0,0,0.2);
            }}
            th {{
                background-color: #f2f2f2;
                font-weight: bold;
            }}
            tr:nth-child(even) {{
                background-color: #f9f9f9;
            }}
            ul {{
                list-style-type: none;
                padding: 0;
            }}
            li {{
                background: #ecf0f1;
                margin-bottom: 8px;
                padding: 10px;
                border-radius: 4px;
            }}
            .error {{
                color: #c0392b;
                background-color: #f2dede;
                border: 1px solid #ebccd1;
                padding: 15px;
                border-radius: 4px;
                margin-bottom: 1em;
            }}
            .download-section {{
                margin-top: 1em;
                margin-bottom: 1em;
            }}


            /* Spinner styles */
            .spinner {{
                border: 4px solid rgba(255, 255, 255, 0.3);
                border-top: 4px solid #fff;
                border-radius: 50%;
                width: 20px;
                height: 20px;
                animation: spin 1s linear infinite;
                margin-left: 8px;
                display: none;
            }}

            @keyframes spin {{
                0% {{ transform: rotate(0deg); }}
                100% {{ transform: rotate(360deg); }}
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>DOC BioWeb Summary</h1>
            <form method="post" id="searchForm">
                <label for="coords">Coordinates (e.g., -40.2986, 175.7544):</label>
                <input type="text" id="coords" name="coords" size="40" value="{submitted_coords}" required>

                <label for="radius">Search Radius (1-50 km):</label>
                <input type="number" id="radius" name="radius" min="1" max="50" value="{submitted_radius}" required>

                <button type="submit" id="searchButton">
                    Search
                    <div class="spinner" id="spinner"></div>
                </button>
            </form>
    """

    if error:
        html += f'<div class="error">{error}</div>'

    if bat_results:
        html += '<div class="results-container"><h2>Bat Analysis Results</h2>'
        if bat_results.get("error"):
            html += f'<div class="error">{bat_results["error"]}</div>'
        else:
            counts = bat_results['counts']
            summary_table = bat_results['summary_table']
            html += f"""
                <h3>Summary within {submitted_radius} km radius:</h3>
                <ul>
                    <li><b>Total monitoring events:</b> {counts['total_events']}</li>
                    <li><b>Positive bat detections:</b> {counts['positive_detections']}</li>
                    <li><b>Chalinolobus tuberculatus (long-tailed) count:</b> {counts['chalinolobus_tuberculatus']} (including {counts['chalinolobus_tuberculatus_roosts']} roosts)</li>
                    <li><b>Mystacina tuberculata (short-tailed) count:</b> {counts['mystacina_tuberculata']} (including {counts['mystacina_tuberculata_roosts']} roosts)</li>
                    {"" if counts['unknown_bat_species'] == 0 else f"<li><b>Unknown bat species count:</b> {counts['unknown_bat_species']}</li>"}
                </ul>

                <h3>Nearest Record Summary (from full dataset):</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Species</th>
                            <th>All time nearest record</th>
                            <th>All time nearest roost</th>
                            <th>Nearest record 2018 to 2023</th>
                            <th>Nearest roost 2018 to 2023</th>
                            <th>Nearest record 2013 to 2023</th>
                            <th>Nearest roost 2013 to 2023</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            for row in summary_table:
                html += f"""
                    <tr>
                        <td>{row['Species']}</td>
                        <td>{row['All time nearest record']}</td>
                        <td>{row['All time nearest roost']}</td>
                        <td>{row['Nearest record 2018 to 2023']}</td>
                        <td>{row['Nearest roost 2018 to 2023']}</td>
                        <td>{row['Nearest record 2013 to 2023']}</td>
                        <td>{row['Nearest roost 2013 to 2023']}</td>
                    </tr>
                """
            html += "</tbody></table>"
            # Moved Bat Download Button here, outside main form
            html += f'''
            <div class="download-section">
                <form method="post" action="/download_bat_data">
                    <input type="hidden" name="coords" value="{submitted_coords}">
                    <input type="hidden" name="radius" value="{submitted_radius}">
                    <button type="submit" class="download-button">Download all occurrences within {submitted_radius} km</button>
                </form>
            </div>
            '''
        html += "</div>"

    if herp_results:
        html += f'<div class="results-container"><h2>Herpetofauna Analysis Results</h2>'
        html += f"<h3>Summary within {submitted_radius} km radius:</h3>"
        if herp_results.get("unique_species_count") is not None:
             html += f"<ul><li><b>Unique species count:</b> {herp_results['unique_species_count']}</li></ul>"
        if herp_results.get("error"):
            html += f'<div class="error">{herp_results["error"]}</div>'
        elif herp_results.get("message"):
            html += f"<p>{herp_results['message']}</p>"
        else:
            html += f"""
                <table>
                    <thead>
                        <tr>
                            <th>Species</th>
                            <th>Common Name</th>
                            <th>Observation Type</th>
                            <th>Most recent sighting within {submitted_radius} km</th>
                            <th>Nearest Record (all time)</th>
                            <th>Nearest Record 2018 to 2023</th>
                            <th>Nearest Record 2013 to 2023</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            for item in herp_results['results']:
                html += f"""
                    <tr>
                        <td>{item['species']}</td>
                        <td>{item['common_name']}</td>
                        <td>{item['observation_type_summary']}</td>
                        <td>{item['most_recent_sighting']}</td>
                        <td>{item['all_time']}</td>
                        <td>{item['past_5_years']}</td>
                        <td>{item['past_10_years']}</td>
                    </tr>
                """
            html += "</tbody></table>"
            # Moved Herpetofauna Download Button here, outside main form
            html += f'''
            <div class="download-section">
                <form method="post" action="/download_herp_data">
                    <input type="hidden" name="coords" value="{submitted_coords}">
                    <input type="hidden" name="radius" value="{submitted_radius}">
                    <button type="submit" class="download-button">Download all occurrences within {submitted_radius} km</button>
                </form>
            </div>
            '''
        html += "</div>"

    html += """
        </div>
        <script>
            document.getElementById('searchForm').addEventListener('submit', function() {
                const searchButton = document.getElementById('searchButton');
                const spinner = document.getElementById('spinner');

                searchButton.innerHTML = 'Searching ';
                searchButton.disabled = true;
                searchButton.appendChild(spinner);
                spinner.style.display = 'block';
            });
            // Download buttons will now function independently due to being in separate forms
            // No need for specific JS to manage their state for now, as they trigger full page POSTs
        </script>
    </body>
    </html>
    """
    return html

# --- Flask App and Data Processing Logic ---

app = Flask(__name__)

def calculate_direction(user_coords, record_coords):
    """
    Calculates the cardinal direction from user_coords to record_coords based on bearing.
    """
    lat1, lon1 = math.radians(user_coords[0]), math.radians(user_coords[1])
    lat2, lon2 = math.radians(record_coords[0]), math.radians(record_coords[1])

    delta_lon = lon2 - lon1

    y = math.sin(delta_lon) * math.cos(lat2)
    x = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(delta_lon)
    
    initial_bearing = math.degrees(math.atan2(y, x))
    
    # Normalize the bearing to be within 0-360 degrees
    bearing = (initial_bearing + 360) % 360

    if 337.5 <= bearing < 360 or 0 <= bearing < 22.5:
        return "north"
    elif 22.5 <= bearing < 67.5:
        return "northeast"
    elif 67.5 <= bearing < 112.5:
        return "east"
    elif 112.5 <= bearing < 157.5:
        return "southeast"
    elif 157.5 <= bearing < 202.5:
        return "south"
    elif 202.5 <= bearing < 247.5:
        return "southwest"
    elif 247.5 <= bearing < 292.5:
        return "west"
    elif 292.5 <= bearing < 337.5:
        return "northwest"
    else:
        return "unknown"

def process_bat_data(df, user_coords, radius_km):
    """Processes the bat data based on user input."""
    # Ensure distance_km is calculated based on current user_coords
    df['distance_km'] = df.apply(lambda row: geodesic(user_coords, (row['Latitude'], row['Longitude'])).km, axis=1)

    # Handle 'Both species detected' by duplicating rows for correct counting
    both_species_mask = df['batspecies'] == 'Both species detected'
    if both_species_mask.any():
        both_df = df[both_species_mask].copy()
        
        chalinolobus_df = both_df.copy()
        chalinolobus_df['batspecies'] = 'Chalinolobus tuberculatus'
        
        mystacina_df = both_df.copy()
        mystacina_df['batspecies'] = 'Mystacina tuberculata'
        
        calc_df_for_counts = pd.concat([df[~both_species_mask], chalinolobus_df, mystacina_df], ignore_index=True)
    else:
        calc_df_for_counts = df.copy() # No 'Both species detected' to expand

    radius_df = calc_df_for_counts[calc_df_for_counts['distance_km'] <= radius_km]

    bat_counts = {
        "total_events": len(radius_df), # Total events within radius after species expansion
        "positive_detections": len(radius_df[radius_df['batspecies'] != 'No bat species detected']),
        "chalinolobus_tuberculatus": len(radius_df[radius_df['batspecies'] == 'Chalinolobus tuberculatus']),
        "mystacina_tuberculata": len(radius_df[radius_df['batspecies'] == 'Mystacina tuberculata']),
        "unknown_bat_species": len(radius_df[radius_df['batspecies'] == 'Unknown bat species']),
        "chalinolobus_tuberculatus_roosts": len(radius_df[(radius_df['batspecies'] == 'Chalinolobus tuberculatus') & (radius_df['roost'] == 1)]),
        "mystacina_tuberculata_roosts": len(radius_df[(radius_df['batspecies'] == 'Mystacina tuberculata') & (radius_df['roost'] == 1)]),
    }

    summary_table = []
    species_list_for_table = ['Chalinolobus tuberculatus', 'Mystacina tuberculata']
    if bat_counts['unknown_bat_species'] > 0:
        species_list_for_table.append('Unknown bat species')

    start_date_5y = pd.to_datetime('2018-01-01', utc=True)
    start_date_10y = pd.to_datetime('2013-01-01', utc=True)
    end_date_2023 = pd.to_datetime('2023-12-31', utc=True)

    for species in species_list_for_table:
        species_full_df = df[df['batspecies'] == species] # Use original df with distance_km calculated

        # Helper function to get nearest record/roost string
        def get_nearest_info(subset_df, is_roost=False, date_range_str=""):
            if is_roost:
                subset_df = subset_df[subset_df['roost'] == 1]
            if subset_df.empty:
                return f"No {'roosts' if is_roost else 'records'} found{date_range_str}"
            
            nearest_item = subset_df.loc[subset_df['distance_km'].idxmin()]
            direction = calculate_direction(user_coords, (nearest_item['Latitude'], nearest_item['Longitude']))
            return f"{nearest_item['distance_km']:.1f} km {direction}"

        # All time nearest record
        all_time_str = get_nearest_info(species_full_df, is_roost=False)
        # All time nearest roost
        all_time_roost_str = get_nearest_info(species_full_df, is_roost=True)

        # Filter for 2018-2023
        past_5_years_df = species_full_df[(species_full_df['date'].dt.tz_localize(None) >= start_date_5y.tz_localize(None)) & \
                                          (species_full_df['date'].dt.tz_localize(None) <= end_date_2023.tz_localize(None))]
        past_5y_str = get_nearest_info(past_5_years_df, is_roost=False, date_range_str=" for 2018-2023")
        past_5y_roost_str = get_nearest_info(past_5_years_df, is_roost=True, date_range_str=" for 2018-2023")


        # Filter for 2013-2023
        past_10_years_df = species_full_df[(species_full_df['date'].dt.tz_localize(None) >= start_date_10y.tz_localize(None)) & \
                                           (species_full_df['date'].dt.tz_localize(None) <= end_date_2023.tz_localize(None))]
        past_10y_str = get_nearest_info(past_10_years_df, is_roost=False, date_range_str=" for 2013-2023")
        past_10y_roost_str = get_nearest_info(past_10_years_df, is_roost=True, date_range_str=" for 2013-2023")


        summary_table.append({
            "Species": species,
            "All time nearest record": all_time_str,
            "All time nearest roost": all_time_roost_str,
            "Nearest record 2018 to 2023": past_5y_str,
            "Nearest roost 2018 to 2023": past_5y_roost_str,
            "Nearest record 2013 to 2023": past_10y_str,
            "Nearest roost 2013 to 2023": past_10y_roost_str
        })

    return {"counts": bat_counts, "summary_table": summary_table}

def process_herpetofauna_data(df, user_coords, radius_km):
    """Processes the herpetofauna data."""
    # Ensure distance_km is calculated based on current user_coords
    df['distance_km'] = df.apply(lambda row: geodesic(user_coords, (row['Latitude'], row['Longitude'])).km, axis=1)

    radius_df = df[df['distance_km'] <= radius_km].copy()

    herp_results = []
    unique_species_in_radius = sorted(radius_df['scientific_name'].unique())

    unique_species_count = len(unique_species_in_radius)
    
    start_date_5y_herp = pd.to_datetime('2018-01-01', utc=True)
    start_date_10y_herp = pd.to_datetime('2013-01-01', utc=True)
    end_date_herp = pd.to_datetime('2023-12-31', utc=True)

    for species_scientific in unique_species_in_radius:
        species_in_radius_df = radius_df[radius_df['scientific_name'] == species_scientific]
        
        common_name = species_in_radius_df['common_name'].iloc[0] if not species_in_radius_df.empty else ""
        
        total_count_for_species_in_radius = len(species_in_radius_df)

        observation_types = species_in_radius_df['sightingty'].value_counts()
        observation_type_parts = [f"{sighting_type} ({count})" for sighting_type, count in observation_types.items()]
        
        observation_type_summary = ""
        if observation_type_parts:
            observation_type_summary = ", ".join(observation_type_parts)
            observation_type_summary += f"<br>&nbsp;<br><b>Total</b> ({total_count_for_species_in_radius})"
        else:
            observation_type_summary = f"<b>Total</b> ({total_count_for_species_in_radius})"

        # All time nearest record (within radius)
        nearest_record_in_radius = species_in_radius_df.loc[species_in_radius_df['distance_km'].idxmin()]
        direction_all_time = calculate_direction(user_coords, (nearest_record_in_radius['Latitude'], nearest_record_in_radius['Longitude']))
        all_time_str = f"{nearest_record_in_radius['distance_km']:.1f} km {direction_all_time}"

        # Most Recent Sighting (within radius)
        most_recent_sighting_date = species_in_radius_df['date'].max()
        if pd.notna(most_recent_sighting_date):
            most_recent_sighting_str = most_recent_sighting_date.strftime("%d/%m/%Y")
        else:
            most_recent_sighting_str = "No records found"


        # Data for this species from the *FULL dataset* for date range searches
        species_full_df = df[df['scientific_name'] == species_scientific]

        # Helper function to get nearest record string for herpetofauna
        def get_herp_nearest_info(subset_df, date_range_str=""):
            if subset_df.empty:
                return f"No records found{date_range_str}"
            
            nearest_item = subset_df.loc[subset_df['distance_km'].idxmin()]
            direction = calculate_direction(user_coords, (nearest_item['Latitude'], nearest_item['Longitude']))
            return f"{nearest_item['distance_km']:.1f} km {direction}"

        # Filter for 2018-2023 from the FULL species dataset
        past_5_years_full_df = species_full_df[(species_full_df['date'].dt.tz_localize(None) >= start_date_5y_herp.tz_localize(None)) & \
                                               (species_full_df['date'].dt.tz_localize(None) <= end_date_herp.tz_localize(None))]
        past_5y_str = get_herp_nearest_info(past_5_years_full_df, date_range_str=" for 2018-2023")

        # Filter for 2013-2023 from the FULL species dataset
        past_10_years_full_df = species_full_df[(species_full_df['date'].dt.tz_localize(None) >= start_date_10y_herp.tz_localize(None)) & \
                                                (species_full_df['date'].dt.tz_localize(None) <= end_date_herp.tz_localize(None))]
        past_10y_str = get_herp_nearest_info(past_10_years_full_df, date_range_str=" for 2013-2023")

        herp_results.append({
            "species": species_scientific,
            "common_name": common_name,
            "observation_type_summary": observation_type_summary,
            "most_recent_sighting": most_recent_sighting_str,
            "all_time": all_time_str,
            "past_5_years": past_5y_str,
            "past_10_years": past_10y_str
        })

    if not herp_results and unique_species_count == 0:
        return {"message": f"No herpetofauna records found within {radius_km} km.", "unique_species_count": 0}
    elif not herp_results and unique_species_count > 0:
         return {"message": f"No herpetofauna records found within {radius_km} km.", "unique_species_count": 0}

    return {"results": herp_results, "unique_species_count": unique_species_count}

@app.route('/', methods=['GET', 'POST'])
def index():
    if initial_data_load_error:
        return render_html_page(error=initial_data_load_error)

    if bat_data_df is None or herp_data_df is None:
        return render_html_page(error="Data files could not be loaded. Please check the server logs for details.")

    if request.method == 'POST':
        try:
            coords_str = request.form['coords']
            lat_str, lon_str = coords_str.split(',')
            user_coords = (float(lat_str.strip()), float(lon_str.strip()))
            radius_km = int(request.form['radius'])

            if not (1 <= radius_km <= 50):
                return render_html_page(error="Radius must be between 1 and 50 km.",
                                        submitted_coords=coords_str,
                                        submitted_radius=radius_km)

            # Pass the already loaded and preprocessed dataframes
            bat_results = process_bat_data(bat_data_df.copy(), user_coords, radius_km)
            herp_results = process_herpetofauna_data(herp_data_df.copy(), user_coords, radius_km)

            if bat_results and bat_results.get("error"):
                return render_html_page(error=bat_results["error"],
                                        submitted_coords=coords_str,
                                        submitted_radius=radius_km)
            if herp_results and herp_results.get("error"):
                return render_html_page(error=herp_results["error"],
                                        submitted_coords=coords_str,
                                        submitted_radius=radius_km)

            return render_html_page(bat_results=bat_results,
                                    herp_results=herp_results,
                                    submitted_coords=coords_str,
                                    submitted_radius=radius_km)

        except ValueError:
            return render_html_page(error="Invalid coordinates. Please use the format 'latitude, longitude' e.g., -40.298, 175.754",
                                    submitted_coords=request.form.get('coords', ""),
                                    submitted_radius=int(request.form.get('radius', 25)))
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return render_html_page(error=f"An unexpected error occurred: {e}. Please check your inputs and the data files.",
                                    submitted_coords=request.form.get('coords', ""),
                                    submitted_radius=int(request.form.get('radius', 25)))

    return render_html_page()

@app.route('/download_bat_data', methods=['POST'])
def download_bat_data():
    if initial_data_load_error or bat_data_df is None:
        return "Error: Data not loaded. " + (initial_data_load_error or ""), 500
    
    try:
        coords_str = request.form['coords']
        lat_str, lon_str = coords_str.split(',')
        user_coords = (float(lat_str.strip()), float(lon_str.strip()))
        radius_km = int(request.form['radius'])

        filtered_df = bat_data_df.copy()
        filtered_df['distance_km'] = filtered_df.apply(
            lambda row: geodesic(user_coords, (row['Latitude'], row['Longitude'])).km, axis=1
        )
        filtered_df = filtered_df[filtered_df['distance_km'] <= radius_km].copy()

        # Add direction column
        filtered_df['direction'] = filtered_df.apply(
            lambda row: calculate_direction(user_coords, (row['Latitude'], row['Longitude'])), axis=1
        )

        # Convert 'roost' from 0/1 to False/True strings
        if 'roost' in filtered_df.columns:
            filtered_df['roost'] = filtered_df['roost'].map({0: 'False', 1: 'True'})

        # Define custom sort order for batspecies
        bat_species_order = [
            "Both species detected",
            "Chalinolobus tuberculatus",
            "Mystacina tuberculata",
            "Unknown bat species",
            "No bat species detected" # Corrected 'No bat species' to 'No bat species detected'
        ]
        
        # Convert 'batspecies' to a categorical type with custom order
        filtered_df['batspecies'] = pd.Categorical(filtered_df['batspecies'], categories=bat_species_order, ordered=True)

        # Sort by species name (custom order) and then by distance
        filtered_df = filtered_df.sort_values(by=['batspecies', 'distance_km'])

        # Select only the desired columns for the output CSV
        desired_bat_columns = [
            'batspecies', 'locationna', 'roost', 'date', 'numberofpa',
            'detectorty', 'nightsout', 'surveymeth',
            'Longitude', 'Latitude',
            'distance_km', 'direction'
        ]
        
        # Ensure all desired columns exist before selecting to prevent KeyError
        final_columns = [col for col in desired_bat_columns if col in filtered_df.columns]
        filtered_df = filtered_df[final_columns]

        output = io.StringIO()
        filtered_df.to_csv(output, index=False)
        output.seek(0)
        
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=f'bat_data_within_{radius_km}km.csv'
        )
    except Exception as e:
        print(f"Error during bat data download: {e}")
        return f"An error occurred during bat data download: {e}", 500

@app.route('/download_herp_data', methods=['POST'])
def download_herp_data():
    if initial_data_load_error or herp_data_df is None:
        return "Error: Data not loaded. " + (initial_data_load_error or ""), 500

    try:
        coords_str = request.form['coords']
        lat_str, lon_str = coords_str.split(',')
        user_coords = (float(lat_str.strip()), float(lon_str.strip()))
        radius_km = int(request.form['radius'])

        filtered_df = herp_data_df.copy()
        filtered_df['distance_km'] = filtered_df.apply(
            lambda row: geodesic(user_coords, (row['Latitude'], row['Longitude'])).km, axis=1
        )
        filtered_df = filtered_df[filtered_df['distance_km'] <= radius_km].copy()

        # Add direction column
        filtered_df['direction'] = filtered_df.apply(
            lambda row: calculate_direction(user_coords, (row['Latitude'], row['Longitude'])), axis=1
        )

        # Convert recordveri from 0/1 to False/True strings
        if 'recordveri' in filtered_df.columns:
            filtered_df['recordveri'] = filtered_df['recordveri'].map({0: 'False', 1: 'True'})


        # Sort by species name and then by distance
        filtered_df = filtered_df.sort_values(by=['scientific_name', 'distance_km'])

        # Select only the desired columns for the output CSV
        desired_herp_columns = [
            'scientific_name', 'common_name', 'recordveri', 'date', 'placename',
            'sightingty', 'numberofin', 'identifica', 'ageinyears',
            'Longitude', 'Latitude', # These are already renamed from x/y in preprocessing
            'distance_km', 'direction'
        ]
        
        # Ensure all desired columns exist before selecting to prevent KeyError
        # If a column doesn't exist, it will be skipped from the final output
        final_columns = [col for col in desired_herp_columns if col in filtered_df.columns]
        filtered_df = filtered_df[final_columns]

        output = io.StringIO()
        filtered_df.to_csv(output, index=False)
        output.seek(0)
        
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=f'herpetofauna_data_within_{radius_km}km.csv'
        )
    except Exception as e:
        print(f"Error during herpetofauna data download: {e}")
        return f"An error occurred during herpetofauna data download: {e}", 500


def open_browser():
    """Opens the web browser to the application's URL."""
    webbrowser.open_new('http://127.0.0.1:5000/')

if __name__ == '__main__':
    Timer(1, open_browser).start()
    app.run(port=5000, debug=False)
