import pandas as pd
from flask import Flask, request, render_template_string, send_file
import webbrowser
from threading import Timer
from geopy.distance import geodesic
import math
from datetime import datetime
import os
import io
import sys # Import the sys module for PyInstaller path handling

# --- Global DataFrames and Initial Preprocessing ---
bat_data_df = None
herp_data_df = None
threat_status_df = None # New global DataFrame for threat status
initial_data_load_error = None

# Define the expected filenames
BAT_FILENAME = "DOC_Bat_bioweb_data_2023.csv"
HERP_FILENAME = "DOC_Bioweb_Herpetofauna_data_2023.csv"
THREAT_STATUS_FILENAME = "threat_status.csv" # New threat status file

def load_and_preprocess_data():
    global bat_data_df, herp_data_df, threat_status_df, initial_data_load_error

    # Determine the base directory for finding data files
    # When running as a PyInstaller executable, sys.frozen is True and sys.argv[0] points to the executable itself.
    # When running as a normal Python script, __file__ points to the script.
    if getattr(sys, 'frozen', False):
        # Running in a PyInstaller bundle, use the path of the executable
        base_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    else:
        # Running as a normal Python script
        base_dir = os.path.dirname(os.path.abspath(__file__))

    bat_filepath = os.path.join(base_dir, BAT_FILENAME)
    herp_filepath = os.path.join(base_dir, HERP_FILENAME)
    threat_status_filepath = os.path.join(base_dir, THREAT_STATUS_FILENAME) # Path for new CSV

    missing_files = []
    if not os.path.exists(bat_filepath):
        missing_files.append(BAT_FILENAME)
    if not os.path.exists(herp_filepath):
        missing_files.append(HERP_FILENAME)
    if not os.path.exists(threat_status_filepath): # Check for new CSV
        missing_files.append(THREAT_STATUS_FILENAME)

    if missing_files:
        initial_data_load_error = f"Missing file(s): {', '.join(missing_files)}.<br><br>Recommendation: Please ensure all required CSVs are in the same directory as 'bioweb.py'."
        return

    try:
        # Load and preprocess Bat data
        temp_bat_df = pd.read_csv(bat_filepath, low_memory=False)
        # For bat data, 'x' and 'y' are consistently used and renamed to Latitude/Longitude
        temp_bat_df.rename(columns={'x': 'Longitude', 'y': 'Latitude'}, inplace=True)
        temp_bat_df['date'] = pd.to_datetime(temp_bat_df['date'], errors='coerce', dayfirst=True)
        # Explicitly convert 'roost' to integer, coercing errors to NaN, then filling NaN with 0, and dropping other NaNs
        temp_bat_df['roost'] = pd.to_numeric(temp_bat_df['roost'], errors='coerce').fillna(0).astype(int)
        temp_bat_df.dropna(subset=['Latitude', 'Longitude', 'date', 'batspecies'], inplace=True) # 'roost' no longer needs to be in subset for dropna as its NaNs are filled
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
        
        # Process blank 'sightingty' as "Undefined" BEFORE dropping NaNs from other critical columns
        if 'sightingty' in temp_herp_df.columns:
            # First, convert to string and strip whitespace, then replace empty strings with pandas.NA
            temp_herp_df['sightingty'] = temp_herp_df['sightingty'].astype(str).str.replace('"', '').str.strip()
            temp_herp_df['sightingty'].replace('', pd.NA, inplace=True)
            # Now fill any actual NaNs (including those created from empty strings) with 'Undefined'
            temp_herp_df['sightingty'].fillna('Undefined', inplace=True)

        # Drop NaNs from other critical columns, 'sightingty' is now robustly handled
        temp_herp_df.dropna(subset=['Latitude', 'Longitude', 'date', 'scientific_name', 'common_name'], inplace=True)
        
        # Ensure other critical columns are also cleaned of quotes/strip if needed
        temp_herp_df['scientific_name'] = temp_herp_df['scientific_name'].str.replace('"', '').str.strip()
        temp_herp_df['common_name'] = temp_herp_df['common_name'].str.replace('"', '').str.strip()


        herp_data_df = temp_herp_df

        # Load and preprocess Threat Status data
        temp_threat_status_df = pd.read_csv(threat_status_filepath, low_memory=False)
        # Clean 'Current Species Name' for matching
        temp_threat_status_df['Current Species Name'] = temp_threat_status_df['Current Species Name'].str.replace('"', '').str.strip()
        threat_status_df = temp_threat_status_df

    except Exception as e:
        initial_data_load_error = f"An error occurred during data loading or preprocessing: {e}"

# Call the data loading function once when the script starts
load_and_preprocess_data()


# --- HTML and CSS Template ---
def render_html_page(bat_results=None, herp_results=None, submitted_coords="", submitted_radius=25, error=None):
    """
    Generates the full HTML for the web page, embedding the results dynamically.
    """
    # Define CSS classes for threat status highlighting
    # Note: No background-color for 'Extinct' or 'Introduced and Naturalised'
    CSS_CLASSES_FOR_THREAT_STATUS = {
        'Threatened': 'threatened-bg',
        'At Risk': 'at-risk-bg',
        'Not Threatened': 'not-threatened-bg',
        'Non-resident Native': 'non-resident-native-bg',
        'Extinct': 'extinct-text-color', # Just text color, no background
        'Unknown': 'unknown-text-color' # Light grey for unknown text color
    }

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
                max-width: 1200px; /* Increased width */
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
                width: 100%; /* Ensure table takes full available width */
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
                display: flex; /* Use flexbox for button alignment */
                gap: 10px;    /* Space between buttons */
                flex-wrap: wrap; /* Allow wrapping on small screens */
            }}
            .download-section form {{
                margin-bottom: 0; /* Remove extra margin from form inside download section */
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

            /* Threat Status Cell Styling */
            /* Apply padding here to keep consistent cell size with other table cells */
            .threat-status-cell {{
                padding: 12px;
                text-align: left; /* Ensure text alignment is consistent */
            }}
            .threatened-bg {{ background-color: #FFCCCC; }}     /* Light Red */
            .at-risk-bg {{ background-color: #FFEBCC; }}        /* Light Orange */
            .not-threatened-bg {{ background-color: #D9FFD9; }} /* Light Green */
            .non-resident-native-bg {{ background-color: #CCEEFF; }} /* Greenish-blue turquoise */
            .extinct-text-color {{ color: #888; background-color: transparent; }} /* Grey text, no background */
            .unknown-text-color {{ color: #A0A0A0; background-color: transparent; }} /* Slightly darker grey for unknown */
            /* Introduced and Naturalised will have default background (white/f9f9f9) and black text */
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
                            <th>Nearest record 2013 to 2023</th>
                            <th>Nearest record 2018 to 2023</th>
                            <th>All time nearest roost</th>
                            <th>Nearest roost 2013 to 2023</th>
                            <th>Nearest roost 2018 to 2023</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            for row in summary_table:
                html += f"""
                    <tr>
                        <td>{row['Species']}</td>
                        <td>{row['All time nearest record']}</td>
                        <td>{row['Nearest record 2013 to 2023']}</td>
                        <td>{row['Nearest record 2018 to 2023']}</td>
                        <td>{row['All time nearest roost']}</td>
                        <td>{row['Nearest roost 2013 to 2023']}</td>
                        <td>{row['Nearest roost 2018 to 2023']}</td>
                    </tr>
                """
            html += "</tbody></table>"
            # Download buttons for bat data
            html += f'''
            <div class="download-section">
                <form method="post" action="/download_bat_data">
                    <input type="hidden" name="coords" value="{submitted_coords}">
                    <input type="hidden" name="radius" value="{submitted_radius}">
                    <button type="submit" class="download-button">Download all occurrences within {submitted_radius} km</button>
                </form>
                <form method="post" action="/download_bat_summary_data">
                    <input type="hidden" name="coords" value="{submitted_coords}">
                    <input type="hidden" name="radius" value="{submitted_radius}">
                    <button type="submit" class="download-button">Download Summary CSV</button>
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
                            <th>Taxa Group</th>
                            <th>Species</th>
                            <th>Common Name</th>
                            <th>Threat Status</th>
                            <th>Observation Type</th>
                            <th>Most recent sighting within {submitted_radius} km</th>
                            <th>Nearest Record (all time)</th>
                            <th>Nearest Record 2013 to 2023</th>
                            <th>Nearest Record 2018 to 2023</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            for item in herp_results['results']:
                # Determine the class for the threat status cell
                category_for_class = item.get('category_for_sort', 'Unknown')
                threat_status_cell_class = CSS_CLASSES_FOR_THREAT_STATUS.get(category_for_class, '')
                
                # If category is 'Introduced and Naturalised', no specific class for background/text color
                if category_for_class == 'Introduced and Naturalised':
                    threat_status_cell_class = ''
                elif category_for_class == 'unknown':
                     threat_status_cell_class = CSS_CLASSES_FOR_THREAT_STATUS.get('Unknown', '')

                # Add the base class 'threat-status-cell' along with any specific highlighting class
                cell_class_attr = f' class="threat-status-cell {threat_status_cell_class}"' if threat_status_cell_class else ' class="threat-status-cell"'

                html += f"""
                    <tr>
                        <td>{item['taxa_group']}</td>
                        <td>{item['species']}</td>
                        <td>{item['common_name']}</td>
                        <td{cell_class_attr}>{item['threat_status_display']}</td>
                        <td>{item['observation_type_summary']}</td>
                        <td>{item['most_recent_sighting']}</td>
                        <td>{item['all_time']}</td>
                        <td>{item['past_10_years']}</td>
                        <td>{item['past_5_years']}</td>
                    </tr>
                """
            html += "</tbody></table>"
            # Download buttons for herpetofauna data
            html += f'''
            <div class="download-section">
                <form method="post" action="/download_herp_data">
                    <input type="hidden" name="coords" value="{submitted_coords}">
                    <input type="hidden" name="radius" value="{submitted_radius}">
                    <button type="submit" class="download-button">Download all occurrences within {submitted_radius} km</button>
                </form>
                <form method="post" action="/download_herp_summary_data">
                    <input type="hidden" name="coords" value="{submitted_coords}">
                    <input type="hidden" name="radius" value="{submitted_radius}">
                    <button type="submit" class="download-button">Download Summary CSV</button>
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
                # Ensure we are operating on a copy to avoid potential SettingWithCopyWarning
                # and ensure the filter is applied independently.
                subset_df = subset_df[subset_df['roost'] == 1].copy()
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
    
    # Store the summary table in a DataFrame for easy CSV export
    summary_df = pd.DataFrame(summary_table)
    # Reorder columns for display and CSV export (and thus for CSV download)
    display_summary_columns = [
        "Species",
        "All time nearest record",
        "Nearest record 2013 to 2023",
        "Nearest record 2018 to 2023",
        "All time nearest roost",
        "Nearest roost 2013 to 2023",
        "Nearest roost 2018 to 2023"
    ]
    # Ensure all desired columns exist before selecting
    final_summary_columns = [col for col in display_summary_columns if col in summary_df.columns]
    summary_df = summary_df[final_summary_columns]


    return {"counts": bat_counts, "summary_table": summary_table, "summary_df": summary_df}

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

    # Define custom sort orders for Taxa, Category, and Status
    taxa_order = ['Amphibian', 'Reptile', 'unknown']
    category_order = ['Threatened', 'At Risk', 'Not Threatened', 'Non-resident Native', 'Introduced and Naturalised', 'Extinct', 'unknown'] # Changed order for Extinct
    status_order = [
        'Nationally Critical', 'Nationally Endangered', 'Nationally Vulnerable',
        'Nationally Increasing', 'Declining', 'Relict', 'Uncommon', 'Recovering',
        'Migrant', 'Vagrant', 'Coloniser', 'Introduced and Naturalised', 'Extinct', 'unknown' # Changed order for Extinct
    ]

    for species_scientific in unique_species_in_radius:
        species_in_radius_df = radius_df[radius_df['scientific_name'] == species_scientific]
        
        common_name = species_in_radius_df['common_name'].iloc[0] if not species_in_radius_df.empty else ""
        
        total_count_for_species_in_radius = len(species_in_radius_df)

        # Get threat status information
        threat_info = threat_status_df[threat_status_df['Current Species Name'] == species_scientific]
        
        taxa_group = "unknown"
        threat_status_display = "unknown"
        category_for_sort = "unknown"
        status_for_sort = "unknown"

        if not threat_info.empty:
            threat_row = threat_info.iloc[0]
            taxa_group = threat_row['Taxa']
            category = threat_row['Category']
            status = threat_row['Status']
            
            # Special handling for Threat status display string
            if category == status:
                threat_status_display = category
            else:
                threat_status_display = f"{category} - {status}"
            
            category_for_sort = category # Use raw category for sorting
            status_for_sort = status # Use raw status for sorting

        # Ensure sightingty is filled with 'Undefined' before counting
        observation_types = species_in_radius_df['sightingty'].value_counts()
        
        # For display in HTML, still create a string with total at the end, WITH HTML breaks
        observation_type_html_summary = ""
        if observation_types.empty: 
             observation_type_html_summary = f"<b>Total</b> ({total_count_for_species_in_radius})"
        else:
            observation_type_parts = [f"{sighting_type} ({count})" for sighting_type, count in observation_types.items()]
            observation_type_html_summary = ", ".join(observation_type_parts)
            observation_type_html_summary += f"<br>&nbsp;<br><b>Total</b> ({total_count_for_species_in_radius})"

        # For CSV, create a clean string without HTML tags
        observation_type_csv_summary = ", ".join([f"{sighting_type} ({count})" for sighting_type, count in observation_types.items()])


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
            "taxa_group": taxa_group, # New column
            "threat_status_display": threat_status_display, # New column for display
            "category_for_sort": category_for_sort, # For sorting and coloring
            "status_for_sort": status_for_sort, # For sorting
            "observation_type_summary": observation_type_html_summary, # For HTML display
            "observation_type_csv_summary": observation_type_csv_summary, # For CSV export, without HTML
            "total_observations_for_species": total_count_for_species_in_radius, # For CSV export
            "most_recent_sighting": most_recent_sighting_str,
            "all_time": all_time_str,
            "past_5_years": past_5y_str,
            "past_10_years": past_10y_str
        })

    if not herp_results and unique_species_count == 0:
        return {"message": f"No herpetofauna records found within {radius_km} km.", "unique_species_count": 0}
    elif not herp_results and unique_species_count > 0:
         return {"message": f"No herpetofauna records found within {radius_km} km.", "unique_species_count": 0}
    
    # Convert herp_results to DataFrame for sorting and final processing
    summary_df = pd.DataFrame(herp_results)

    # Convert columns to CategoricalDtype for custom sorting
    summary_df['taxa_group'] = pd.Categorical(summary_df['taxa_group'], categories=taxa_order, ordered=True)
    summary_df['category_for_sort'] = pd.Categorical(summary_df['category_for_sort'], categories=category_order, ordered=True)
    summary_df['status_for_sort'] = pd.Categorical(summary_df['status_for_sort'], categories=status_order, ordered=True)

    # Apply sorting
    summary_df = summary_df.sort_values(
        by=['taxa_group', 'category_for_sort', 'status_for_sort', 'species'], # Added species for consistent sub-sorting
        ascending=True
    )
    # Reset index after sorting
    summary_df = summary_df.reset_index(drop=True)

    # The 'results' list for HTML display should reflect this sorting
    herp_results_sorted = summary_df.to_dict(orient='records')


    # Reorder columns for display (HTML table).
    # Note: 'category_for_sort' and 'status_for_sort' are internal for sorting/coloring, not for display
    # 'observation_type_csv_summary' and 'total_observations_for_species' are for CSV, not HTML table
    display_summary_columns = [
        "taxa_group",
        "species",
        "common_name",
        "threat_status_display",
        "observation_type_summary", # Use the HTML-formatted summary for display
        "most_recent_sighting",
        "all_time",
        "past_10_years", 
        "past_5_years"   
    ]
    # Filter columns to ensure they exist before selecting for the HTML display
    final_display_columns = [col for col in display_summary_columns if col in summary_df.columns]
    summary_df = summary_df[final_display_columns] # This df is primarily for consistent column existence check, results_sorted used for values


    return {"results": herp_results_sorted, "unique_species_count": unique_species_count, "summary_df": summary_df}

@app.route('/', methods=['GET', 'POST'])
def index():
    # Load data only if not already loaded or if there was an initial error
    # This prevents reloading on every POST request for data that should be static per app launch
    # and ensures error is displayed if data files are missing at startup.
    if initial_data_load_error:
        return render_html_page(error=initial_data_load_error)

    if bat_data_df is None or herp_data_df is None or threat_status_df is None:
        # This case should ideally be caught by initial_data_load_error, but as a fallback
        return render_html_page(error="Data files could not be loaded. Please ensure all required CSVs are in the same directory as the executable.")

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
        
        # Add combined Lat/Long column
        filtered_df['Lat_Long_Combined'] = filtered_df.apply(
            lambda row: f"{row['Latitude']}, {row['Longitude']}", axis=1
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
            "No bat species detected"
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
            'Lat_Long_Combined', # New combined column
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
            download_name=f'bat_data_occurrences_within_{radius_km}km.csv'
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

        # Add combined Lat/Long column
        filtered_df['Lat_Long_Combined'] = filtered_df.apply(
            lambda row: f"{row['Latitude']}, {row['Longitude']}", axis=1
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
            'Lat_Long_Combined', # New combined column
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
            download_name=f'herpetofauna_data_occurrences_within_{radius_km}km.csv'
        )
    except Exception as e:
        print(f"Error during herpetofauna data download: {e}")
        return f"An error occurred during herpetofauna data download: {e}", 500

@app.route('/download_bat_summary_data', methods=['POST'])
def download_bat_summary_data():
    if initial_data_load_error or bat_data_df is None:
        return "Error: Data not loaded. " + (initial_data_load_error or ""), 500
    
    try:
        coords_str = request.form['coords']
        lat_str, lon_str = coords_str.split(',')
        user_coords = (float(lat_str.strip()), float(lon_str.strip()))
        radius_km = int(request.form['radius'])

        # Recalculate bat results to get the summary_df
        bat_results = process_bat_data(bat_data_df.copy(), user_coords, radius_km)
        summary_df = bat_results.get('summary_df')

        if summary_df is None or summary_df.empty:
            return "No bat summary data available to download.", 404
        
        output = io.StringIO()
        summary_df.to_csv(output, index=False)
        output.seek(0)

        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=f'bat_summary_data_within_{radius_km}km.csv'
        )
    except Exception as e:
        print(f"Error during bat summary data download: {e}")
        return f"An error occurred during bat summary data download: {e}", 500

@app.route('/download_herp_summary_data', methods=['POST'])
def download_herp_summary_data():
    if initial_data_load_error or herp_data_df is None or threat_status_df is None:
        return "Error: Data not loaded. " + (initial_data_load_error or ""), 500
    
    try:
        coords_str = request.form['coords']
        lat_str, lon_str = coords_str.split(',')
        user_coords = (float(lat_str.strip()), float(lon_str.strip()))
        radius_km = int(request.form['radius'])

        # Recalculate herpetofauna results to get the structured data
        herp_processed_data = process_herpetofauna_data(herp_data_df.copy(), user_coords, radius_km)
        herp_results_list = herp_processed_data.get('results', [])

        if not herp_results_list:
            return "No herpetofauna summary data available to download.", 404
        
        # Prepare data for new DataFrame rows for CSV
        summary_data_for_df = []
        for item in herp_results_list:
            row = {
                "Taxa Group": item['taxa_group'], # New column for CSV
                "Species": item['species'],
                "Common Name": item['common_name'],
                "Threat Status": item['threat_status_display'], # New column for CSV
                "Observation Type Summary": item['observation_type_csv_summary'], 
                "Total Observations": item['total_observations_for_species'], 
                "Most recent sighting within {} km".format(radius_km): item['most_recent_sighting'],
                "Nearest Record (all time)": item['all_time'],
                "Nearest Record 2013 to 2023": item['past_10_years'],
                "Nearest Record 2018 to 2023": item['past_5_years']
            }
            summary_data_for_df.append(row)

        summary_df = pd.DataFrame(summary_data_for_df)

        # Define the final column order for the CSV
        final_csv_columns = [
            "Taxa Group", # First column in CSV
            "Species",
            "Common Name",
            "Threat Status", # After Common Name
            "Observation Type Summary",
            "Total Observations", 
            "Most recent sighting within {} km".format(radius_km),
            "Nearest Record (all time)",
            "Nearest Record 2013 to 2023",
            "Nearest Record 2018 to 2023"
        ]
        
        # Ensure all desired columns exist before selecting
        final_csv_columns = [col for col in final_csv_columns if col in summary_df.columns]
        summary_df = summary_df[final_csv_columns]

        output = io.StringIO()
        summary_df.to_csv(output, index=False)
        output.seek(0)

        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=f'herpetofauna_summary_data_within_{radius_km}km.csv'
        )
    except Exception as e:
        print(f"Error during herpetofauna summary data download: {e}")
        return f"An error occurred during herpetofauna summary data download: {e}", 500


def open_browser():
    """Opens the web browser to the application's URL."""
    webbrowser.open_new('http://127.0.0.1:5000/')

if __name__ == '__main__':
    Timer(1, open_browser).start()
    app.run(port=5000, debug=False)
