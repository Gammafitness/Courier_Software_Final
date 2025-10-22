"""
Flask-based courier recommendation application

This application demonstrates a simple implementation of the courier
recommendation logic described in the project requirements.  It reads
pincode data from Excel files stored under the `data/` directory (one
file per courier) and courier pricing rules from a JSON configuration
file (`couriers.json`).  When the user submits a pincode, weight and
declared value, the backend computes the shipping price for each
courier and renders the results in a table.

If you wish to fetch pincode data directly from Google Sheets instead
of local Excel files, you can integrate with the `gspread` library.
See the comments in `load_pincode_data` for guidance.

To run the app locally:

    pip install -r requirements.txt
    python app.py

Then open http://127.0.0.1:5000/ in your browser.

Note: This code is a minimal proof-of-concept.  In a production
environment you should add proper error handling, input validation,
authentication and security measures.
"""

import json
import os
from functools import lru_cache, wraps
from typing import Dict, Any, List

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    session,
    send_file,
)
from werkzeug.security import generate_password_hash, check_password_hash
import pandas as pd
import io

# Defer importing matplotlib until needed in download_results to avoid GUI backends
plt = None  # type: ignore
PdfPages = None  # type: ignore

try:
    # Optional import: if gspread is available, it can be used to fetch
    # data from Google Sheets instead of local Excel files.
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    gspread = None  # type: ignore

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "courier_secret_key")

# Path to user credentials file.  The file stores a JSON object of
# username to password hash.  Example:
# {"admin": "pbkdf2:sha256:260000$..."}
USERS_FILE = os.path.join(os.path.dirname(__file__), 'users.json')

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
COURIER_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'couriers.json')


def load_courier_config() -> Dict[str, Any]:
    """Load courier configuration from JSON file.

    The JSON structure looks like this:
    {
        "Bluedart": {
            "file": "Bluedart (3).xlsx",  # Excel file in data/ directory
            "sheet_name": "Bluedart",       # Excel sheet name
            "zone_rates": {
                "West": 30,
                "South": 25,
                ...
            },
            "docket_charges": 50,
            "fuel_surcharge_percent": 0.15,
            "fuel_surcharge_basis": "subtotal",  # or "freight"
            "insurance_percent": 0.10,
            "min_insurance_charges": 5,
            "oda_type": "fixed",  # or "case1", "case2" etc.
            "oda_fixed_charge": 100,
            "min_total_charges": 200,
            "green_charge": 20,
            "green_location": "Delhi",
            "gst_percent": 0.18
        },
        ...
    }
    """
    if not os.path.exists(COURIER_CONFIG_PATH):
        return {}
    with open(COURIER_CONFIG_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_users() -> Dict[str, str]:
    """Load users from the JSON credentials file."""
    if not os.path.exists(USERS_FILE):
        return {}
    with open(USERS_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


# -----------------------------------------------------------------------------
# ODA case rules
#
# Some couriers may use customised ODA (Out of Delivery Area) charge rules that
# depend on the weight or base freight.  The default implementation below
# provides examples for three case names.  You can modify these functions or
# add new entries to the ODA_CASE_RULES dictionary to suit your own pricing
# policy.  When adding or editing a courier via the admin interface, specify
# `oda_type` as "fixed" (for a constant charge defined by `oda_fixed_charge` in
# the courier config) or one of the keys defined here (e.g. "case1").  The
# selected function will be invoked with the base freight, shipment weight and
# ODA distance to compute the ODA charge.
#
def oda_case1(base_freight: float, weight: float, distance: float) -> float:
    """Example ODA case 1: charge 10 currency units per kg."""
    return 10.0 * weight


def oda_case2(base_freight: float, weight: float, distance: float) -> float:
    """Example ODA case 2: charge 20 currency units per kg."""
    return 20.0 * weight


def oda_case3(base_freight: float, weight: float, distance: float) -> float:
    """Example ODA case 3: charge 10% of base freight."""
    return 0.10 * base_freight


# Mapping of ODA type names to calculation functions.  Extend or modify as
# required for your business rules.
ODA_CASE_RULES = {
    'case1': oda_case1,
    'case2': oda_case2,
    'case3': oda_case3,
}

# Default ODA Case 1 matrix.  This matrix encodes Bluedart's ODA charges as
# published in the "Bluedart ODA Charges copy.xlsx" file.  It is used when
# `oda_case_file` is not provided for a courier.  Each sublist corresponds to
# a row from the Excel sheet: the first row contains the column headers
# (weight ranges) and the first column in subsequent rows contains the
# distance ranges.  The remaining values are the ODA charges.
DEFAULT_CASE1_MATRIX = [
    [
        "Distance in KMs",
        "0-100 Kgs",
        "101-250 Kgs",
        "251-500 Kgs",
        "501-1000 Kgs",
        "1001-1500 Kgs",
    ],
    [
        "20-50 Kms",
        550,
        990,
        1100,
        1375,
        1650,
    ],
    [
        "51-100 Kms",
        825,
        1210,
        1375,
        1650,
        1925,
    ],
    [
        "101-150 Kms",
        1100,
        1650,
        1925,
        2200,
        2750,
    ],
    [
        "151-200 Kms",
        1375,
        1925,
        2200,
        2475,
        3300,
    ],
    [
        "201-250 Kms",
        1650,
        2200,
        2750,
        3300,
        3960,
    ],
    [
        "251-300 Kms",
        1925,
        2500,
        3150,
        3800,
        4560,
    ],
    [
        "301-350 Kms",
        2200,
        2800,
        3550,
        4300,
        5160,
    ],
    [
        "351-400 Kms",
        2475,
        3100,
        3950,
        4800,
        5760,
    ],
    [
        "401-450 Kms",
        2750,
        3400,
        4350,
        5300,
        6360,
    ],
    [
        "451-500 Kms",
        3025,
        3700,
        4750,
        5800,
        6960,
    ],
]


# -----------------------------------------------------------------------------
# ODA matrix loader and case1 computation
#
# When a courier uses the ODA pricing rule "Case 1", we look up the charge
# based on the ODA distance (km) and weight (kg) using a rate matrix stored in
# an Excel file.  The configuration for a courier must include
# `oda_case_file` and optionally `oda_case_sheet`, specifying the filename in
# the `data/` directory and the sheet name.  The matrix is expected to have
# distance ranges in the first column (excluding the header row) and weight
# ranges in the first row.  Values in the cells represent the ODA charge.

from functools import lru_cache
import re


@lru_cache(maxsize=8)
def load_oda_matrix(filename: str, sheet_name: str | None = None) -> pd.DataFrame:
    """Load the ODA charge matrix from an Excel file.

    Parameters
    ----------
    filename : str
        Name of the Excel file located in the `data/` directory.
    sheet_name : str or None
        Name of the sheet to load.  If None, the first sheet is used.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing the matrix.  The first row must contain weight
        ranges and the first column distance ranges.
    """
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"ODA matrix file '{filename}' not found in data directory")
    xls = pd.ExcelFile(path)
    if sheet_name and sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)
    else:
        df = pd.read_excel(xls)
    return df


def parse_range(value: str) -> tuple[float, float] | None:
    """Parse a range string like '20-50 Kms' or '0-100 Kgs' into (low, high).

    Returns None if the pattern cannot be parsed.
    """
    if not isinstance(value, str):
        return None
    numbers = re.findall(r"\d+", value)
    if len(numbers) >= 2:
        low, high = float(numbers[0]), float(numbers[1])
        return low, high
    return None


def get_case1_charge(distance: float, weight: float, config: Dict[str, Any]) -> float:
    """Compute ODA charge for Case 1 using the matrix defined in config.

    Parameters
    ----------
    distance : float
        ODA distance in kilometres.
    weight : float
        Shipment weight in kilograms.
    config : dict
        Courier configuration containing `oda_case_file` and optionally
        `oda_case_sheet`.

    Returns
    -------
    float
        The ODA charge based on the matrix.  Returns 0.0 if the matrix or
        match cannot be found.
    """
    file = config.get('oda_case_file')
    sheet = config.get('oda_case_sheet')
    # Load matrix from file if provided, otherwise fall back to default matrix
    if file:
        try:
            matrix = load_oda_matrix(file, sheet)
        except Exception:
            # fall back to default if file cannot be loaded
            matrix = None
    else:
        matrix = None
    if matrix is None:
        # Use the built-in constant matrix without pandas
        # Determine column (weight) index
        weight_idx = None
        for j, header in enumerate(DEFAULT_CASE1_MATRIX[0][1:], start=0):
            rng = parse_range(str(header))
            if rng and rng[0] <= weight <= rng[1]:
                weight_idx = j  # index into values after distance label
                break
        # Determine row (distance) index
        distance_idx = None
        for i, row in enumerate(DEFAULT_CASE1_MATRIX[1:], start=0):
            rng = parse_range(str(row[0]))
            if rng and rng[0] <= distance <= rng[1]:
                distance_idx = i
                break
        if weight_idx is not None and distance_idx is not None:
            try:
                # value is row[weight_idx + 1] because first element is distance label
                value = DEFAULT_CASE1_MATRIX[1 + distance_idx][1 + weight_idx]
                return float(value)
            except Exception:
                return 0.0
        return 0.0
    # The first row (index 0) should contain weight ranges starting from
    # column index 1.  The first column (index 0) of subsequent rows should
    # contain distance ranges.
    # Determine the row index for distance
    row_index = None
    for i in range(1, len(matrix)):
        rng = parse_range(str(matrix.iloc[i, 0]))
        if rng and rng[0] <= distance <= rng[1]:
            row_index = i
            break
    # Determine the column index for weight
    col_index = None
    if not matrix.empty:
        for j in range(1, matrix.shape[1]):
            rng = parse_range(str(matrix.iloc[0, j]))
            if rng and rng[0] <= weight <= rng[1]:
                col_index = j
                break
    if row_index is not None and col_index is not None:
        try:
            value = matrix.iloc[row_index, col_index]
            if pd.isna(value):
                return 0.0
            return float(value)
        except Exception:
            return 0.0
    return 0.0


def save_users(users: Dict[str, str]) -> None:
    """Save users to the credentials file."""
    with open(USERS_FILE, 'w', encoding='utf-8') as f:
        json.dump(users, f, indent=2)


def login_required(view_func):
    """Decorator to enforce login on protected routes."""
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if 'username' not in session:
            return redirect(url_for('login', next=request.url))
        return view_func(*args, **kwargs)
    return wrapper


@lru_cache(maxsize=16)
def load_pincode_data(filename: str, sheet_name: str) -> pd.DataFrame:
    """Load pincode data from an Excel file or Google Sheet.

    Parameters
    ----------
    filename : str
        The file name within the data directory, or a Google Sheet ID if
        using gspread.
    sheet_name : str
        The sheet (tab) name in the workbook.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns: Pincode, Location, State, Status,
        ODA Distance (optional), Zone

    Notes
    -----
    If `gspread` is installed and the `filename` does not have a file
    extension, the function assumes it refers to a Google Sheet ID.
    In that case you must set the environment variable
    `GOOGLE_SERVICE_ACCOUNT_FILE` to the path of your service account
    credentials JSON.  The `sheet_name` must match the tab name.
    """
    # Determine whether to read from local Excel or Google Sheet
    is_excel = any(filename.lower().endswith(ext) for ext in ['.xlsx', '.xls'])
    if is_excel or gspread is None:
        # Local Excel file
        path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Pincode file '{filename}' not found in data directory")
        xls = pd.ExcelFile(path)
        if sheet_name not in xls.sheet_names:
            raise ValueError(f"Sheet '{sheet_name}' not found in '{filename}'")
        df = pd.read_excel(xls, sheet_name=sheet_name)
        return df
    else:
        # Google Sheet (requires gspread and credentials)
        credentials_path = os.environ.get('GOOGLE_SERVICE_ACCOUNT_FILE')
        if not credentials_path:
            raise EnvironmentError(
                "GOOGLE_SERVICE_ACCOUNT_FILE environment variable must be set "
                "to the service account credentials JSON to load Google Sheets data"
            )
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        credentials = Credentials.from_service_account_file(credentials_path, scopes=scopes)
        gc = gspread.authorize(credentials)
        sh = gc.open_by_key(filename)
        worksheet = sh.worksheet(sheet_name)
        records = worksheet.get_all_records()
        df = pd.DataFrame(records)
        return df


def calculate_charges(df: pd.DataFrame, config: Dict[str, Any], pincode: str, weight: float, value: float) -> Dict[str, Any]:
    """Calculate courier charges for a given pincode, weight and value."""
    # Attempt to convert pincode to integer for matching if numeric
    try:
        pincode_int = int(pincode)
    except ValueError:
        pincode_int = None

    # Look up pincode row
    row = None
    if pincode_int is not None:
        match = df[df['Pincode'] == pincode_int]
        if not match.empty:
            row = match.iloc[0]
    if row is None:
        match = df[df['Pincode'].astype(str) == str(pincode)]
        if not match.empty:
            row = match.iloc[0]
    if row is None:
        return {'status': 'Not serviceable'}

    status = str(row['Status']).strip()
    location = str(row['Location']).strip()
    state = str(row['State']).strip()
    zone = str(row['Zone']).strip()
    # Fetch ODA distance if present.  If the column is missing or value is NaN,
    # default to 0.0
    # Determine ODA distance column.  Many sheets may label it differently, so
    # match any column containing the words 'oda' and 'distance'.
    oda_distance_col = None
    for col in row.index:
        col_name = str(col).strip().lower()
        if 'oda' in col_name and 'distance' in col_name:
            oda_distance_col = col
            break
    if oda_distance_col is not None:
        try:
            value_obj = row[oda_distance_col]
            oda_distance = float(value_obj) if value_obj not in [None, ''] and not pd.isna(value_obj) else 0.0
        except Exception:
            oda_distance = 0.0
    else:
        oda_distance = 0.0

    # Fetch zone rate
    zone_rate = config['zone_rates'].get(zone)
    if zone_rate is None:
        return {'status': f"Zone '{zone}' not configured"}
    base_freight = zone_rate * weight

    docket = config['docket_charges']
    insurance = max(value * config['insurance_percent'], config['min_insurance_charges'])
    # Apply green tax based on zone (not location), if the courier config specifies a green location/zone
    green_cfg = config.get('green_location', '')
    if green_cfg:
        # Compare zone names case-insensitively
        green_tax = config['green_charge'] if zone.lower() == green_cfg.strip().lower() else 0
    else:
        green_tax = 0

    # ODA charges.  If an ODA distance is recorded (>0), compute the
    # corresponding charge based on the courier's `oda_type` setting.  A value
    # of zero distance implies no ODA surcharge.  The shipment remains
    # serviceable even if an ODA surcharge applies.
    oda_charge = 0.0
    # If the destination is an ODA location (status 'ODA') or has a distance > 0, compute ODA charges
    if oda_distance > 0 or status.lower() == 'oda':
        oda_type = str(config.get('oda_type', 'fixed')).lower().replace(' ', '')
        if oda_type == 'fixed':
            oda_charge = float(config.get('oda_fixed_charge', 0))
        elif oda_type == 'case1':
            # Case 1: use matrix lookup based on distance and weight
            oda_charge = get_case1_charge(oda_distance, weight, config)
        elif oda_type == 'rate_per_km':
            # Rate-per-km: calculate ODA charge based on rate * weight, but enforce a minimum charge if provided.
            rate = float(config.get('oda_rate_per_km', 0))
            min_rate = float(config.get('oda_min_rate_per_km', 0))
            calculated = rate * float(weight)
            oda_charge = max(calculated, min_rate)
        else:
            # Attempt to use a custom rule defined in ODA_CASE_RULES
            rule = ODA_CASE_RULES.get(oda_type)
            if rule:
                oda_charge = rule(base_freight, weight, oda_distance)

    subtotal_before_fuel = base_freight + docket + insurance + green_tax + oda_charge

    if config['fuel_surcharge_basis'] == 'freight':
        fuel = base_freight * config['fuel_surcharge_percent']
    else:
        fuel = subtotal_before_fuel * config['fuel_surcharge_percent']

    subtotal = subtotal_before_fuel + fuel
    if subtotal < config['min_total_charges']:
        subtotal = config['min_total_charges']

    gst_amount = subtotal * config['gst_percent']
    total = subtotal + gst_amount

    return {
        'status': status,
        'location': location,
        'state': state,
        'zone': zone,
        'zone_rate': zone_rate,
        'base_freight': base_freight,
        'docket': docket,
        'insurance': insurance,
        'green_tax': green_tax,
        'oda_charge': oda_charge,
        'fuel': fuel,
        'subtotal': subtotal,
        'gst': gst_amount,
        'total': total,
        'oda_distance': oda_distance,
    }


@app.route('/', methods=['GET', 'POST'])
@login_required
def index():
    couriers = load_courier_config()
    result_rows: List[Dict[str, Any]] = []
    # Prepare variables to repopulate the form after submission
    pincode_input = ''
    weight_input = ''
    value_input = ''

    if request.method == 'POST':
        # Read form fields
        pincode_input = request.form.get('pincode', '').strip()
        weight_input = request.form.get('weight', '').strip()
        value_input = request.form.get('value', '').strip()

        # Check for uploaded Excel file
        excel_file = request.files.get('excel_file')
        if excel_file and excel_file.filename:
            # Try to parse the uploaded file
            try:
                df_uploaded = pd.read_excel(excel_file)
            except Exception as exc:
                flash(f'Failed to read uploaded Excel file: {exc}', 'danger')
                return render_template('index.html', couriers=couriers.keys(), results=[],
                                       pincode_input=pincode_input,
                                       weight_input=weight_input,
                                       value_input=value_input)
            # Normalize column names to lower for matching
            cols = {c.lower().strip(): c for c in df_uploaded.columns}
            # Identify required columns
            pin_col = None
            weight_col = None
            value_col = None
            for key in cols:
                if 'pincode' in key or 'pin code' in key:
                    pin_col = cols[key]
                elif 'weight' in key:
                    weight_col = cols[key]
                elif 'value' in key:
                    value_col = cols[key]
            if not pin_col or not weight_col:
                flash('Uploaded Excel must contain Pincode and Weight columns.', 'danger')
                return render_template('index.html', couriers=couriers.keys(), results=[],
                                       pincode_input=pincode_input,
                                       weight_input=weight_input,
                                       value_input=value_input)
            pincode_list = df_uploaded[pin_col].astype(str).str.strip().tolist()
            weight_list = []
            for val in df_uploaded[weight_col]:
                try:
                    weight_list.append(float(val))
                except Exception:
                    weight_list.append(0.0)
            # Values are optional; if missing, default to 0
            value_list = []
            if value_col:
                for val in df_uploaded[value_col]:
                    try:
                        value_list.append(float(val))
                    except Exception:
                        value_list.append(0.0)
            else:
                value_list = [0.0] * len(pincode_list)
            # Set input fields for user feedback
            pincode_input = ','.join(pincode_list)
            weight_input = ','.join([str(w) for w in weight_list])
            value_input = ','.join([str(v) for v in value_list]) if any(value_list) else ''
        else:
            # No file uploaded: parse comma-separated lists
            # Split by comma; empty entries are ignored
            pincode_list = [p.strip() for p in pincode_input.split(',') if p.strip()]
            weight_list_raw = [w.strip() for w in weight_input.split(',') if w.strip()]
            value_list_raw = [v.strip() for v in value_input.split(',') if v.strip()]
            # Convert weights/values to floats; allow blanks to be reused
            try:
                weight_list = [float(w) for w in weight_list_raw]
            except ValueError:
                flash('Weight must be numeric (use comma to separate multiple values)', 'danger')
                return render_template('index.html', couriers=couriers.keys(), results=[],
                                       pincode_input=pincode_input,
                                       weight_input=weight_input,
                                       value_input=value_input)
            # Convert values to floats; if no values provided, default to zero for all
            value_list = []
            if value_list_raw:
                try:
                    value_list = [float(v) for v in value_list_raw]
                except ValueError:
                    flash('Value of shipment must be numeric (use comma to separate multiple values)', 'danger')
                    return render_template('index.html', couriers=couriers.keys(), results=[],
                                           pincode_input=pincode_input,
                                           weight_input=weight_input,
                                           value_input=value_input)
            # If values list is empty, treat value as 0 for all shipments
            if not value_list:
                value_list = [0.0]
            # Extend weight_list and value_list to match length of pincode_list
            if not weight_list:
                flash('Weight is required', 'danger')
                return render_template('index.html', couriers=couriers.keys(), results=[],
                                       pincode_input=pincode_input,
                                       weight_input=weight_input,
                                       value_input=value_input)
            while len(weight_list) < len(pincode_list):
                weight_list.append(weight_list[-1])
            while len(value_list) < len(pincode_list):
                value_list.append(value_list[-1])

        # Ensure there is at least one pincode and weight.  If not, show error.
        if not pincode_list:
            flash('Please enter at least one pincode or upload an Excel file.', 'danger')
            return render_template('index.html', couriers=couriers.keys(), results=[],
                                   pincode_input=pincode_input,
                                   weight_input=weight_input,
                                   value_input=value_input)
        if not weight_list:
            flash('Please enter weight(s) corresponding to the pincodes or upload an Excel file.', 'danger')
            return render_template('index.html', couriers=couriers.keys(), results=[],
                                   pincode_input=pincode_input,
                                   weight_input=weight_input,
                                   value_input=value_input)

        # Build detailed results for each pincode separately
        results_by_pin: List[Dict[str, Any]] = []
        # Preload pincode dataframes per courier once
        courier_dfs: Dict[str, Any] = {}
        for cname, config in couriers.items():
            try:
                courier_dfs[cname] = load_pincode_data(config['file'], config['sheet_name'])
            except Exception as exc:
                courier_dfs[cname] = f"Error loading data: {exc}"
        # Iterate over each pincode and compute charges per courier
        for idx, pin in enumerate(pincode_list):
            w = weight_list[idx]
            v = value_list[idx] if idx < len(value_list) else 0.0
            courier_results = []
            for cname, config in couriers.items():
                df_or_err = courier_dfs.get(cname)
                if isinstance(df_or_err, str):
                    courier_results.append({'courier': cname, 'status': df_or_err})
                    continue
                df = df_or_err
                res = calculate_charges(df, config, pin, w, v)
                status_lower = str(res.get('status', '')).lower()
                # Always include row; if not serviceable, show status only
                if status_lower not in ['serviceable', 'oda']:
                    courier_results.append({
                        'courier': cname,
                        'status': res.get('status', 'Not serviceable')
                    })
                else:
                    courier_results.append({
                        'courier': cname,
                        'status': res.get('status'),
                        'location': res.get('location'),
                        'state': res.get('state'),
                        'zone': res.get('zone'),
                        'zone_rate': res.get('zone_rate'),
                        'oda_distance': res.get('oda_distance'),
                        'base_freight': res.get('base_freight'),
                        'docket': res.get('docket'),
                        'insurance': res.get('insurance'),
                        'green_tax': res.get('green_tax'),
                        'oda_charge': res.get('oda_charge'),
                        'fuel': res.get('fuel'),
                        'subtotal': res.get('subtotal'),
                        'gst': res.get('gst'),
                        'total': res.get('total'),
                    })
            results_by_pin.append({
                'pincode': pin,
                'weight': w,
                'value': v,
                'couriers': courier_results
            })
        # Store results in session for download
        session['latest_results'] = results_by_pin
        return render_template('index.html', couriers=couriers.keys(),
                               results=results_by_pin,
                               pincode_input=pincode_input,
                               weight_input=weight_input,
                               value_input=value_input)
    # GET request
    return render_template('index.html', couriers=couriers.keys(), results=[],
                           pincode_input=pincode_input,
                           weight_input=weight_input,
                           value_input=value_input)


@app.route('/admin/add_courier', methods=['GET', 'POST'])
@login_required
def add_courier():
    couriers = load_courier_config()
    if request.method == 'POST':
        data = request.form.to_dict()
        name = data.get('name', '').strip()
        if not name:
            flash('Courier name is required', 'danger')
            return render_template('add_courier.html')
        if name in couriers:
            flash('Courier name already exists', 'danger')
            return render_template('add_courier.html')
        try:
            zone_rates = json.loads(data.get('zone_rates', '{}'))
        except json.JSONDecodeError as exc:
            flash(f'Invalid JSON for zone rates: {exc}', 'danger')
            return render_template('add_courier.html')
        # Convert numeric fields, treating empty strings as 0
        def to_float(val: str) -> float:
            return float(val) if val and str(val).strip() else 0.0
        new_config = {
            'file': data.get('file', '').strip(),
            'sheet_name': data.get('sheet_name', '').strip(),
            'zone_rates': zone_rates,
            'docket_charges': to_float(data.get('docket_charges', '0')),
            'fuel_surcharge_percent': to_float(data.get('fuel_surcharge_percent', '0')),
            'fuel_surcharge_basis': data.get('fuel_surcharge_basis', 'freight'),
            'insurance_percent': to_float(data.get('insurance_percent', '0')),
            'min_insurance_charges': to_float(data.get('min_insurance_charges', '0')),
            'oda_type': data.get('oda_type', 'fixed'),
            'oda_fixed_charge': to_float(data.get('oda_fixed_charge', '0')),
            'min_total_charges': to_float(data.get('min_total_charges', '0')),
            'green_charge': to_float(data.get('green_charge', '0')),
            'green_location': data.get('green_location', ''),
            'gst_percent': to_float(data.get('gst_percent', '0.18')),
            'oda_case_file': data.get('oda_case_file', '').strip(),
            'oda_case_sheet': data.get('oda_case_sheet', '').strip(),
            'oda_rate_per_km': to_float(data.get('oda_rate_per_km', '0')),
            # Minimum ODA rate per km charge when using rate_per_km
            'oda_min_rate_per_km': to_float(data.get('oda_min_rate_per_km', '0')),
        }
        couriers[name] = new_config
        with open(COURIER_CONFIG_PATH, 'w', encoding='utf-8') as f:
            json.dump(couriers, f, indent=2)
        flash(f'Courier {name} added successfully', 'success')
        return redirect(url_for('index'))
    return render_template('add_courier.html')


@app.route('/admin/manage_courier')
@login_required
def manage_courier():
    """Display a list of couriers with options to edit or delete."""
    couriers = load_courier_config()
    return render_template('manage_courier.html', couriers=couriers)


@app.route('/admin/delete_courier/<name>', methods=['POST'])
@login_required
def delete_courier(name: str):
    couriers = load_courier_config()
    if name in couriers:
        couriers.pop(name)
        with open(COURIER_CONFIG_PATH, 'w', encoding='utf-8') as f:
            json.dump(couriers, f, indent=2)
        flash(f'Courier {name} deleted', 'success')
    else:
        flash(f'Courier {name} not found', 'danger')
    return redirect(url_for('manage_courier'))


@app.route('/admin/edit_courier/<name>', methods=['GET', 'POST'])
@login_required
def edit_courier(name: str):
    """Edit an existing courier's configuration."""
    couriers = load_courier_config()
    if name not in couriers:
        flash(f'Courier {name} not found', 'danger')
        return redirect(url_for('manage_courier'))
    if request.method == 'POST':
        data = request.form.to_dict()
        try:
            zone_rates = json.loads(data.get('zone_rates', '{}'))
        except json.JSONDecodeError as exc:
            flash(f'Invalid JSON for zone rates: {exc}', 'danger')
            return render_template('edit_courier.html', name=name, config=couriers[name])
        def to_float(val: str) -> float:
            return float(val) if val and str(val).strip() else 0.0
        couriers[name] = {
            'file': data.get('file', '').strip(),
            'sheet_name': data.get('sheet_name', '').strip(),
            'zone_rates': zone_rates,
            'docket_charges': to_float(data.get('docket_charges', '0')),
            'fuel_surcharge_percent': to_float(data.get('fuel_surcharge_percent', '0')),
            'fuel_surcharge_basis': data.get('fuel_surcharge_basis', 'freight'),
            'insurance_percent': to_float(data.get('insurance_percent', '0')),
            'min_insurance_charges': to_float(data.get('min_insurance_charges', '0')),
            'oda_type': data.get('oda_type', 'fixed'),
            'oda_fixed_charge': to_float(data.get('oda_fixed_charge', '0')),
            'min_total_charges': to_float(data.get('min_total_charges', '0')),
            'green_charge': to_float(data.get('green_charge', '0')),
            'green_location': data.get('green_location', ''),
            'gst_percent': to_float(data.get('gst_percent', '0.18')),
            'oda_case_file': data.get('oda_case_file', '').strip(),
            'oda_case_sheet': data.get('oda_case_sheet', '').strip(),
            'oda_rate_per_km': to_float(data.get('oda_rate_per_km', '0')),
            'oda_min_rate_per_km': to_float(data.get('oda_min_rate_per_km', '0')),
        }
        with open(COURIER_CONFIG_PATH, 'w', encoding='utf-8') as f:
            json.dump(couriers, f, indent=2)
        flash(f'Courier {name} updated successfully', 'success')
        return redirect(url_for('manage_courier'))
    # GET: prefill form with existing config
    return render_template('edit_courier.html', name=name, config=couriers[name])


@app.route('/login', methods=['GET', 'POST'])
def login():
    """User login page."""
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        users = load_users()
        if username in users:
            stored = users[username]
            # Ensure stored value is a string and trim whitespace
            stored_str = str(stored).strip()
            # If the stored password looks like a pbkdf2 hash, verify via check_password_hash.
            if stored_str.startswith('pbkdf2:'):
                valid = check_password_hash(stored_str, password)
            else:
                # Otherwise compare plain text (not recommended for production).
                valid = stored_str == password
            if valid:
                session['username'] = username
                flash('Logged in successfully', 'success')
                next_url = request.args.get('next') or url_for('index')
                return redirect(next_url)
        flash('Invalid username or password', 'danger')
    return render_template('login.html')


@app.route('/logout')
def logout():
    """Log the user out by clearing the session."""
    session.pop('username', None)
    flash('Logged out', 'success')
    return redirect(url_for('index'))


@app.route('/change_password', methods=['GET', 'POST'])
@login_required
def change_password():
    """Allow logged-in user to change their password."""
    if request.method == 'POST':
        current_password = request.form.get('current_password', '').strip()
        new_password = request.form.get('new_password', '').strip()
        confirm_password = request.form.get('confirm_password', '').strip()
        username = session.get('username')
        users = load_users()
        if not username or username not in users:
            flash('User not found', 'danger')
            return redirect(url_for('login'))
        stored_pw = users[username]
        # Check current password
        if isinstance(stored_pw, str) and stored_pw.startswith('pbkdf2:'):
            valid_current = check_password_hash(stored_pw, current_password)
        else:
            valid_current = stored_pw == current_password
        if not valid_current:
            flash('Current password is incorrect', 'danger')
        elif new_password != confirm_password or not new_password:
            flash('New passwords do not match or are empty', 'danger')
        else:
            # Store new password in plain text for simplicity (hashed passwords are still recognised for existing users)
            users[username] = new_password
            save_users(users)
            flash('Password changed successfully', 'success')
            return redirect(url_for('index'))
    return render_template('change_password.html')


@app.route('/download_results/<fmt>')
@login_required
def download_results(fmt: str):
    """
    Download the latest computed results as an Excel or PDF file.

    The results are stored in the session under ``latest_results``.  If no
    results are available, the user is redirected back to the dashboard with
    a flash message.  Supported formats are ``excel`` and ``pdf``.  If PDF
    generation fails or matplotlib is not available, the data is served as
    Excel.
    """
    results_by_pin = session.get('latest_results')
    if not results_by_pin:
        flash('No results available for download.', 'warning')
        return redirect(url_for('index'))
    # Flatten results into a list of rows
    rows: List[Dict[str, Any]] = []
    for group in results_by_pin:
        pincode = group.get('pincode')
        weight = group.get('weight')
        value = group.get('value')
        for r in group.get('couriers', []):
            row = {
                'Pincode': pincode,
                'Weight': weight,
                'Value': value,
                'Courier': r.get('courier'),
                'Status': r.get('status'),
            }
            status_lower = str(r.get('status', '')).lower()
            if status_lower in ['serviceable', 'oda']:
                # Add detailed fields
                row.update({
                    'Location': r.get('location'),
                    'State': r.get('state'),
                    'Zone': r.get('zone'),
                    'Zone Rate': r.get('zone_rate'),
                    'ODA Distance': r.get('oda_distance'),
                    'Base Freight': r.get('base_freight'),
                    'Docket Charges': r.get('docket'),
                    'Insurance': r.get('insurance'),
                    'Green Tax': r.get('green_tax'),
                    'ODA Charges': r.get('oda_charge'),
                    'Fuel Surcharge': r.get('fuel'),
                    'Sub Total': r.get('subtotal'),
                    'GST': r.get('gst'),
                    'Total': r.get('total'),
                })
            rows.append(row)
    df = pd.DataFrame(rows)
    # Generate Excel
    if fmt.lower() == 'excel':
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='results.xlsx'
        )
    # Generate PDF if requested
    if fmt.lower() == 'pdf':
        try:
            # Import matplotlib with a non-GUI backend to avoid GUI issues
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as _plt  # type: ignore
            from matplotlib.backends.backend_pdf import PdfPages as _PdfPages  # type: ignore
            # Determine figure size based on number of rows
            n_rows = len(df)
            fig_height = max(1.0, 0.5 + 0.3 * n_rows)
            fig, ax = _plt.subplots(figsize=(12, fig_height))
            ax.axis('tight')
            ax.axis('off')
            table = ax.table(
                cellText=df.values,
                colLabels=df.columns,
                loc='center',
                cellLoc='center'
            )
            table.auto_set_font_size(False)
            table.set_fontsize(8)
            table.auto_set_column_width(col=list(range(len(df.columns))))
            pdf_bytes = io.BytesIO()
            with _PdfPages(pdf_bytes) as pdf:
                pdf.savefig(fig, bbox_inches='tight')
            pdf_bytes.seek(0)
            _plt.close(fig)
            return send_file(
                pdf_bytes,
                mimetype='application/pdf',
                as_attachment=True,
                download_name='results.pdf'
            )
        except Exception:
            # On failure, fall through to Excel
            pass
    # Fallback: serve as Excel
    flash('PDF generation failed or unsupported. Downloading Excel instead.', 'warning')
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name='results.xlsx'
    )


# -----------------------------------------------------------------------------
# Download template route
# -----------------------------------------------------------------------------

@app.route('/download_template')
@login_required
def download_template():
    """
    Serve a blank Excel template with columns ``Pincode``, ``Weight`` and ``Value``.
    Users can fill this template and upload it to perform bulk pincode lookups.
    """
    # Create a simple DataFrame with headers only
    df_template = pd.DataFrame({
        'Pincode': [''],
        'Weight': [''],
        'Value': [''],
    })
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df_template.to_excel(writer, index=False)
    output.seek(0)
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name='pincode_template.xlsx'
    )


if __name__ == '__main__':
    # Run the Flask app on port 5050 instead of the default 5000.
    app.run(debug=True, port=5050)