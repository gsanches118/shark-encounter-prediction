from fpdf import FPDF
import pandas as pd
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))


class SharkReportPDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 16)
        self.cell(0, 10, "Shark Encounter Risk Report", new_x="LMARGIN", new_y="NEXT", align="C")
        self.set_font("Helvetica", "", 10)
        self.cell(0, 6, f"Generated: {datetime.now().strftime('%d %B %Y')}", new_x="LMARGIN", new_y="NEXT", align="C")
        self.cell(0, 6, "Australian Coastline", new_x="LMARGIN", new_y="NEXT", align="C")
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")


def _default_risk_data():
    """Default risk data used when the pipeline has not been run yet."""
    return pd.DataFrame([
        {"location_name": "Ballina", "state": "New South Wales", "score_date": "2026-04-15", "risk_score": 72, "risk_level": "High", "recent_sightings_7d": 3, "avg_rainfall_7d_mm": 54.2, "is_whale_season": False, "time_of_day_risk": "dawn", "historical_incident_count": 24},
        {"location_name": "Byron Bay", "state": "New South Wales", "score_date": "2026-04-15", "risk_score": 55, "risk_level": "High", "recent_sightings_7d": 2, "avg_rainfall_7d_mm": 48.7, "is_whale_season": False, "time_of_day_risk": "dawn", "historical_incident_count": 18},
        {"location_name": "Port Lincoln", "state": "South Australia", "score_date": "2026-04-15", "risk_score": 47, "risk_level": "Medium", "recent_sightings_7d": 1, "avg_rainfall_7d_mm": 12.3, "is_whale_season": False, "time_of_day_risk": "midday", "historical_incident_count": 22},
        {"location_name": "Margaret River", "state": "Western Australia", "score_date": "2026-04-15", "risk_score": 42, "risk_level": "Medium", "recent_sightings_7d": 1, "avg_rainfall_7d_mm": 31.0, "is_whale_season": False, "time_of_day_risk": "dusk", "historical_incident_count": 14},
        {"location_name": "Gold Coast", "state": "Queensland", "score_date": "2026-04-15", "risk_score": 38, "risk_level": "Medium", "recent_sightings_7d": 1, "avg_rainfall_7d_mm": 22.5, "is_whale_season": False, "time_of_day_risk": "midday", "historical_incident_count": 11},
        {"location_name": "Coffs Harbour", "state": "New South Wales", "score_date": "2026-04-15", "risk_score": 30, "risk_level": "Medium", "recent_sightings_7d": 0, "avg_rainfall_7d_mm": 55.1, "is_whale_season": False, "time_of_day_risk": "midday", "historical_incident_count": 9},
        {"location_name": "Bondi Beach", "state": "New South Wales", "score_date": "2026-04-15", "risk_score": 20, "risk_level": "Low", "recent_sightings_7d": 0, "avg_rainfall_7d_mm": 8.4, "is_whale_season": False, "time_of_day_risk": "midday", "historical_incident_count": 7},
        {"location_name": "Noosa", "state": "Queensland", "score_date": "2026-04-15", "risk_score": 18, "risk_level": "Low", "recent_sightings_7d": 0, "avg_rainfall_7d_mm": 5.2, "is_whale_season": False, "time_of_day_risk": "midday", "historical_incident_count": 4},
        {"location_name": "Manly Beach", "state": "New South Wales", "score_date": "2026-04-15", "risk_score": 12, "risk_level": "Low", "recent_sightings_7d": 0, "avg_rainfall_7d_mm": 7.1, "is_whale_season": False, "time_of_day_risk": "midday", "historical_incident_count": 3},
        {"location_name": "Torquay", "state": "Victoria", "score_date": "2026-04-15", "risk_score": 8, "risk_level": "Low", "recent_sightings_7d": 0, "avg_rainfall_7d_mm": 3.8, "is_whale_season": False, "time_of_day_risk": "midday", "historical_incident_count": 2},
    ])


def generate_report():
    csv_path = os.path.join(os.path.dirname(__file__), "..", "data", "processed", "risk_scores.csv")

    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
    else:
        print("No processed risk scores found, using defaults")
        df = _default_risk_data()

    pdf = SharkReportPDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=15)

    # -- page 1: overview & methodology --
    pdf.add_page()

    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "1. Overview", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)
    pdf.multi_cell(0, 5,
        "This report presents a daily shark encounter risk assessment for popular beach "
        "locations across Australia. The model combines historical incident data from the "
        "Australian Shark-Incident Database, real-time sighting reports from the Dorsal app, "
        "and environmental factors including rainfall and seasonal whale migration patterns."
    )
    pdf.ln(5)

    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "2. Methodology", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)

    pdf.multi_cell(0, 5,
        "Risk scores are calculated using a weighted formula (0-100 scale) based on:\n\n"
        "  - Recent sightings (last 7 days): up to 40 points\n"
        "      Strongest predictor. Each confirmed sighting adds 15 points.\n\n"
        "  - Historical incident density: up to 20 points\n"
        "      Locations with >20 recorded incidents score highest.\n\n"
        "  - Rainfall (7-day average): up to 15 points\n"
        "      Heavy rain (>50mm avg) washes nutrients into the ocean,\n"
        "      attracting baitfish and consequently sharks.\n\n"
        "  - Whale migration season (May-Nov): 10 points\n"
        "      Large sharks follow whale migration routes along the east coast.\n\n"
        "  - Time of day: up to 10 points\n"
        "      Dawn and dusk periods carry higher risk due to reduced\n"
        "      visibility and shark feeding patterns."
    )
    pdf.ln(3)

    pdf.set_font("Helvetica", "", 10)
    pdf.multi_cell(0, 5,
        "Classification thresholds:\n"
        "  - Low: score < 25\n"
        "  - Medium: score 25-49\n"
        "  - High: score >= 50"
    )
    pdf.ln(5)

    # -- page 2: variables detail --
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "3. Variables & Data Sources", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)

    variables = [
        ("Variable", "Source", "Update Frequency"),
        ("Shark incidents (historical)", "Australian Shark-Incident Database (Taronga)", "Quarterly"),
        ("Shark sightings (real-time)", "Dorsal Watch app (community + tags)", "Daily"),
        ("Rainfall (7-day)", "Bureau of Meteorology (BOM)", "Daily"),
        ("Season / whale migration", "Calendar-based (May-November)", "Static"),
        ("Time of day", "System clock at scoring time", "Real-time"),
        ("Beach location coords", "Curated list of popular beaches", "Static"),
    ]

    col_widths = [65, 80, 40]
    pdf.set_font("Helvetica", "B", 9)
    for i, header in enumerate(variables[0]):
        pdf.cell(col_widths[i], 7, header, border=1)
    pdf.ln()

    pdf.set_font("Helvetica", "", 9)
    for row in variables[1:]:
        for i, val in enumerate(row):
            pdf.cell(col_widths[i], 7, val, border=1)
        pdf.ln()

    pdf.ln(8)

    # -- results table --
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "4. Current Risk Scores", new_x="LMARGIN", new_y="NEXT")

    headers = ["Location", "State", "Score", "Level", "Sightings 7d", "Rain 7d mm"]
    widths = [35, 35, 18, 18, 28, 25]

    pdf.set_font("Helvetica", "B", 8)
    for i, h in enumerate(headers):
        pdf.cell(widths[i], 7, h, border=1)
    pdf.ln()

    pdf.set_font("Helvetica", "", 8)
    for _, row in df.iterrows():
        level = str(row.get("risk_level", ""))
        if level == "High":
            pdf.set_fill_color(255, 200, 200)
        elif level == "Medium":
            pdf.set_fill_color(255, 240, 200)
        else:
            pdf.set_fill_color(200, 255, 200)

        vals = [
            str(row.get("location_name", "")),
            str(row.get("state", ""))[:18],
            str(int(row.get("risk_score", 0))),
            level,
            str(int(row.get("recent_sightings_7d", 0))),
            str(round(row.get("avg_rainfall_7d_mm", 0), 1)),
        ]
        for i, v in enumerate(vals):
            pdf.cell(widths[i], 7, v, border=1, fill=True)
        pdf.ln()

    # -- page 3: potential integrations --
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "5. Potential Integrations", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)

    pdf.multi_cell(0, 5,
        "The risk scoring output is designed as an API-ready dataset that could integrate "
        "with existing platforms to provide real-time alerts to ocean users:"
    )
    pdf.ln(3)

    integrations = [
        (
            "Surfline",
            "Display shark risk level alongside surf conditions. A coloured badge "
            "(green/amber/red) would appear on the beach forecast page, giving surfers "
            "a heads-up before paddling out."
        ),
        (
            "Gold Coast City Council",
            "Feed risk levels into the council's beach safety dashboard. When a location "
            "hits 'High', lifeguards get an automatic notification and digital signage at "
            "beach entries updates in real time."
        ),
        (
            "Strava / Garmin",
            "For ocean swimmers who log open-water sessions, a pre-session notification: "
            "'Shark risk at Bondi is currently Medium - 2 sightings in the last 7 days.'"
        ),
        (
            "Dorsal App",
            "Close the loop: predictions feed back into Dorsal's map as a risk overlay, "
            "complementing the community sightings they already show."
        ),
    ]

    for name, desc in integrations:
        pdf.set_font("Helvetica", "B", 11)
        pdf.cell(0, 8, name, new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 10)
        pdf.multi_cell(0, 5, desc)
        pdf.ln(3)

    pdf.ln(5)
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, "6. Limitations & Next Steps", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)
    pdf.multi_cell(0, 5,
        "- The model uses a rule-based weighted score, not ML. A logistic regression or "
        "gradient boosting model trained on labelled encounter data would improve accuracy.\n\n"
        "- Dorsal API access is required for real-time sightings. The pipeline falls back "
        "to cached data when the API is unavailable.\n\n"
        "- BOM rainfall data coverage varies by station. Stations further from the coast "
        "may not accurately reflect beach conditions.\n\n"
        "- Additional variables worth exploring: water temperature, moon phase, "
        "proximity to river mouths, and tagged shark movement corridors."
    )

    # -- app integration pages --
    featured_locations = ["Ballina", "Byron Bay", "Gold Coast"]
    for i, loc_name in enumerate(featured_locations):
        loc_rows = df[df["location_name"] == loc_name]
        if loc_rows.empty:
            continue
        page_num = 7 + i
        _draw_app_screen(pdf, loc_rows.iloc[0], page_num)

    out_path = os.path.join(os.path.dirname(__file__), "shark_encounter_report.pdf")
    pdf.output(out_path)
    print(f"Report saved to {out_path}")


def _draw_app_screen(pdf, loc, page_num):
    """Draw a mobile app screen for a location showing model output + AI insight."""
    from gemini_insights import generate_insight

    name = str(loc["location_name"])
    state = str(loc["state"])
    level = str(loc["risk_level"])
    score = int(loc["risk_score"])
    now = datetime.now()

    # state abbreviation for the header
    state_abbr = {
        "New South Wales": "NSW", "Queensland": "QLD", "Victoria": "VIC",
        "South Australia": "SA", "Western Australia": "WA", "Tasmania": "TAS",
    }.get(state, state[:3].upper())

    # generate Gemini insight
    insight_text = generate_insight(dict(loc))

    pdf.add_page()
    pdf.set_font("Helvetica", "B", 14)
    pdf.cell(0, 10, f"{page_num}. App Integration - {name} ({level} Risk)", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "I", 9)
    pdf.cell(0, 5, "Screen showing risk output with Gemini-generated insight summary", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)

    # phone frame
    phone_x = 55
    phone_y = pdf.get_y()
    phone_w = 100
    phone_h = 220

    # outer bezel
    pdf.set_draw_color(60, 60, 60)
    pdf.set_line_width(1.5)
    pdf.rect(phone_x, phone_y, phone_w, phone_h, style="D")

    # notch
    notch_w = 30
    notch_x = phone_x + (phone_w - notch_w) / 2
    pdf.set_fill_color(60, 60, 60)
    pdf.rect(notch_x, phone_y + 1, notch_w, 4, style="F")

    # inner screen
    screen_x = phone_x + 4
    screen_y = phone_y + 12
    screen_w = phone_w - 8
    screen_h = phone_h - 24
    pdf.set_fill_color(18, 32, 47)
    pdf.set_draw_color(18, 32, 47)
    pdf.rect(screen_x, screen_y, screen_w, screen_h, style="FD")

    # status bar
    pdf.set_text_color(180, 180, 180)
    pdf.set_font("Helvetica", "", 6)
    pdf.set_xy(screen_x + 3, screen_y + 2)
    pdf.cell(25, 4, now.strftime("%H:%M"))
    pdf.set_xy(screen_x + screen_w - 28, screen_y + 2)
    pdf.cell(25, 4, "4G  87%", align="R")

    # app header bar
    header_y = screen_y + 8
    pdf.set_fill_color(15, 52, 96)
    pdf.rect(screen_x, header_y, screen_w, 14, style="F")
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 10)
    pdf.set_xy(screen_x, header_y + 2)
    pdf.cell(screen_w, 10, "SharkSafe Australia", align="C")

    # location + date
    loc_y = header_y + 18
    pdf.set_text_color(220, 220, 220)
    pdf.set_font("Helvetica", "", 7)
    pdf.set_xy(screen_x + 5, loc_y)
    pdf.cell(50, 4, now.strftime("%A, %d %B %Y"))
    pdf.set_xy(screen_x + screen_w - 30, loc_y)
    pdf.cell(25, 4, now.strftime("%I:%M %p"), align="R")

    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 13)
    pdf.set_xy(screen_x + 5, loc_y + 6)
    pdf.cell(screen_w - 10, 8, f"{name}, {state_abbr}")

    # risk badge
    badge_y = loc_y + 17
    if level == "High":
        badge_r, badge_g, badge_b = 220, 50, 50
    elif level == "Medium":
        badge_r, badge_g, badge_b = 230, 170, 30
    else:
        badge_r, badge_g, badge_b = 40, 180, 80

    badge_w = 60
    badge_x = screen_x + (screen_w - badge_w) / 2
    pdf.set_fill_color(badge_r, badge_g, badge_b)
    pdf.rect(badge_x, badge_y, badge_w, 20, style="F")

    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 14)
    pdf.set_xy(badge_x, badge_y + 1)
    pdf.cell(badge_w, 10, f"{level.upper()} RISK", align="C")
    pdf.set_font("Helvetica", "", 8)
    pdf.set_xy(badge_x, badge_y + 11)
    pdf.cell(badge_w, 6, f"Score: {score}/100", align="C")

    # AI insight box
    insight_y = badge_y + 24
    insight_x = screen_x + 5
    insight_w = screen_w - 10
    pdf.set_fill_color(25, 42, 62)
    pdf.rect(insight_x, insight_y, insight_w, 28, style="F")

    # Gemini sparkle icon label
    pdf.set_text_color(130, 170, 255)
    pdf.set_font("Helvetica", "B", 6)
    pdf.set_xy(insight_x + 2, insight_y + 1)
    pdf.cell(50, 4, "* Gemini AI Insight")

    pdf.set_text_color(210, 220, 235)
    pdf.set_font("Helvetica", "", 6)
    pdf.set_xy(insight_x + 2, insight_y + 6)
    pdf.multi_cell(insight_w - 4, 3.5, insight_text)

    # detail cards
    card_y = insight_y + 32
    card_x = screen_x + 5
    card_w = screen_w - 10
    card_h = 11

    whale_str = "Yes" if loc["is_whale_season"] else "No"
    whale_note = "migration active" if loc["is_whale_season"] else "outside migration window"
    rain = loc["avg_rainfall_7d_mm"]
    if rain > 50:
        rain_note = "heavy runoff"
    elif rain > 20:
        rain_note = "moderate runoff"
    else:
        rain_note = "low runoff"

    details = [
        ("Sightings (7 days)", str(int(loc["recent_sightings_7d"])), "reports in area"),
        ("Rainfall (7-day avg)", f"{rain} mm", rain_note),
        ("Whale Season", whale_str, whale_note),
        ("Time of Day Risk", str(loc["time_of_day_risk"]).capitalize(), "current conditions"),
        ("Historical Incidents", str(int(loc["historical_incident_count"])), "total recorded"),
        ("Last Updated", now.strftime("%H:%M %p"), now.strftime("%d/%m/%Y")),
    ]

    for label, value, note in details:
        pdf.set_fill_color(30, 50, 70)
        pdf.rect(card_x, card_y, card_w, card_h, style="F")

        pdf.set_text_color(160, 180, 200)
        pdf.set_font("Helvetica", "", 5.5)
        pdf.set_xy(card_x + 3, card_y + 0.5)
        pdf.cell(40, 5, label)

        pdf.set_text_color(255, 255, 255)
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_xy(card_x + 3, card_y + 5)
        pdf.cell(30, 5, value)

        pdf.set_text_color(120, 140, 160)
        pdf.set_font("Helvetica", "", 5.5)
        pdf.set_xy(card_x + card_w - 42, card_y + 5)
        pdf.cell(40, 5, note, align="R")

        card_y += card_h + 1.5

    # bottom nav bar
    nav_y = screen_y + screen_h - 12
    pdf.set_fill_color(12, 25, 40)
    pdf.rect(screen_x, nav_y, screen_w, 12, style="F")
    pdf.set_text_color(140, 140, 140)
    pdf.set_font("Helvetica", "", 6)
    tabs = ["Map", "Alerts", "Report", "Settings"]
    tab_w = screen_w / len(tabs)
    for i, tab in enumerate(tabs):
        pdf.set_xy(screen_x + i * tab_w, nav_y + 3)
        pdf.cell(tab_w, 6, tab, align="C")

    # reset
    pdf.set_text_color(0, 0, 0)
    pdf.set_draw_color(0, 0, 0)
    pdf.set_line_width(0.2)


if __name__ == "__main__":
    generate_report()
