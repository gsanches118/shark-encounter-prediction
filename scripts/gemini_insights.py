import os
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = "gemini-1.5-flash"


def generate_insight(location_data):
    """
    Call Gemini to generate a short natural-language insight
    explaining why a location has its current risk level.

    Falls back to a rule-based summary if the API key is missing
    or the call fails.
    """
    prompt = _build_prompt(location_data)

    if GEMINI_API_KEY:
        try:
            return _call_gemini(prompt)
        except Exception as e:
            print(f"Gemini API error: {e}, using fallback")

    return _fallback_insight(location_data)


def _build_prompt(d):
    return (
        f"You are a marine safety analyst. Given the following shark encounter "
        f"risk data for {d['location_name']}, {d['state']}, write a 2-3 sentence "
        f"summary explaining why the risk level is {d['risk_level']}. "
        f"Be concise and practical, aimed at surfers and swimmers.\n\n"
        f"Risk score: {d['risk_score']}/100\n"
        f"Risk level: {d['risk_level']}\n"
        f"Recent sightings (7 days): {d['recent_sightings_7d']}\n"
        f"Average rainfall (7 days): {d['avg_rainfall_7d_mm']} mm\n"
        f"Whale season active: {d['is_whale_season']}\n"
        f"Time of day: {d['time_of_day_risk']}\n"
        f"Historical incidents on record: {d['historical_incident_count']}\n"
    )


def _call_gemini(prompt):
    """Hit the Gemini REST API."""
    import requests

    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    )

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "maxOutputTokens": 120,
            "temperature": 0.4,
        },
    }

    resp = requests.post(url, json=payload, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    return data["candidates"][0]["content"]["parts"][0]["text"].strip()


def _fallback_insight(d):
    """Rule-based insight when Gemini API is not available."""
    name = d["location_name"]
    level = d["risk_level"]
    sightings = int(d["recent_sightings_7d"])
    rain = d["avg_rainfall_7d_mm"]
    hist = int(d["historical_incident_count"])
    whale = d["is_whale_season"]
    tod = d["time_of_day_risk"]

    parts = []

    if level == "High":
        parts.append(f"{name} is rated HIGH risk today.")
        if sightings >= 2:
            parts.append(
                f"{sightings} shark sightings were reported in the last 7 days, "
                f"which is the strongest driver of this score."
            )
        if rain > 40:
            parts.append(
                f"Heavy rainfall ({rain}mm avg) has increased coastal runoff, "
                f"likely attracting baitfish and predators inshore."
            )
        if hist > 15:
            parts.append(
                f"This area has {hist} historically recorded incidents, "
                f"making it one of the highest-risk stretches in Australia."
            )
    elif level == "Medium":
        parts.append(f"{name} is rated MEDIUM risk today.")
        if sightings >= 1:
            parts.append(
                f"{sightings} sighting(s) reported recently. "
                f"Stay alert and swim between the flags."
            )
        if rain > 20:
            parts.append(
                f"Moderate rainfall ({rain}mm avg over 7 days) may be "
                f"drawing baitfish closer to shore."
            )
        if hist > 10:
            parts.append(f"The area has a notable history of {hist} recorded incidents.")
    else:
        parts.append(f"{name} is rated LOW risk today.")
        parts.append("No recent sightings and conditions are favourable.")

    if whale:
        parts.append("Whale migration season is active, which can attract large sharks along the coast.")

    if tod in ("dawn", "dusk"):
        parts.append(f"Current time ({tod}) is a higher-risk period due to reduced visibility.")

    return " ".join(parts[:4])


if __name__ == "__main__":
    # verify
    test_data = {
        "location_name": "Ballina",
        "state": "New South Wales",
        "risk_score": 72,
        "risk_level": "High",
        "recent_sightings_7d": 3,
        "avg_rainfall_7d_mm": 54.2,
        "is_whale_season": False,
        "time_of_day_risk": "dawn",
        "historical_incident_count": 24,
    }
    print(generate_insight(test_data))
