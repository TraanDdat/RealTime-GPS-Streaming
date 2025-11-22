# build_gps_dashboard.py
import json
from pathlib import Path

TOTAL_USERS = 182
DATASOURCE_UID = "cf4h4hrs2h728d"
OUTPUT = Path(__file__).parent / "gps_latest_points.json"

def color(uid: int) -> str:
    # Golden ratio hue spacing for better distribution
    hue = (uid * 0.618033988749895) % 1.0  # 0..1
    s, v = 0.78, 0.88
    import colorsys
    r, g, b = colorsys.hsv_to_rgb(hue, s, v)
    return f"rgb({int(r*255)},{int(g*255)},{int(b*255)})"

def build_targets():
    return [{
        "dataset": "gpsdb",
        "datasource": {"type": "grafana-postgresql-datasource", "uid": DATASOURCE_UID},
        "editorMode": "code",
        "format": "table",
        "hide": False,
        "rawQuery": True,
    # Alias lat/lon to latitude/longitude for clearer geomap field detection
    "rawSql": f"SELECT user_id, ts, lat AS latitude, lon AS longitude FROM gps_processed WHERE user_id = '{uid}' ORDER BY ts DESC LIMIT 1;",
        "refId": f"USER_{uid}",
        "sql": {"columns": [{"parameters": [], "type": "function"}],
                "groupBy": [{"property": {"type": "string"}, "type": "groupBy"}],
                "limit": 50}
    } for uid in range(TOTAL_USERS)]

def build_layers():
    return [{
        "type": "markers",
        "name": f"LAYER_USER_{uid}",
        "config": {
            "showLegend": True,
            "style": {
                "size": {"fixed": 5, "min": 2, "max": 15},
                "color": {"fixed": color(uid)},
                "opacity": 1,
                "symbol": {"mode": "fixed", "fixed": "img/icons/marker/circle.svg"},
                "symbolAlign": {"horizontal": "center", "vertical": "center"},
                "textConfig": {"fontSize": 12, "textAlign": "center", "textBaseline": "middle", "offsetX": 0, "offsetY": 0},
                "rotation": {"fixed": 0, "mode": "mod", "min": -360, "max": 360}
            }
        },
        # Match aliased column names from query
        "location": {"mode": "coords", "latitude": "latitude", "longitude": "longitude"},
        "tooltip": True,
        "filterData": {"id": "byRefId", "options": f"USER_{uid}"}
    } for uid in range(TOTAL_USERS)]

def build_dashboard():
    panel = {
        "id": 1,
        "type": "geomap",
        "title": f"GPS ({TOTAL_USERS} Users Latest Points)",
        "gridPos": {"x": 0, "y": 0, "h": 10, "w": 24},
        "pluginVersion": "12.3.0",
        "fieldConfig": {
            "defaults": {
                "custom": {"hideFrom": {"tooltip": False, "viz": False, "legend": False}},
                "mappings": [],
                "thresholds": {"mode": "absolute", "steps": [{"color": "dark-yellow", "value": None}, {"color": "red", "value": 80}]},
                "color": {"mode": "thresholds"}
            },
            "overrides": []
        },
        "datasource": {"type": "grafana-postgresql-datasource", "uid": DATASOURCE_UID},
        "targets": build_targets(),
        "options": {
            "view": {"allLayers": True, "id": "coords", "lat": 39.896484, "lon": 116.308651, "noRepeat": False, "zoom": 12, "lastOnly": False, "layer": "Layer 1"},
            "controls": {"showZoom": True, "mouseWheelZoom": True, "showAttribution": True, "showScale": False, "showMeasure": False, "showDebug": False},
            "tooltip": {"mode": "details"},
            "basemap": {"config": {}, "name": "Layer 0", "noRepeat": False, "type": "osm-standard"},
            "layers": build_layers()
        }
    }
    return {
        "uid": "gps-latest-182",
        "title": f"GPS Latest Points ({TOTAL_USERS} Users)",
        "timezone": "browser",
        "schemaVersion": 39,
        "version": 1,
        "refresh": "Off",
        "style": "dark",
        "editable": True,
        "time": {"from": "now-30m", "to": "now"},
        "panels": [panel],
        "links": []
    }

def main():
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    dashboard = build_dashboard()
    OUTPUT.write_text(json.dumps(dashboard, indent=2))
    print(f"Wrote dashboard {OUTPUT} with {TOTAL_USERS} users (0..{TOTAL_USERS-1})")

if __name__ == "__main__":
    main()