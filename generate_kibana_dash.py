import json

cities = ["Milano", "Pechino", "Parigi", "Roma"]
ndjson_lines = []

for i, city in enumerate(cities):
    vis_state = {
        "title": f"{city} - PM10 Real vs Predicted",
        "type": "line",
        "params": {
            "type": "line",
            "grid": {"categoryLines": False},
            "categoryAxes": [
                {
                    "id": "CategoryAxis-1",
                    "type": "category",
                    "position": "bottom",
                    "show": True,
                    "style": {},
                    "scale": {"type": "linear"},
                    "labels": {"show": True, "filter": True, "truncate": 100},
                    "title": {}
                }
            ],
            "valueAxes": [
                {
                    "id": "ValueAxis-1",
                    "name": "LeftAxis-1",
                    "type": "value",
                    "position": "left",
                    "show": True,
                    "style": {},
                    "scale": {"type": "linear", "mode": "normal"},
                    "labels": {"show": True, "filter": False, "truncate": 100},
                    "title": {"text": "PM10"}
                }
            ],
            "seriesParams": [
                {
                    "show": True,
                    "type": "line",
                    "mode": "normal",
                    "data": {"label": "Predicted PM10", "id": "1"},
                    "valueAxis": "ValueAxis-1",
                    "drawLinesBetweenPoints": True,
                    "showCircles": True
                },
                {
                    "show": True,
                    "type": "line",
                    "mode": "normal",
                    "data": {"label": "Real PM10", "id": "3"},
                    "valueAxis": "ValueAxis-1",
                    "drawLinesBetweenPoints": True,
                    "showCircles": True
                }
            ],
            "addTooltip": True,
            "addLegend": True,
            "legendPosition": "right",
            "times": [],
            "addTimeMarker": False
        },
        "aggs": [
            {
                "id": "1",
                "enabled": True,
                "type": "max",
                "schema": "metric",
                "params": {"field": "predicted_pm10"}
            },
            {
                "id": "3",
                "enabled": True,
                "type": "max",
                "schema": "metric",
                "params": {"field": "pm10"}
            },
            {
                "id": "2",
                "enabled": True,
                "type": "date_histogram",
                "schema": "segment",
                "params": {
                    "field": "@timestamp",
                    "timeRange": "",
                    "useNormalizedEsInterval": False,
                    "interval": "1h",
                    "drop_partials": False,
                    "min_doc_count": 1,
                    "extended_bounds": {}
                }
            }
        ]
    }

    search_source = {
        "query": {
            "query": f"zone.keyword:\"{city}\"",
            "language": "kuery"
        },
        "filter": [],
        "indexRefName": "kibanaSavedObjectMeta.searchSourceJSON.index"
    }

    vis_obj = {
        "type": "visualization",
        "id": f"vis-pm10-{city.lower()}",
        "attributes": {
            "title": f"{city} - PM10 Real vs Predicted",
            "visState": json.dumps(vis_state),
            "uiStateJSON": "{}",
            "description": f"Comparazione PM10 reale e predetto per {city}",
            "version": 1,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps(search_source)
            }
        },
        "references": [
            {
                "name": "kibanaSavedObjectMeta.searchSourceJSON.index",
                "type": "index-pattern",
                "id": "city-predictions*"
            }
        ],
        "migrationVersion": {"visualization": "7.17.0"},
        "coreMigrationVersion": "7.17.9"
    }
    
    ndjson_lines.append(json.dumps(vis_obj))

panels = []
for i, city in enumerate(cities):
    w = 24
    h = 15
    x = (i % 2) * 24
    y = (i // 2) * 15
    panel = {
        "gridData": {"w": w, "h": h, "x": x, "y": y, "i": str(i+1)},
        "version": "7.17.9",
        "panelIndex": str(i+1),
        "type": "visualization",
        "panelRefName": f"panel_{i}",
        "embeddableConfig": {}
    }
    panels.append(panel)

dashboard_search_source = {
    "query": {"query": "", "language": "kuery"},
    "filter": []
}

dashboard_obj = {
    "type": "dashboard",
    "id": "dashboard-cities-pm10",
    "attributes": {
        "title": "SmartCity - PM10 per Città",
        "hits": 0,
        "description": "Dashboard con grafici separati per ogni città, con PM10 reale e PM10 predetto",
        "panelsJSON": json.dumps(panels),
        "optionsJSON": json.dumps({"useMargins": True, "hidePanelTitles": False}),
        "version": 1,
        "timeRestore": True,
        "timeTo": "now",
        "timeFrom": "now-1000h",
        "kibanaSavedObjectMeta": {
            "searchSourceJSON": json.dumps(dashboard_search_source)
        }
    },
    "references": [
        {
            "name": f"panel_{i}",
            "type": "visualization",
            "id": f"vis-pm10-{city.lower()}"
        } for i, city in enumerate(cities)
    ],
    "migrationVersion": {"dashboard": "7.15.0"},
    "coreMigrationVersion": "7.17.9"
}

ndjson_lines.append(json.dumps(dashboard_obj))

index_pattern_obj = {
    "type": "index-pattern",
    "id": "city-predictions*",
    "attributes": {
        "title": "city-predictions*",
        "timeFieldName": "@timestamp"
    },
    "migrationVersion": {"index-pattern": "7.11.0"},
    "coreMigrationVersion": "7.17.9"
}
ndjson_lines.insert(0, json.dumps(index_pattern_obj))

with open("cities_dashboard.ndjson", "w") as f:
    for line in ndjson_lines:
        f.write(line + "\n")

print("File cities_dashboard.ndjson generated.")
