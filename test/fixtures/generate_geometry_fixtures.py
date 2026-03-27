#!/usr/bin/env python3
# /// script
# dependencies = [
#   "pyarrow",
#   "shapely",
# ]
# ///

from pathlib import Path
import json

import pyarrow as pa
import pyarrow.parquet as pq
from shapely.geometry import (
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
)


FIXTURES = {
    "sample-gp11.parquet": [
        ("alpha", 1, Point(77.1, 12.1)),
        ("bravo", 2, Point(77.2, 12.2)),
    ],
    "sample-point.parquet": [
        ("alpha", 1, Point(77.1, 12.1)),
        ("bravo", 2, Point(77.2, 12.2)),
    ],
    "sample-multipoint.parquet": [
        ("alpha", 1, MultiPoint([(77.1, 12.1), (77.15, 12.15)])),
        ("bravo", 2, MultiPoint([(77.2, 12.2), (77.25, 12.25)])),
    ],
    "sample-linestring.parquet": [
        ("alpha", 1, LineString([(77.1, 12.1), (77.2, 12.2)])),
        ("bravo", 2, LineString([(77.3, 12.3), (77.4, 12.4)])),
    ],
    "sample-multilinestring.parquet": [
        (
            "alpha",
            1,
            MultiLineString(
                [[(77.1, 12.1), (77.2, 12.2)], [(77.2, 12.2), (77.25, 12.3)]]
            ),
        ),
        (
            "bravo",
            2,
            MultiLineString(
                [[(77.3, 12.3), (77.4, 12.4)], [(77.4, 12.4), (77.45, 12.5)]]
            ),
        ),
    ],
    "sample-polygon.parquet": [
        (
            "alpha",
            1,
            Polygon(
                [(77.1, 12.1), (77.2, 12.1), (77.2, 12.2), (77.1, 12.2), (77.1, 12.1)]
            ),
        ),
        (
            "bravo",
            2,
            Polygon(
                [(77.3, 12.3), (77.4, 12.3), (77.4, 12.4), (77.3, 12.4), (77.3, 12.3)]
            ),
        ),
    ],
    "sample-multipolygon.parquet": [
        (
            "alpha",
            1,
            MultiPolygon(
                [
                    Polygon(
                        [(77.1, 12.1), (77.2, 12.1), (77.2, 12.2), (77.1, 12.2), (77.1, 12.1)]
                    ),
                    Polygon(
                        [(77.22, 12.22), (77.26, 12.22), (77.26, 12.26), (77.22, 12.26), (77.22, 12.22)]
                    ),
                ]
            ),
        ),
        (
            "bravo",
            2,
            MultiPolygon(
                [
                    Polygon(
                        [(77.3, 12.3), (77.4, 12.3), (77.4, 12.4), (77.3, 12.4), (77.3, 12.3)]
                    ),
                    Polygon(
                        [(77.42, 12.42), (77.46, 12.42), (77.46, 12.46), (77.42, 12.46), (77.42, 12.42)]
                    ),
                ]
            ),
        ),
    ],
    "sample-mixed-point-multipoint.parquet": [
        ("alpha", 1, Point(77.1, 12.1)),
        ("bravo", 2, MultiPoint([(77.2, 12.2), (77.25, 12.25)])),
    ],
    "sample-mixed-line-multiline.parquet": [
        ("alpha", 1, LineString([(77.1, 12.1), (77.2, 12.2)])),
        (
            "bravo",
            2,
            MultiLineString([[(77.3, 12.3), (77.4, 12.4)], [(77.4, 12.4), (77.45, 12.5)]]),
        ),
    ],
    "sample-mixed-polygon-multipolygon.parquet": [
        (
            "alpha",
            1,
            Polygon(
                [(77.1, 12.1), (77.2, 12.1), (77.2, 12.2), (77.1, 12.2), (77.1, 12.1)]
            ),
        ),
        (
            "bravo",
            2,
            MultiPolygon(
                [
                    Polygon(
                        [(77.3, 12.3), (77.4, 12.3), (77.4, 12.4), (77.3, 12.4), (77.3, 12.3)]
                    ),
                    Polygon(
                        [(77.42, 12.42), (77.46, 12.42), (77.46, 12.46), (77.42, 12.46), (77.42, 12.42)]
                    ),
                ]
            ),
        ),
    ],
    "sample-mixed-point-polygon.parquet": [
        ("alpha", 1, Point(77.1, 12.1)),
        (
            "bravo",
            2,
            Polygon(
                [(77.3, 12.3), (77.4, 12.3), (77.4, 12.4), (77.3, 12.4), (77.3, 12.3)]
            ),
        ),
    ],
}

NESTED_ROWS = [
    {
        "name": "alpha",
        "value": 1,
        "geometry": Point(77.1, 12.1),
        "info": {
            "code": "A1",
            "score": 11,
            "nest": {
                "rank": "high",
                "tags": ["north", "east"],
            },
        },
        "dims": {
            "h": 10,
            "w": 20,
            "deep": {
                "count": 2,
                "notes": ["first", "second"],
            },
        },
        "items": ["red", "blue"],
    },
    {
        "name": "bravo",
        "value": 2,
        "geometry": Point(77.2, 12.2),
        "info": {
            "code": "B2",
            "score": 22,
            "nest": {
                "rank": "low",
                "tags": [],
            },
        },
        "dims": {
            "h": 30,
            "w": 40,
            "deep": {
                "count": 1,
                "notes": [],
            },
        },
        "items": [],
    },
]

LIST_STRUCT_ROWS = [
    {
        "name": "alpha",
        "value": 1,
        "geometry": Point(77.1, 12.1),
        "info": {
            "code": "A1",
            "score": 11,
            "nest": {
                "rank": "high",
                "tags": ["north", "east"],
            },
        },
        "items": [
            {"id": 101, "label": "red"},
            {"id": 102, "label": "blue"},
        ],
    },
    {
        "name": "bravo",
        "value": 2,
        "geometry": Point(77.2, 12.2),
        "info": {
            "code": "B2",
            "score": 22,
            "nest": {
                "rank": "low",
                "tags": [],
            },
        },
        "items": [],
    },
]

LONG_FIELD_ROWS = [
    {
        "name": "alpha",
        "geometry": Point(77.1, 12.1),
        "transport_details": {
            "identifier_code": "A-001",
            "identifier_name": "Alpha Depot",
        },
        "transport_depth": {
            "identifier_code": "D-001",
            "identifier_name": "Deep Alpha",
        },
        "administrative_details": {
            "identifier_code": "ADM-1",
        },
    },
    {
        "name": "bravo",
        "geometry": Point(77.2, 12.2),
        "transport_details": {
            "identifier_code": "B-002",
            "identifier_name": "Bravo Depot",
        },
        "transport_depth": {
            "identifier_code": "D-002",
            "identifier_name": "Deep Bravo",
        },
        "administrative_details": {
            "identifier_code": "ADM-2",
        },
    },
]


def build_table(rows):
    bbox_type = pa.struct(
        [
            ("xmin", pa.float64()),
            ("ymin", pa.float64()),
            ("xmax", pa.float64()),
            ("ymax", pa.float64()),
        ]
    )

    names, values, geometries, bboxes, geometry_types = [], [], [], [], []
    for name, value, geom in rows:
        minx, miny, maxx, maxy = geom.bounds
        names.append(name)
        values.append(value)
        geometries.append(geom.wkb)
        bboxes.append({"xmin": minx, "ymin": miny, "xmax": maxx, "ymax": maxy})
        geometry_types.append(geom.geom_type)

    table = pa.table(
        {
            "name": pa.array(names, type=pa.string()),
            "value": pa.array(values, type=pa.int64()),
            "geometry": pa.array(geometries, type=pa.binary()),
            "bbox": pa.array(bboxes, type=bbox_type),
        }
    )

    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": sorted(set(geometry_types)),
                "crs": None,
                "covering": {
                    "bbox": {
                        "xmin": ["bbox", "xmin"],
                        "ymin": ["bbox", "ymin"],
                        "xmax": ["bbox", "xmax"],
                        "ymax": ["bbox", "ymax"],
                    }
                },
            }
        },
    }

    metadata = dict(table.schema.metadata or {})
    metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")
    return table.replace_schema_metadata(metadata)


def build_nested_table(rows):
    bbox_type = pa.struct(
        [
            ("xmin", pa.float64()),
            ("ymin", pa.float64()),
            ("xmax", pa.float64()),
            ("ymax", pa.float64()),
        ]
    )
    info_type = pa.struct(
        [
            ("code", pa.string()),
            ("score", pa.int64()),
            ("nest", pa.struct([("rank", pa.string()), ("tags", pa.list_(pa.string()))])),
        ]
    )
    dims_type = pa.struct(
        [
            ("h", pa.int64()),
            ("w", pa.int64()),
            ("deep", pa.struct([("count", pa.int64()), ("notes", pa.list_(pa.string()))])),
        ]
    )
    names, values, geometries, bboxes, infos, dims, items = [], [], [], [], [], [], []
    geometry_types = []

    for row in rows:
        geom = row["geometry"]
        minx, miny, maxx, maxy = geom.bounds
        names.append(row["name"])
        values.append(row["value"])
        geometries.append(geom.wkb)
        bboxes.append({"xmin": minx, "ymin": miny, "xmax": maxx, "ymax": maxy})
        infos.append(row["info"])
        dims.append(row["dims"])
        items.append(row["items"])
        geometry_types.append(geom.geom_type)

    table = pa.table(
        {
            "name": pa.array(names, type=pa.string()),
            "value": pa.array(values, type=pa.int64()),
            "geometry": pa.array(geometries, type=pa.binary()),
            "bbox": pa.array(bboxes, type=bbox_type),
            "info": pa.array(infos, type=info_type),
            "dims": pa.array(dims, type=dims_type),
            "items": pa.array(items, type=pa.list_(pa.string())),
        }
    )

    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": sorted(set(geometry_types)),
                "crs": None,
                "covering": {
                    "bbox": {
                        "xmin": ["bbox", "xmin"],
                        "ymin": ["bbox", "ymin"],
                        "xmax": ["bbox", "xmax"],
                        "ymax": ["bbox", "ymax"],
                    }
                },
            }
        },
    }

    metadata = dict(table.schema.metadata or {})
    metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")
    return table.replace_schema_metadata(metadata)


def build_list_struct_table(rows):
    bbox_type = pa.struct(
        [
            ("xmin", pa.float64()),
            ("ymin", pa.float64()),
            ("xmax", pa.float64()),
            ("ymax", pa.float64()),
        ]
    )
    info_type = pa.struct(
        [
            ("code", pa.string()),
            ("score", pa.int64()),
            ("nest", pa.struct([("rank", pa.string()), ("tags", pa.list_(pa.string()))])),
        ]
    )
    item_type = pa.struct([("id", pa.int64()), ("label", pa.string())])

    names, values, geometries, bboxes, infos, items = [], [], [], [], [], []
    geometry_types = []

    for row in rows:
        geom = row["geometry"]
        minx, miny, maxx, maxy = geom.bounds
        names.append(row["name"])
        values.append(row["value"])
        geometries.append(geom.wkb)
        bboxes.append({"xmin": minx, "ymin": miny, "xmax": maxx, "ymax": maxy})
        infos.append(row["info"])
        items.append(row["items"])
        geometry_types.append(geom.geom_type)

    table = pa.table(
        {
            "name": pa.array(names, type=pa.string()),
            "value": pa.array(values, type=pa.int64()),
            "geometry": pa.array(geometries, type=pa.binary()),
            "bbox": pa.array(bboxes, type=bbox_type),
            "info": pa.array(infos, type=info_type),
            "items": pa.array(items, type=pa.list_(item_type)),
        }
    )

    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": sorted(set(geometry_types)),
                "crs": None,
                "covering": {
                    "bbox": {
                        "xmin": ["bbox", "xmin"],
                        "ymin": ["bbox", "ymin"],
                        "xmax": ["bbox", "xmax"],
                        "ymax": ["bbox", "ymax"],
                    }
                },
            }
        },
    }

    metadata = dict(table.schema.metadata or {})
    metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")
    return table.replace_schema_metadata(metadata)


def build_long_field_table(rows):
    bbox_type = pa.struct(
        [
            ("xmin", pa.float64()),
            ("ymin", pa.float64()),
            ("xmax", pa.float64()),
            ("ymax", pa.float64()),
        ]
    )
    transport_type = pa.struct(
        [
            ("identifier_code", pa.string()),
            ("identifier_name", pa.string()),
        ]
    )
    admin_type = pa.struct([("identifier_code", pa.string())])

    names, geometries, bboxes = [], [], []
    transport_details, transport_depth, administrative_details = [], [], []
    geometry_types = []

    for row in rows:
        geom = row["geometry"]
        minx, miny, maxx, maxy = geom.bounds
        names.append(row["name"])
        geometries.append(geom.wkb)
        bboxes.append({"xmin": minx, "ymin": miny, "xmax": maxx, "ymax": maxy})
        transport_details.append(row["transport_details"])
        transport_depth.append(row["transport_depth"])
        administrative_details.append(row["administrative_details"])
        geometry_types.append(geom.geom_type)

    table = pa.table(
        {
            "name": pa.array(names, type=pa.string()),
            "geometry": pa.array(geometries, type=pa.binary()),
            "bbox": pa.array(bboxes, type=bbox_type),
            "transport_details": pa.array(transport_details, type=transport_type),
            "transport_depth": pa.array(transport_depth, type=transport_type),
            "administrative_details": pa.array(administrative_details, type=admin_type),
        }
    )

    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": sorted(set(geometry_types)),
                "crs": None,
                "covering": {
                    "bbox": {
                        "xmin": ["bbox", "xmin"],
                        "ymin": ["bbox", "ymin"],
                        "xmax": ["bbox", "xmax"],
                        "ymax": ["bbox", "ymax"],
                    }
                },
            }
        },
    }

    metadata = dict(table.schema.metadata or {})
    metadata[b"geo"] = json.dumps(geo_meta).encode("utf-8")
    return table.replace_schema_metadata(metadata)


def main():
    out_dir = Path(__file__).resolve().parent
    for file_name, rows in FIXTURES.items():
        pq.write_table(build_table(rows), out_dir / file_name, row_group_size=1)
        print(file_name)
    pq.write_table(build_nested_table(NESTED_ROWS), out_dir / "sample-nested-attrs.parquet", row_group_size=1)
    print("sample-nested-attrs.parquet")
    pq.write_table(build_list_struct_table(LIST_STRUCT_ROWS), out_dir / "sample-nested-list-struct.parquet", row_group_size=1)
    print("sample-nested-list-struct.parquet")
    pq.write_table(build_long_field_table(LONG_FIELD_ROWS), out_dir / "sample-long-fieldnames.parquet", row_group_size=1)
    print("sample-long-fieldnames.parquet")


if __name__ == "__main__":
    main()
