import asyncio
import os
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Literal, Optional

import asyncpg

# import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

# Load DB_URL from .env
load_dotenv()
DB_URL = os.getenv("DB_URL")
print("Loaded DB_URL:", DB_URL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    max_retries = 5
    delay = 3
    for attempt in range(1, max_retries + 1):
        try:
            app.state.db_pool = await asyncpg.create_pool(DB_URL)
            print(f"✅ Connected to database (attempt {attempt})")
            break
        except Exception as e:
            print(f"❌ DB connection failed on attempt {attempt}: {e}")
            if attempt == max_retries:
                raise e
            await asyncio.sleep(delay)
    yield
    await app.state.db_pool.close()


app = FastAPI(lifespan=lifespan)


@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"message": "Welcome to Indian Census API 2 🚀"}


@app.get("/states", summary="Get State Metadata", tags=["States"])
async def list_states(
    fields: Optional[str] = Query(
        None,
        description="Comma-separated list of fields to return. Allowed: state, name, level, district_count, subdistrict_count, town_count, village_count",
        example="state,name,district_count",
    ),
    sort_by: Optional[
        Literal[
            "state",
            "name",
            "level",
            "district_count",
            "subdistrict_count",
            "town_count",
            "village_count",
        ]
    ] = Query(
        "state", description="Field to sort by. Default is 'state'.", example="state"
    ),
    sort_order: Optional[Literal["asc", "desc"]] = Query(
        "asc", description="Sort order: 'asc' or 'desc'.", example="desc"
    ),
    state_code: Optional[int] = Query(
        None, description="Filter by state code.", example=1
    ),
    limit: int = Query(
        100,
        ge=1,
        le=1000,
        description="Number of results to return (1–1000).",
        example=50,
    ),
    offset: int = Query(0, ge=0, description="Pagination offset.", example=0),
):
    base_columns = ["state", "name", "level"]
    count_fields = {
        "district_count": ("district", "district"),
        "subdistrict_count": ("subdistt", "sub-district"),
        "village_count": ("town_village", "village"),
        "town_count": ("town_village", "town"),
    }
    allowed_fields = base_columns + list(count_fields.keys())

    # Handle selected fields
    if fields:
        selected_fields = [
            f.strip() for f in fields.split(",") if f.strip() in allowed_fields
        ]
        if not selected_fields:
            raise HTTPException(status_code=400, detail="No valid fields selected")
    else:
        selected_fields = ["state", "name"]

    selected_base = [f for f in selected_fields if f in base_columns]
    selected_counts = [f for f in selected_fields if f in count_fields]

    base_field_str = ", ".join(f"{f}" for f in selected_base)

    # Final SELECT query (no parameters inside subquery)
    base_query = f"""
        SELECT * FROM (
            SELECT DISTINCT ON (state) {base_field_str}
            FROM census_data
            WHERE TRIM(LOWER(level)) = 'state'
            ORDER BY state, {sort_by or 'state'} {(sort_order or 'asc').upper()}
        ) AS sorted_states
        {f"WHERE state = {state_code}" if state_code is not None else ""}
        ORDER BY {sort_by or 'state'} {(sort_order or 'asc').upper()}
        LIMIT {limit} OFFSET {offset}
    """

    async with app.state.db_pool.acquire() as conn:
        try:
            base_rows = await conn.fetch(base_query)
            results = [dict(r) for r in base_rows]

            # Fetch all counts (batch per count field)
            counts_by_state = {}
            for field in selected_counts:
                code_column, level_name = count_fields[field]
                count_query = f"""
                    SELECT state, COUNT(DISTINCT {code_column}) AS count
                    FROM census_data
                    WHERE TRIM(LOWER(level)) = $1
                    GROUP BY state
                """
                rows = await conn.fetch(count_query, level_name.lower())
                for row in rows:
                    st_code = row["state"]
                    if st_code not in counts_by_state:
                        counts_by_state[st_code] = {}
                    counts_by_state[st_code][field] = row["count"]

            # Merge counts into main result
            for result in results:
                st_code = result["state"]
                result.update(counts_by_state.get(st_code, {}))

            return JSONResponse(content=results)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/state-population")
async def get_state_population(
    states: str = Query(
        ...,  # Required
        description="Comma-separated state names (e.g ?states=Karnataka,Tamil%20Nadu)",
        example="Karnataka,Tamil Nadu",
    ),
    tru: Optional[str] = Query(
        None,
        description="Specify 'Total', 'Rural', or 'Urban'. If omitted, returns all.",
    ),
    include_population: bool = Query(
        True, description="Whether to include population data"
    ),
):
    allowed_tru = ["total", "rural", "urban"]
    if tru and tru.lower() not in allowed_tru:
        raise HTTPException(
            status_code=400, detail="Invalid tru value. Use: total, rural, or urban"
        )

    # 👇 Split the comma-separated string into list of states
    state_list = [s.strip().lower() for s in states.split(",") if s.strip()]
    if not state_list:
        raise HTTPException(status_code=400, detail="No valid state names provided.")

    placeholders = ", ".join(f"${i+1}" for i in range(len(state_list)))
    base_values = list(state_list)

    meta_query = f"""
        SELECT DISTINCT name, state
        FROM census_data
        WHERE TRIM(LOWER(level)) = 'state'
        AND TRIM(LOWER(name)) IN ({placeholders})
    """

    pop_query = f"""
        SELECT name, state, tru, tot_p AS population
        FROM census_data
        WHERE TRIM(LOWER(level)) = 'state'
        AND TRIM(LOWER(name)) IN ({placeholders})
    """

    if tru:
        meta_query += f" AND TRIM(LOWER(tru)) = ${len(base_values)+1}"
        pop_query += f" AND TRIM(LOWER(tru)) = ${len(base_values)+1}"
        base_values.append(tru.strip().lower())

    async with app.state.db_pool.acquire() as conn:
        try:
            meta_rows = await conn.fetch(meta_query, *base_values)
            if not meta_rows:
                raise HTTPException(status_code=404, detail="No matching states found")

            results = []
            for row in meta_rows:
                state_entry = {"name": row["name"], "state": row["state"]}
                if include_population:
                    state_entry["population"] = {}
                results.append(state_entry)

            if include_population:
                pop_rows = await conn.fetch(pop_query, *base_values)
                for row in pop_rows:
                    for state in results:
                        if state["state"] == row["state"]:
                            state["population"][row["tru"].lower()] = row["population"]

            return JSONResponse(content=results)

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/state-gender-population")
async def get_state_gender_population(
    states: str = Query(
        ...,
        description="Comma-separated list of state names (e.g. ?states=Karnataka,Tamil%20Nadu)",
        example="Karnataka,Tamil Nadu",
    ),
    tru: Optional[str] = Query(
        None,
        description="Total, Rural, or Urban",
        example="Rural",
    ),
):
    allowed_tru = ["total", "rural", "urban"]
    if tru and tru.lower() not in allowed_tru:
        raise HTTPException(status_code=400, detail="Invalid TRU value")

    # 👇 Convert comma-separated string to list
    values = [s.strip().lower() for s in states.split(",") if s.strip()]
    if not values:
        raise HTTPException(status_code=400, detail="No valid state names provided.")

    placeholders = ", ".join(f"${i+1}" for i in range(len(values)))

    gender_query = f"""
        SELECT name, state, tru, tot_p, tot_m, tot_f
        FROM census_data
        WHERE TRIM(LOWER(level)) = 'state'
        AND TRIM(LOWER(name)) IN ({placeholders})
    """

    if tru:
        gender_query += f" AND TRIM(LOWER(tru)) = ${len(values)+1}"
        values.append(tru.lower())

    async with app.state.db_pool.acquire() as conn:
        try:
            rows = await conn.fetch(gender_query, *values)
            if not rows:
                raise HTTPException(status_code=404, detail="No data found")

            grouped = defaultdict(lambda: {"population": {}})

            for row in rows:
                state_id = row["state"]
                state_name = row["name"]
                tru_key = row["tru"].lower()

                if "name" not in grouped[state_id]:
                    grouped[state_id]["name"] = state_name
                    grouped[state_id]["state"] = state_id

                grouped[state_id]["population"][tru_key] = {
                    "total": row["tot_p"],
                    "male": row["tot_m"],
                    "female": row["tot_f"],
                }

            return JSONResponse(content=list(grouped.values()))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/state-literacy")
async def get_state_literacy(
    states: str = Query(
        ...,
        description="Comma-separated list of state names (e.g. ?states=Karnataka,Tamil%20Nadu)",
        example="Karnataka,Tamil Nadu",
    ),
    tru: Optional[str] = Query(
        None,
        description="Total, Rural, or Urban. If not provided, all 3 will be returned.",
        example="Rural",
    ),
):
    allowed_tru = ["total", "rural", "urban"]
    if tru and tru.lower() not in allowed_tru:
        raise HTTPException(status_code=400, detail="Invalid TRU value")

    # 👇 Convert comma-separated string into list
    values = [s.strip().lower() for s in states.split(",") if s.strip()]
    if not values:
        raise HTTPException(status_code=400, detail="No valid state names provided.")

    placeholders = ", ".join(f"${i+1}" for i in range(len(values)))

    literacy_query = f"""
        SELECT name, state, tru, p_lit, m_lit, f_lit
        FROM census_data
        WHERE TRIM(LOWER(level)) = 'state'
        AND TRIM(LOWER(name)) IN ({placeholders})
    """

    if tru:
        literacy_query += f" AND TRIM(LOWER(tru)) = ${len(values)+1}"
        values.append(tru.lower())

    async with app.state.db_pool.acquire() as conn:
        try:
            rows = await conn.fetch(literacy_query, *values)
            if not rows:
                raise HTTPException(status_code=404, detail="No data found")

            grouped = {}
            for row in rows:
                state_id = row["state"]
                state_name = row["name"]
                key = (state_id, state_name)
                if key not in grouped:
                    grouped[key] = {}

                tru_key = row["tru"].lower() if tru is None else None
                literacy_data = {
                    "total": row["p_lit"],
                    "male": row["m_lit"],
                    "female": row["f_lit"],
                }

                if tru_key:
                    grouped[key][tru_key] = literacy_data
                else:
                    grouped[key] = literacy_data

            result = []
            for (state_id, name), data in grouped.items():
                obj = {
                    "name": name,
                    "state": state_id,
                }
                if tru:
                    obj["tru"] = tru.capitalize()
                    obj["literacy"] = data
                else:
                    obj["literacy"] = data
                result.append(obj)

            return JSONResponse(content=result)

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/state-workers")
async def get_state_workers(
    states: str = Query(
        ...,
        description="Comma-separated list of state names (e.g. ?states=Karnataka,Tamil%20Nadu)",
        example="Karnataka,Tamil Nadu",
    ),
    tru: Optional[str] = Query(
        None, description="Total, Rural, or Urban", example="Urban"
    ),
):
    allowed_tru = ["total", "rural", "urban"]
    if tru and tru.lower() not in allowed_tru:
        raise HTTPException(status_code=400, detail="Invalid TRU value")

    values = [s.strip().lower() for s in states.split(",") if s.strip()]
    if not values:
        raise HTTPException(status_code=400, detail="No valid state names provided.")

    placeholders = ", ".join(f"${i+1}" for i in range(len(values)))

    # Base query
    worker_query = f"""
        SELECT name, state, tru, tot_work_p, tot_work_m, tot_work_f
        FROM census_data
        WHERE TRIM(LOWER(level)) = 'state'
        AND TRIM(LOWER(name)) IN ({placeholders})
    """

    # TRU filter
    if tru:
        worker_query += f" AND TRIM(LOWER(tru)) = ${len(values)+1}"
        values.append(tru.lower())

    async with app.state.db_pool.acquire() as conn:
        try:
            rows = await conn.fetch(worker_query, *values)
            if not rows:
                raise HTTPException(status_code=404, detail="No data found")

            grouped = {}
            for row in rows:
                key = row["state"]
                tru_key = row["tru"].lower()

                if key not in grouped:
                    grouped[key] = {
                        "name": row["name"],
                        "state": row["state"],
                        "workers": {},
                    }

                grouped[key]["workers"][tru_key] = {
                    "total": row["tot_work_p"],
                    "male": row["tot_work_m"],
                    "female": row["tot_work_f"],
                }

            return JSONResponse(content=list(grouped.values()))

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/state-caste-population")
async def get_state_caste_population(
    states: str = Query(
        ...,
        description="Comma-separated list of state names (e.g. ?states=Karnataka,Tamil%20Nadu)",
        example="Karnataka,Tamil Nadu",
    ),
    tru: Optional[str] = Query(
        None, description="Total, Rural, or Urban", example="Rural"
    ),
):
    allowed_tru = ["total", "rural", "urban"]
    if tru and tru.lower() not in allowed_tru:
        raise HTTPException(status_code=400, detail="Invalid TRU value")

    values = [s.strip().lower() for s in states.split(",") if s.strip()]
    if not values:
        raise HTTPException(status_code=400, detail="No valid state names provided.")

    placeholders = ", ".join(f"${i+1}" for i in range(len(values)))

    caste_query = f"""
        SELECT name, state, tru,
               p_sc AS sc_total, m_sc AS sc_male, f_sc AS sc_female,
               p_st AS st_total, m_st AS st_male, f_st AS st_female
        FROM census_data
        WHERE TRIM(LOWER(level)) = 'state'
        AND TRIM(LOWER(name)) IN ({placeholders})
    """

    if tru:
        caste_query += f" AND TRIM(LOWER(tru)) = ${len(values) + 1}"
        values.append(tru.lower())

    async with app.state.db_pool.acquire() as conn:
        try:
            rows = await conn.fetch(caste_query, *values)
            if not rows:
                raise HTTPException(status_code=404, detail="No data found")

            grouped = {}
            for row in rows:
                state_id = row["state"]
                tru_key = row["tru"].lower()

                if state_id not in grouped:
                    grouped[state_id] = {
                        "name": row["name"],
                        "state": row["state"],
                        "caste_population": {},
                    }

                grouped[state_id]["caste_population"][tru_key] = {
                    "sc": {
                        "total": row["sc_total"],
                        "male": row["sc_male"],
                        "female": row["sc_female"],
                    },
                    "st": {
                        "total": row["st_total"],
                        "male": row["st_male"],
                        "female": row["st_female"],
                    },
                }

            return JSONResponse(content=list(grouped.values()))

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/state-non-workers")
async def get_state_non_workers(
    states: str = Query(
        ...,
        description="Comma-separated list of state names (e.g. ?states=Karnataka,Tamil%20Nadu)",
        example="Karnataka,Tamil Nadu",
    ),
    tru: Optional[str] = Query(
        None, description="Total, Rural, or Urban", example="Urban"
    ),
):
    allowed_tru = ["total", "rural", "urban"]
    if tru and tru.lower() not in allowed_tru:
        raise HTTPException(status_code=400, detail="Invalid TRU value")

    # Convert comma-separated string to lowercase list
    values = [s.strip().lower() for s in states.split(",") if s.strip()]
    if not values:
        raise HTTPException(status_code=400, detail="No valid state names provided.")

    placeholders = ", ".join(f"${i+1}" for i in range(len(values)))

    query = f"""
        SELECT name, state, tru, non_work_p, non_work_m, non_work_f
        FROM census_data
        WHERE TRIM(LOWER(level)) = 'state'
        AND TRIM(LOWER(name)) IN ({placeholders})
    """

    if tru:
        query += f" AND TRIM(LOWER(tru)) = ${len(values)+1}"
        values.append(tru.lower())

    async with app.state.db_pool.acquire() as conn:
        try:
            rows = await conn.fetch(query, *values)
            if not rows:
                raise HTTPException(status_code=404, detail="No data found")

            grouped = {}
            for row in rows:
                state_key = f"{row['state']}_{row['name']}"
                if state_key not in grouped:
                    grouped[state_key] = {
                        "name": row["name"],
                        "state": row["state"],
                        "non_workers": {},
                    }

                grouped[state_key]["non_workers"][row["tru"].lower()] = {
                    "total": row["non_work_p"],
                    "male": row["non_work_m"],
                    "female": row["non_work_f"],
                }

            return JSONResponse(content=list(grouped.values()))

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/state-locations")
async def get_state_locations(
    state: str = Query(..., description="State name (e.g. 'Karnataka')")
):
    state_name = state.strip().lower()

    async with app.state.db_pool.acquire() as conn:
        try:
            # Get state info
            meta_query = """
                SELECT DISTINCT state, name
                FROM census_data
                WHERE TRIM(LOWER(level)) = 'state' AND TRIM(LOWER(name)) = $1
                LIMIT 1
            """
            meta_row = await conn.fetchrow(meta_query, state_name)
            if not meta_row:
                raise HTTPException(status_code=404, detail="State not found")

            state_code = meta_row["state"]

            # Helper function to fetch location names for a given level
            async def fetch_names(level_name):
                rows = await conn.fetch(
                    """
                    SELECT DISTINCT name
                    FROM census_data
                    WHERE TRIM(LOWER(level)) = $1 AND state = $2
                    ORDER BY name
                    """,
                    level_name,
                    state_code,
                )
                return [row["name"] for row in rows]

            # Fetch sequentially to avoid asyncpg conflict
            districts = await fetch_names("district")
            subdistricts = await fetch_names("sub-district")
            towns = await fetch_names("town")
            villages = await fetch_names("village")

            result = {
                "name": meta_row["name"],
                "state": state_code,
                "districts": districts,
                "subdistricts": subdistricts,
                "towns": towns,
                "villages": villages,
            }

            return JSONResponse(content=result)

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/state-households")
async def get_state_households(
    states: str = Query(
        ...,
        description="Comma-separated list of state names (e.g. ?states=Karnataka,Tamil%20Nadu)",
        example="Karnataka,Tamil Nadu",
    ),
    tru: Optional[str] = Query(
        None,
        description="Total, Rural, or Urban — filters by TRU. If omitted, shows all.",
        example="Rural",
    ),
):
    allowed_tru = ["total", "rural", "urban"]
    if tru and tru.lower() not in allowed_tru:
        raise HTTPException(status_code=400, detail="Invalid TRU value")

    # Convert comma-separated string to list of lowercase values
    values = [s.strip().lower() for s in states.split(",") if s.strip()]
    if not values:
        raise HTTPException(status_code=400, detail="No valid state names provided.")

    placeholders = ", ".join(f"${i+1}" for i in range(len(values)))

    base_query = f"""
        SELECT name, state, tru, no_hh, tot_p, tot_m, tot_f, p_06, m_06, f_06
        FROM census_data
        WHERE TRIM(LOWER(level)) = 'state'
        AND TRIM(LOWER(name)) IN ({placeholders})
    """

    if tru:
        base_query += f" AND TRIM(LOWER(tru)) = ${len(values)+1}"
        values.append(tru.lower())

    async with app.state.db_pool.acquire() as conn:
        try:
            rows = await conn.fetch(base_query, *values)
            if not rows:
                raise HTTPException(status_code=404, detail="No data found")

            state_data = {}
            for row in rows:
                state_id = row["state"]
                name = row["name"]
                tru_value = row["tru"].lower()

                if state_id not in state_data:
                    state_data[state_id] = {
                        "name": name,
                        "state": state_id,
                    }

                    if tru:
                        state_data[state_id]["households"] = row["no_hh"]
                    else:
                        state_data[state_id]["households"] = {}

                    state_data[state_id]["population"] = {
                        "total": row["tot_p"],
                        "male": row["tot_m"],
                        "female": row["tot_f"],
                    }
                    state_data[state_id]["under_6"] = {
                        "total": row["p_06"],
                        "male": row["m_06"],
                        "female": row["f_06"],
                    }

                if not tru:
                    state_data[state_id]["households"][tru_value] = row["no_hh"]

            return JSONResponse(content=list(state_data.values()))

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/state-location-hierarchy")
async def get_location_hierarchy(
    state: str = Query(..., description="State name"),
    include_subdistricts: bool = Query(True, description="Include sub-districts"),
    include_places: bool = Query(True, description="Include towns and villages"),
):
    async with app.state.db_pool.acquire() as conn:
        try:
            state_name = state.strip().lower()

            # 1. Get state info
            state_row = await conn.fetchrow(
                """
                SELECT DISTINCT state, name
                FROM census_data
                WHERE TRIM(LOWER(level)) = 'state' AND TRIM(LOWER(name)) = $1
                LIMIT 1
            """,
                state_name,
            )

            if not state_row:
                raise HTTPException(status_code=404, detail="State not found")

            state_code = state_row["state"]
            state_display = state_row["name"]

            # 2. Get districts
            districts = await conn.fetch(
                """
                SELECT DISTINCT district, name
                FROM census_data
                WHERE state = $1 AND TRIM(LOWER(level)) = 'district'
                ORDER BY name
            """,
                state_code,
            )

            # Optionally fetch subdistricts
            sub_map = defaultdict(list)
            place_map = defaultdict(lambda: {"towns": [], "villages": []})

            if include_subdistricts:
                subdistricts = await conn.fetch(
                    """
                    SELECT DISTINCT subdistt, district, name
                    FROM census_data
                    WHERE state = $1 AND TRIM(LOWER(level)) = 'sub-district'
                """,
                    state_code,
                )

                # Optionally fetch towns & villages
                if include_places:
                    places = await conn.fetch(
                        """
                        SELECT name, level, district, subdistt
                        FROM census_data
                        WHERE state = $1 AND TRIM(LOWER(level)) IN ('town', 'village')
                    """,
                        state_code,
                    )

                    for p in places:
                        key = (p["district"], p["subdistt"])
                        if p["level"].lower() == "town":
                            place_map[key]["towns"].append(p["name"])
                        else:
                            place_map[key]["villages"].append(p["name"])

                for sub in subdistricts:
                    key = sub["district"]
                    sub_key = (sub["district"], sub["subdistt"])
                    sub_map[key].append(
                        {
                            "name": sub["name"],
                            **(
                                {
                                    "towns": place_map[sub_key]["towns"],
                                    "villages": place_map[sub_key]["villages"],
                                }
                                if include_places
                                else {}
                            ),
                        }
                    )

            # 3. Build response
            result = {"state": state_display, "districts": []}

            for dist in districts:
                result["districts"].append(
                    {
                        "name": dist["name"],
                        **(
                            {"subdistricts": sub_map.get(dist["district"], [])}
                            if include_subdistricts
                            else {}
                        ),
                    }
                )

            return JSONResponse(content=result)

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


@app.get("/district-population-breakdown")
async def get_district_population_breakdown(
    states: str = Query(
        ...,
        description="Comma-separated state names (e.g. ?states=Karnataka,Tamil%20Nadu)",
    ),
    tru: Optional[str] = Query(None, description="Total, Rural, or Urban"),
    limit: Optional[int] = Query(None, description="Limit number of districts"),
    offset: Optional[int] = Query(0, description="Pagination offset"),
):
    allowed_tru = ["total", "rural", "urban"]
    if tru and tru.lower() not in allowed_tru:
        raise HTTPException(status_code=400, detail="Invalid tru value")

    # Convert comma-separated input to list
    state_values = [s.strip().lower() for s in states.split(",") if s.strip()]
    if not state_values:
        raise HTTPException(status_code=400, detail="No valid state names provided.")

    placeholders = ", ".join(f"${i+1}" for i in range(len(state_values)))
    values = state_values.copy()

    query = f"""
        SELECT c.name AS district, s.name AS state, c.tru,
               c.tot_p AS total, c.tot_m AS male, c.tot_f AS female
        FROM census_data c
        JOIN (
            SELECT DISTINCT state, name
            FROM census_data
            WHERE TRIM(LOWER(level)) = 'state' AND TRIM(LOWER(name)) IN ({placeholders})
        ) s ON c.state = s.state
        WHERE TRIM(LOWER(c.level)) = 'district'
    """

    if tru:
        query += f" AND TRIM(LOWER(c.tru)) = ${len(values)+1}"
        values.append(tru.strip().lower())

    query += " ORDER BY s.name, c.name"

    if limit:
        query += f" LIMIT {limit}"
    if offset:
        query += f" OFFSET {offset}"

    async with app.state.db_pool.acquire() as conn:
        try:
            rows = await conn.fetch(query, *values)

            from collections import defaultdict

            grouped_states = defaultdict(lambda: defaultdict(dict))

            for row in rows:
                state = row["state"]
                district = row["district"]
                tru_key = row["tru"].lower()

                grouped_states[state][district][tru_key] = {
                    "total": row["total"],
                    "male": row["male"],
                    "female": row["female"],
                }

            # Build structured response
            result = []
            for state, districts in grouped_states.items():
                state_obj = {"state": state, "districts": []}
                for dist_name, tru_data in districts.items():
                    state_obj["districts"].append(
                        {"name": dist_name, "population": tru_data}
                    )
                result.append(state_obj)

            return JSONResponse(content=result)

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
