import logging
import os
import traceback

import arcpy
import duckdb
import pandas as pd

from src.utils.arc_logging import setup_arcgis_logging
from src.utils.auth import get_portal_token
from src.utils.constants import URL_BIOMON_EDIT_FS, URL_BIOMON_STATIONS
from src.utils.data_conversion import (
    df_to_esri_features,
    duckdb_to_memory,
    featureservice_to_df,
    run_apply_edits,
)

logger = logging.getLogger("biomon_tools")

LAYERS = {
    "visits": 0,
    "fish_counts": 2,
}

DEFAULT_WKID = 2248

REQUIRED_SCHEMA: dict[str, list[str]] = {
    "Benthics": [
        "STATION",
        "DATE",
        "TAXA",
        "#INDVS",
        "trays",
        "#GRIDS",
    ],
    "Fish": [
        "STATION",
        "SAMPLE_DATE",
        "SPECIES",
        "PASS1",
        "PASS2",
        "ANOMALIES",
        "ANOMALIES_TYPE",
    ],
    "PhysChem": [
        "STATION",
        "SAMPLE_DATE",
        "AIRTEMP",
        "H2OTEMP_C",
        "PH",
        "DISOXY",
        "%_SAT",
        "CONDUCT",
        "Turbidity",
        "MonitoringTime",
        "COMMENTS",
    ],
    "Rhab": [
        "STATION",
        "SAMPLE_DATE",
        "INSTR_COVER",
        "EPI_SUBSTR",
        "EMBEDD",
        "CH_ALTER",
        "SED_DEPOSIT",
        "RIFFLE_FREQ",
        "CHAN_FLOW",
        "LB_BANKVEG",
        "RB_BANKVEG",
        "LB_BANKSTAB",
        "RB_BANKSTAB",
        "LB_BUFFER",
        "RB_BUFFER",
        "COMMENTS",
    ],
}


def validate_excel_schema(xlsx_path: str) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    file_name = os.path.basename(xlsx_path)

    try:
        excel_file = pd.ExcelFile(xlsx_path)
    except PermissionError:
        return [
            f"[{file_name}] Permission denied: The Excel workbook is currently open or locked. Please close it and try again."
        ], []
    except (OSError, ValueError, RuntimeError) as e:
        return [f"[{file_name}] Could not read Excel workbook: {e}"], []

    sheet_names_str = [str(s) for s in excel_file.sheet_names]
    actual_sheets = set(sheet_names_str)
    missing_sheets = set(REQUIRED_SCHEMA.keys()) - actual_sheets

    if missing_sheets:
        errors.append(
            f"[{file_name}] Missing required sheet(s): {', '.join(sorted(missing_sheets))} "
            f"(Found sheets: {', '.join(sheet_names_str)})"
        )

    for sheet_name, req_cols in REQUIRED_SCHEMA.items():
        if sheet_name not in actual_sheets:
            continue
        try:
            df_header = pd.read_excel(xlsx_path, sheet_name=sheet_name, nrows=0)
            actual_cols_upper = {str(col).strip().upper() for col in df_header.columns}

            missing_cols = [
                col for col in req_cols if col.upper() not in actual_cols_upper
            ]
            if missing_cols:
                errors.append(
                    f"[{file_name}] Sheet '{sheet_name}' is missing required field(s): {', '.join(missing_cols)}"
                )

            if (
                sheet_name in {"PhysChem", "Rhab"}
                and "MON_TYPE" not in actual_cols_upper
            ):
                warnings.append(
                    f"[{file_name}] Sheet '{sheet_name}' is missing optional field 'MON_TYPE'. Visit types will be inferred from counts or defaulted."
                )
        except (OSError, ValueError, KeyError, RuntimeError) as e:
            errors.append(
                f"[{file_name}] Error inspecting columns in sheet '{sheet_name}': {e}"
            )

    return errors, warnings


def _refresh_visits_existing(con: duckdb.DuckDBPyConnection, token: str) -> None:
    df_visits = featureservice_to_df(
        f"{URL_BIOMON_EDIT_FS}/{LAYERS['visits']}",
        out_fields=["GlobalID", "STATION", "VISIT_TYPE", "VISIT_DATE", "DATA_NOTES"],
        token=token,
    )
    if not df_visits.empty:
        con.register("df_visits", df_visits)
        con.execute("""
            CREATE OR REPLACE TABLE visits_existing AS
            SELECT
                * EXCLUDE (GEOMWKB, VISIT_DATE)
                , CAST(epoch_ms(VISIT_DATE::BIGINT)::DATE AS TIMESTAMP) + INTERVAL 12 HOUR AS VISIT_DATE
                , ST_GeomFromWKB(GEOMWKB) AS geom
            FROM df_visits;
        """)
    else:
        con.execute("""
            CREATE OR REPLACE TABLE visits_existing (
                GlobalID VARCHAR,
                STATION VARCHAR,
                VISIT_TYPE VARCHAR,
                VISIT_DATE TIMESTAMP,
                DATA_NOTES VARCHAR,
                geom GEOMETRY
            );
        """)


class ImportBioMonDEPExcel:
    def __init__(self):
        self.label = "Import MCDEP Biological Monitoring Excel"
        self.description = (
            "Ingests one or more MCDEP Biological Monitoring data exchange Excel workbooks (.xlsx) "
            "(Benthics, Fish, PhysChem, and Rhab sheets) and upserts Visit and Fish Count "
            "records into the hosted BioMon_EDIT Feature Service, or outputs preview "
            "layers/tables to the active map for pre-import validation."
        )
        self.canRunInBackground = False

    def getParameterInfo(self):
        param_xlsx = arcpy.Parameter(
            displayName="Input BioMon Excel File(s) (.xlsx)",
            name="in_excel_file",
            datatype="DEFile",
            parameterType="Required",
            direction="Input",
            multiValue=True,
        )
        param_xlsx.filter.list = ["xlsx"]  # pyright: ignore[reportOptionalMemberAccess]

        param_validate_only = arcpy.Parameter(
            displayName="Validate Only (Preview Inserts/Updates in Active Map)",
            name="validate_only",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input",
        )
        param_validate_only.value = False

        param_derived_out = arcpy.Parameter(
            displayName="Preview Datasets",
            name="out_preview_datasets",
            datatype=["GPFeatureLayer", "GPTableView"],
            parameterType="Derived",
            direction="Output",
            multiValue=True,
        )

        return [param_xlsx, param_validate_only, param_derived_out]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        param_xlsx = parameters[0]
        param_xlsx.clearMessage()

        raw_files = param_xlsx.valueAsText
        if not raw_files:
            return

        file_list = [f.strip("'") for f in raw_files.split(";")]
        all_errors = []
        all_warnings = []

        for xlsx_path in file_list:
            if not os.path.exists(xlsx_path):
                all_errors.append(
                    f"[{os.path.basename(xlsx_path)}] The specified Excel file does not exist."
                )
                continue

            if not xlsx_path.lower().endswith(".xlsx"):
                all_errors.append(
                    f"[{os.path.basename(xlsx_path)}] Input file must be an .xlsx workbook."
                )
                continue

            schema_errors, schema_warnings = validate_excel_schema(xlsx_path)
            all_errors.extend(schema_errors)
            all_warnings.extend(schema_warnings)

        if all_errors:
            formatted_errors = "\n - ".join(all_errors)
            param_xlsx.setErrorMessage(
                f"Excel Schema Validation Failed:\n - {formatted_errors}"
            )
        elif all_warnings:
            formatted_warnings = "\n - ".join(all_warnings)
            param_xlsx.setWarningMessage(
                f"Excel Schema Warning:\n - {formatted_warnings}"
            )

    def execute(self, parameters, messages):
        if not logger.handlers and not logging.getLogger().handlers:
            setup_arcgis_logging(logging.DEBUG, "biomon_tools")
        else:
            logger.handlers.clear()
            setup_arcgis_logging(logging.DEBUG, "biomon_tools")

        arcpy.env.overwriteOutput = True

        raw_files = parameters[0].valueAsText
        file_list = [f.strip("'") for f in raw_files.split(";")]
        validate_only = bool(parameters[1].value)

        mode_str = "VALIDATION / PREVIEW MODE" if validate_only else "LIVE IMPORT MODE"
        logger.info(
            f"Starting Biological Monitoring data import [{mode_str}] for {len(file_list)} file(s)."
        )
        arcpy.SetProgressor(
            "step", "Validating Excel schema(s) and initializing...", 0, 6, 1
        )

        all_schema_errors = []
        all_schema_warnings = []
        for xlsx_path in file_list:
            errs, warns = validate_excel_schema(xlsx_path)
            all_schema_errors.extend(errs)
            all_schema_warnings.extend(warns)

        if all_schema_errors:
            for err in all_schema_errors:
                logger.error(f"Schema Validation Error: {err}")
            raise arcpy.ExecuteError(
                "Execution halted due to missing required sheets or fields in the input Excel workbook(s)."
            )
        for warn in all_schema_warnings:
            logger.warning(f"Schema Warning: {warn}")

        active_map = None
        try:
            aprx = arcpy.mp.ArcGISProject("CURRENT")
            active_map = aprx.activeMap
        except OSError:
            active_map = None

        con = duckdb.connect(database=":memory:")
        con.sql("INSTALL spatial; LOAD spatial;")

        try:
            # -----------------------------------------------------------------
            # STEP 1 & 2: Authentication & Fetching Existing Data
            # -----------------------------------------------------------------
            arcpy.SetProgressorLabel("Authenticating & fetching existing data...")
            token = get_portal_token()
            if not token:
                logger.warning(
                    "No ArcGIS Pro portal token detected. Proceeding with anonymous request."
                )

            df_stations = featureservice_to_df(
                URL_BIOMON_STATIONS,
                out_fields=["STATION"],
                token=token,
            )
            if df_stations.empty:
                logger.error(
                    "Failed to retrieve Station records from the Stations Feature Service."
                )
                return

            con.register("df_stations", df_stations)
            con.execute("""
                CREATE OR REPLACE TABLE stations AS
                SELECT
                    * EXCLUDE (GEOMWKB)
                    , ST_GeomFromWKB(GEOMWKB) AS geom
                FROM df_stations;
            """)

            _refresh_visits_existing(con, token)
            arcpy.SetProgressorPosition(2)

            # -----------------------------------------------------------------
            # STEP 3: Read & Normalize Excel Sheets in DuckDB (Multi-File)
            # -----------------------------------------------------------------
            arcpy.SetProgressorLabel("Reading Excel sheets into DuckDB...")

            con.execute("""
                CREATE OR REPLACE TABLE raw_benthics_counts (SOURCE_FILE VARCHAR, STATION VARCHAR, VISIT_DATE DATE, BC_TAXA VARCHAR, INDIVIDUALS INT, TRAYS INT, GRIDS INT);
                CREATE OR REPLACE TABLE raw_fish_counts (SOURCE_FILE VARCHAR, STATION VARCHAR, VISIT_DATE DATE, FC_SPECIES VARCHAR, PASS_1 INT, PASS_2 INT, TOTAL_COUNT INT, TOTAL_ANOMALIES INT, ANOMALIES VARCHAR, row_num INT);
                CREATE OR REPLACE TABLE raw_phys_chem (SOURCE_FILE VARCHAR, STATION VARCHAR, VISIT_DATE DATE, VISIT_TYPE VARCHAR, PC_AIRTEMP_F INT, PC_AIRTEMP_C INT, PC_WATERTEMP_C FLOAT, PC_PH FLOAT, PC_DO_CONC FLOAT, PC_DO_SAT FLOAT, PC_COND FLOAT, PC_TURB FLOAT, PC_TIME VARCHAR, PC_NOTES VARCHAR);
                CREATE OR REPLACE TABLE raw_rhab (SOURCE_FILE VARCHAR, STATION VARCHAR, VISIT_DATE DATE, VISIT_TYPE VARCHAR, RHAB_INSTREAMCOVER INT, RHAB_EPISUBSTRATE INT, RHAB_EMBEDDEDNESS INT, RHAB_CHANNELALT INT, RHAB_SEDDEPOSIT INT, RHAB_RIFFLEFREQ INT, RHAB_CHANNELFLOWSTATUS INT, RHAB_VEG_LB INT, RHAB_VEG_RB INT, RHAB_STABILITY_LB INT, RHAB_STABILITY_RB INT, RHAB_BUFFER_LB INT, RHAB_BUFFER_RB INT, RHAB_NOTES VARCHAR);
            """)

            for target_file in file_list:
                safe_target_file = target_file.replace("\\", "/")
                base_name = os.path.basename(target_file)
                logger.info(f"Loading data from {base_name}...")

                con.execute(f"""
                    CREATE OR REPLACE TEMP TABLE _stg_physchem AS
                    SELECT * FROM read_xlsx('{safe_target_file}', sheet='PhysChem', all_varchar=true);

                    CREATE OR REPLACE TEMP TABLE _stg_rhab AS
                    SELECT * FROM read_xlsx('{safe_target_file}', sheet='Rhab', all_varchar=true);
                """)

                pc_cols = {
                    r[1].upper()
                    for r in con.execute(
                        "PRAGMA table_info('_stg_physchem')"
                    ).fetchall()
                }
                rhab_cols = {
                    r[1].upper()
                    for r in con.execute("PRAGMA table_info('_stg_rhab')").fetchall()
                }

                pc_mon_type_expr = (
                    "TRIM(MON_TYPE::VARCHAR)"
                    if "MON_TYPE" in pc_cols
                    else "CAST(NULL AS VARCHAR)"
                )
                rhab_mon_type_expr = (
                    "TRIM(MON_TYPE::VARCHAR)"
                    if "MON_TYPE" in rhab_cols
                    else "CAST(NULL AS VARCHAR)"
                )

                con.execute(f"""
                    INSERT INTO raw_benthics_counts
                    SELECT
                        '{base_name}' AS SOURCE_FILE
                        , TRIM(STATION::VARCHAR) AS STATION
                        , (CAST('1899-12-30' AS DATE) + CAST(FLOOR(TRY_CAST("DATE" AS DOUBLE)) AS INTEGER))::DATE AS VISIT_DATE
                        , REPLACE(LOWER(TAXA::VARCHAR), 'none', 'no benthics observed') AS BC_TAXA
                        , TRY_CAST(NULLIF("#INDVS", 'NULL') AS INTEGER) AS INDIVIDUALS
                        , TRY_CAST(NULLIF(trays, 'NULL') AS INTEGER) AS TRAYS
                        , TRY_CAST(NULLIF("#GRIDS", 'NULL') AS INTEGER) AS GRIDS
                    FROM read_xlsx('{safe_target_file}', sheet='Benthics', all_varchar=true);

                    INSERT INTO raw_fish_counts
                    SELECT
                        '{base_name}' AS SOURCE_FILE
                        , TRIM(STATION::VARCHAR) AS STATION
                        , (CAST('1899-12-30' AS DATE) + CAST(FLOOR(TRY_CAST(SAMPLE_DATE AS DOUBLE)) AS INTEGER))::DATE AS VISIT_DATE
                        , LOWER(SPECIES::VARCHAR).REPLACE('none', 'no fish observed').REPLACE('eastern mosquitofish', 'mosquitofish').REPLACE('cutlips minnow', 'cutlip minnow') AS FC_SPECIES
                        , TRY_CAST(NULLIF(PASS1, 'NULL') AS INTEGER) AS PASS_1
                        , TRY_CAST(NULLIF(PASS2, 'NULL') AS INTEGER) AS PASS_2
                        , TRY_CAST(NULLIF(PASS1, 'NULL') AS INTEGER) + TRY_CAST(NULLIF(PASS2, 'NULL') AS INTEGER) AS TOTAL_COUNT
                        , TRY_CAST(NULLIF(ANOMALIES, 'NULL') AS INTEGER) AS TOTAL_ANOMALIES
                        , CASE
                            WHEN ANOMALIES_TYPE::VARCHAR = 'NULL' THEN NULL
                            ELSE UPPER(ANOMALIES_TYPE::VARCHAR)
                        END AS ANOMALIES
                        , ROW_NUMBER() OVER () AS row_num
                    FROM read_xlsx('{safe_target_file}', sheet='Fish', all_varchar=true);

                    INSERT INTO raw_phys_chem
                    SELECT
                        '{base_name}' AS SOURCE_FILE
                        , TRIM(STATION::VARCHAR) AS STATION
                        , (CAST('1899-12-30' AS DATE) + CAST(FLOOR(TRY_CAST(SAMPLE_DATE AS DOUBLE)) AS INTEGER))::DATE AS VISIT_DATE
                        , {pc_mon_type_expr} AS VISIT_TYPE
                        , (TRY_CAST(NULLIF(AIRTEMP, 'NULL') AS INTEGER) * 9 / 5 + 32) AS PC_AIRTEMP_F
                        , TRY_CAST(NULLIF(AIRTEMP, 'NULL') AS INTEGER) AS PC_AIRTEMP_C
                        , TRY_CAST(NULLIF(H2OTEMP_C, 'NULL') AS FLOAT) AS PC_WATERTEMP_C
                        , TRY_CAST(NULLIF(PH, 'NULL') AS FLOAT) AS PC_PH
                        , TRY_CAST(NULLIF(DISOXY, 'NULL') AS FLOAT) AS PC_DO_CONC
                        , TRY_CAST(NULLIF("%_SAT", 'NULL') AS FLOAT) AS PC_DO_SAT
                        , TRY_CAST(NULLIF(CONDUCT, 'NULL') AS FLOAT) AS PC_COND
                        , TRY_CAST(NULLIF(Turbidity, 'NULL') AS FLOAT) AS PC_TURB
                        , CASE 
                            WHEN NULLIF(TRIM(MonitoringTime), 'NULL') IS NULL THEN NULL
                            WHEN TRY_CAST(NULLIF(TRIM(MonitoringTime), 'NULL') AS DOUBLE) IS NOT NULL 
                                 AND TRY_CAST(NULLIF(TRIM(MonitoringTime), 'NULL') AS DOUBLE) >= 0 
                                 AND TRY_CAST(NULLIF(TRIM(MonitoringTime), 'NULL') AS DOUBLE) < 1
                            THEN STRFTIME(
                                TIMESTAMP '2000-01-01 00:00:00' + INTERVAL (ROUND(TRY_CAST(NULLIF(TRIM(MonitoringTime), 'NULL') AS DOUBLE) * 86400)) SECOND, 
                                '%H%M'
                            )
                            ELSE LPAD(REGEXP_REPLACE(NULLIF(TRIM(MonitoringTime), 'NULL'), '[^0-9]', '', 'g'), 4, '0')
                        END AS PC_TIME
                        , NULLIF(COMMENTS, 'NULL')::VARCHAR AS PC_NOTES
                    FROM _stg_physchem;

                    INSERT INTO raw_rhab
                    SELECT
                        '{base_name}' AS SOURCE_FILE
                        , TRIM(STATION::VARCHAR) AS STATION
                        , (CAST('1899-12-30' AS DATE) + CAST(FLOOR(TRY_CAST(SAMPLE_DATE AS DOUBLE)) AS INTEGER))::DATE AS VISIT_DATE
                        , {rhab_mon_type_expr} AS VISIT_TYPE
                        , TRY_CAST(NULLIF(INSTR_COVER, 'NULL') AS INTEGER) AS RHAB_INSTREAMCOVER
                        , TRY_CAST(NULLIF(EPI_SUBSTR, 'NULL') AS INTEGER) AS RHAB_EPISUBSTRATE
                        , TRY_CAST(NULLIF(EMBEDD, 'NULL') AS INTEGER) AS RHAB_EMBEDDEDNESS
                        , TRY_CAST(NULLIF(CH_ALTER, 'NULL') AS INTEGER) AS RHAB_CHANNELALT
                        , TRY_CAST(NULLIF(SED_DEPOSIT, 'NULL') AS INTEGER) AS RHAB_SEDDEPOSIT
                        , TRY_CAST(NULLIF(RIFFLE_FREQ, 'NULL') AS INTEGER) AS RHAB_RIFFLEFREQ
                        , TRY_CAST(NULLIF(CHAN_FLOW, 'NULL') AS INTEGER) AS RHAB_CHANNELFLOWSTATUS
                        , TRY_CAST(NULLIF(LB_BANKVEG, 'NULL') AS INTEGER) AS RHAB_VEG_LB
                        , TRY_CAST(NULLIF(RB_BANKVEG, 'NULL') AS INTEGER) AS RHAB_VEG_RB
                        , TRY_CAST(NULLIF(LB_BANKSTAB, 'NULL') AS INTEGER) AS RHAB_STABILITY_LB
                        , TRY_CAST(NULLIF(RB_BANKSTAB, 'NULL') AS INTEGER) AS RHAB_STABILITY_RB
                        , TRY_CAST(NULLIF(LB_BUFFER, 'NULL') AS INTEGER) AS RHAB_BUFFER_LB
                        , TRY_CAST(NULLIF(RB_BUFFER, 'NULL') AS INTEGER) AS RHAB_BUFFER_RB
                        , NULLIF(COMMENTS, 'NULL')::VARCHAR AS RHAB_NOTES
                    FROM _stg_rhab;
                """)

            arcpy.SetProgressorPosition(3)

            # -----------------------------------------------------------------
            # STEP 4: Construct & Upsert Primary Visits (PhysChem & Rhab)
            # -----------------------------------------------------------------
            arcpy.SetProgressorLabel("Processing PhysChem & Rhab Visits...")
            con.execute("""
                CREATE OR REPLACE TABLE visits AS
                SELECT
                    COALESCE(pc.SOURCE_FILE, rhab.SOURCE_FILE) AS SOURCE_FILE
                    , COALESCE(pc.STATION, rhab.STATION) AS STATION
                    , CAST(COALESCE(pc.VISIT_DATE, rhab.VISIT_DATE) AS TIMESTAMP) + INTERVAL 12 HOUR AS VISIT_DATE
                    , COALESCE(
                        NULLIF(COALESCE(pc.VISIT_TYPE, rhab.VISIT_TYPE), ''),
                        CASE 
                            WHEN COALESCE(pc.STATION, rhab.STATION) IN (SELECT STATION FROM raw_benthics_counts WHERE VISIT_DATE = COALESCE(pc.VISIT_DATE, rhab.VISIT_DATE)) THEN 'BIB'
                            WHEN COALESCE(pc.STATION, rhab.STATION) IN (SELECT STATION FROM raw_fish_counts WHERE VISIT_DATE = COALESCE(pc.VISIT_DATE, rhab.VISIT_DATE)) THEN 'BIF'
                            WHEN COALESCE(pc.STATION, rhab.STATION) NOT IN (SELECT STATION FROM raw_fish_counts) AND rhab.VISIT_TYPE IS NULL THEN 'OTH'
                            WHEN COALESCE(pc.STATION, rhab.STATION) NOT IN (SELECT STATION FROM raw_fish_counts) AND rhab.VISIT_TYPE IS NOT NULL THEN 'HAB'
                            ELSE 'OTH'
                        END
                    ) AS VISIT_TYPE
                    , CASE WHEN COALESCE(pc.VISIT_TYPE, rhab.VISIT_TYPE, '') = 'BIF' THEN 'DEP' ELSE NULL END AS FISH_LEADER1
                    , CASE WHEN COALESCE(pc.VISIT_TYPE, rhab.VISIT_TYPE, '') = 'BIF' THEN 'DEP' ELSE NULL END AS FISH_LEADER2
                    , CASE WHEN rhab.VISIT_TYPE IS NOT NULL THEN 'DEP' ELSE NULL END AS HAB_ASSESSOR
                    , CASE WHEN COALESCE(pc.VISIT_TYPE, rhab.VISIT_TYPE, '') = 'BIB' THEN 'DEP' ELSE NULL END AS BEN_SAMPLER
                    , CASE WHEN COALESCE(pc.VISIT_TYPE, rhab.VISIT_TYPE, '') = 'BIB' THEN 'DEP' ELSE NULL END AS BEN_SUBSAMPLER
                    , CASE WHEN COALESCE(pc.VISIT_TYPE, rhab.VISIT_TYPE, '') = 'BIF' THEN 'DEP' ELSE NULL END AS FISH_TAXONOMIST1
                    , CASE WHEN COALESCE(pc.VISIT_TYPE, rhab.VISIT_TYPE, '') = 'BIF' THEN 'DEP' ELSE NULL END AS FISH_TAXONOMIST2
                    , 'DEP' AS DATA_ENTERER
                    , CURRENT_DATE AS DATA_ENTER_DATE
                    , CASE 
                        WHEN TRIM(SPLIT_PART(COALESCE(ve.DATA_NOTES, ''), 'Bulk upload as of', 1)) = '' THEN ''
                        ELSE TRIM(SPLIT_PART(COALESCE(ve.DATA_NOTES, ''), 'Bulk upload as of', 1)) || ' '
                    END || 'Bulk upload as of ' || CURRENT_DATE || '.' AS DATA_NOTES
                    , pc.PC_AIRTEMP_F
                    , pc.PC_AIRTEMP_C
                    , pc.PC_WATERTEMP_C
                    , pc.PC_PH
                    , pc.PC_DO_CONC
                    , pc.PC_DO_SAT
                    , pc.PC_COND
                    , pc.PC_TURB
                    , pc.PC_TIME
                    , pc.PC_NOTES
                    , rhab.RHAB_INSTREAMCOVER
                    , rhab.RHAB_EPISUBSTRATE
                    , rhab.RHAB_EMBEDDEDNESS
                    , rhab.RHAB_CHANNELALT
                    , rhab.RHAB_SEDDEPOSIT
                    , rhab.RHAB_RIFFLEFREQ
                    , rhab.RHAB_CHANNELFLOWSTATUS
                    , rhab.RHAB_VEG_LB
                    , rhab.RHAB_VEG_RB
                    , rhab.RHAB_STABILITY_LB
                    , rhab.RHAB_STABILITY_RB
                    , rhab.RHAB_BUFFER_LB
                    , rhab.RHAB_BUFFER_RB
                    , rhab.RHAB_NOTES
                    , COALESCE(ve.geom, s.geom) AS GEOM
                    , ST_X(COALESCE(ve.geom, s.geom)) AS x
                    , ST_Y(COALESCE(ve.geom, s.geom)) AS y
                    , ve.GlobalID
                FROM raw_phys_chem pc
                FULL OUTER JOIN raw_rhab rhab
                    ON pc.STATION = rhab.STATION
                    AND pc.VISIT_DATE = rhab.VISIT_DATE
                    AND COALESCE(pc.VISIT_TYPE, '') = COALESCE(rhab.VISIT_TYPE, '')
                LEFT JOIN visits_existing ve
                    ON COALESCE(pc.STATION, rhab.STATION) = ve.STATION
                    AND DATE(COALESCE(pc.VISIT_DATE, rhab.VISIT_DATE)) = DATE(ve.VISIT_DATE)
                    AND COALESCE(pc.VISIT_TYPE, rhab.VISIT_TYPE, '') = COALESCE(ve.VISIT_TYPE, '')
                LEFT JOIN stations s
                    ON COALESCE(pc.STATION, rhab.STATION) = s.STATION
                WHERE pc.STATION IS NOT NULL OR rhab.STATION IS NOT NULL;
            """)

            # ---- DATA VALIDATION BLOCK ----
            con.execute("""
                ALTER TABLE visits ADD COLUMN IMPORT_STATUS VARCHAR DEFAULT 'OK';
                ALTER TABLE visits ADD COLUMN IMPORT_MESSAGE VARCHAR DEFAULT 'Ready';

                UPDATE visits 
                SET IMPORT_STATUS = 'ERROR'
                    , IMPORT_MESSAGE = CONCAT_WS(' | ', NULLIF(IMPORT_MESSAGE, ''), 'Invalid PH (Must be 0-14)') 
                WHERE PC_PH < 0 OR PC_PH > 14;

                UPDATE visits 
                SET IMPORT_STATUS = 'ERROR'
                    , IMPORT_MESSAGE = CONCAT_WS(' | ', NULLIF(IMPORT_MESSAGE, ''), 'Invalid Turbidity (Must be 0-10)') 
                WHERE PC_TURB < 0 OR PC_TURB > 10;

                UPDATE visits
                SET IMPORT_STATUS = 'ERROR'
                    , IMPORT_MESSAGE = CONCAT_WS(' | ', NULLIF(IMPORT_MESSAGE, ''), 'Invalid MonitoringTime (Must be HHMM)')
                WHERE PC_TIME IS NOT NULL 
                  AND (LENGTH(PC_TIME) != 4 
                       OR TRY_CAST(SUBSTR(PC_TIME, 1, 2) AS INT) >= 24 
                       OR TRY_CAST(SUBSTR(PC_TIME, 3, 2) AS INT) >= 60);
            """)

            rhab_bounds = [
                ("RHAB_INSTREAMCOVER", 20),
                ("RHAB_EPISUBSTRATE", 20),
                ("RHAB_EMBEDDEDNESS", 20),
                ("RHAB_CHANNELALT", 20),
                ("RHAB_SEDDEPOSIT", 20),
                ("RHAB_RIFFLEFREQ", 20),
                ("RHAB_CHANNELFLOWSTATUS", 20),
                ("RHAB_VEG_LB", 20),
                ("RHAB_VEG_RB", 20),
                ("RHAB_STABILITY_LB", 10),
                ("RHAB_STABILITY_RB", 10),
                ("RHAB_BUFFER_LB", 10),
                ("RHAB_BUFFER_RB", 10),
            ]
            for col, max_val in rhab_bounds:
                con.execute(f"""
                    UPDATE visits 
                    SET IMPORT_STATUS = 'ERROR'
                        , IMPORT_MESSAGE = CONCAT_WS(' | ', NULLIF(IMPORT_MESSAGE, ''), 'Invalid {col} (0-{max_val})') 
                    WHERE {col} < 0 OR {col} > {max_val};
                """)

            df_off_by_one = con.sql("""
                SELECT
                    ve.GlobalID
                    , ve.STATION
                    , ve.VISIT_TYPE
                    , CAST(ve.VISIT_DATE AS DATE) AS EXISTING_VISIT_DATE
                    , CAST(v.VISIT_DATE AS DATE) AS IMPORT_DATE
                FROM visits_existing ve
                INNER JOIN visits v
                    ON ve.STATION = v.STATION
                    AND ve.VISIT_TYPE = v.VISIT_TYPE
                WHERE CAST(ve.VISIT_DATE AS DATE) != CAST(v.VISIT_DATE AS DATE)
                    AND (
                        CAST(ve.VISIT_DATE AS DATE) = CAST(v.VISIT_DATE AS DATE) - INTERVAL 1 DAY
                        OR CAST(ve.VISIT_DATE AS DATE) = CAST(v.VISIT_DATE AS DATE) + INTERVAL 1 DAY
                    )
                ORDER BY ve.STATION, v.VISIT_DATE;
            """).df()

            if not df_off_by_one.empty:
                con.execute("""
                    UPDATE visits
                    SET IMPORT_STATUS = CASE WHEN IMPORT_STATUS = 'ERROR' THEN 'ERROR' ELSE 'WARNING' END
                        , IMPORT_MESSAGE = CONCAT_WS(' | ', NULLIF(IMPORT_MESSAGE, ''), 'Possible +/- 1 day date mismatch with existing visit')
                    WHERE (STATION, VISIT_TYPE, DATE(VISIT_DATE)) IN (
                        SELECT STATION, VISIT_TYPE, DATE(IMPORT_DATE) FROM df_off_by_one
                    );
                """)

            has_validation_errors = False
            validation_errors = con.execute(
                "SELECT COUNT(*) FROM visits WHERE IMPORT_STATUS = 'ERROR'"
            ).fetchone()

            if validation_errors and validation_errors[0] > 0:
                has_validation_errors = True

            if has_validation_errors and not validate_only:
                logger.warning(
                    "Data validation errors found in primary visits! Forcing Preview Mode so you can inspect the errors in the active map."
                )
                validate_only = True

            df_visits_update = con.sql(
                "SELECT * EXCLUDE (IMPORT_STATUS, IMPORT_MESSAGE) FROM visits WHERE GlobalID IS NOT NULL AND IMPORT_STATUS != 'ERROR'"
            ).df()
            df_visits_insert = con.sql(
                "SELECT * EXCLUDE (GlobalID, IMPORT_STATUS, IMPORT_MESSAGE) FROM visits WHERE GlobalID IS NULL AND IMPORT_STATUS != 'ERROR'"
            ).df()

            if not validate_only:
                if len(df_visits_update) > 0:
                    run_apply_edits(
                        URL_BIOMON_EDIT_FS,
                        LAYERS["visits"],
                        df_to_esri_features(df_visits_update, False, True, "GlobalID"),
                        token,
                        True,
                    )
                if len(df_visits_insert) > 0:
                    run_apply_edits(
                        URL_BIOMON_EDIT_FS,
                        LAYERS["visits"],
                        df_to_esri_features(
                            df_visits_insert, True, False, "GlobalID", DEFAULT_WKID
                        ),
                        token,
                        False,
                    )
                    _refresh_visits_existing(con, token)
            else:
                if len(df_visits_insert) > 0:
                    con.execute("""
                        INSERT INTO visits_existing (GlobalID, STATION, VISIT_TYPE, VISIT_DATE, DATA_NOTES, geom)
                        SELECT
                            'PREVIEW_NEW_VISIT_' || STATION || '_' || STRFTIME(VISIT_DATE, '%Y%m%d')
                            , STATION
                            , VISIT_TYPE
                            , VISIT_DATE
                            , DATA_NOTES
                            , GEOM
                        FROM visits
                        WHERE GlobalID IS NULL AND IMPORT_STATUS != 'ERROR';
                    """)

            arcpy.SetProgressorPosition(4)

            # -----------------------------------------------------------------
            # STEP 5 & 6: Supplemental Visits & Fish Counts Processing
            # -----------------------------------------------------------------
            arcpy.SetProgressorLabel("Processing supplemental visits & fish counts...")
            con.execute("""
                CREATE OR REPLACE TABLE fc_visits AS
                WITH fish_agg AS (
                    SELECT
                        SOURCE_FILE
                        , STATION
                        , VISIT_DATE
                        , 'BIF' AS VISIT_TYPE
                    FROM raw_fish_counts
                    GROUP BY SOURCE_FILE, STATION, VISIT_DATE
                )
                SELECT
                    fa.SOURCE_FILE
                    , fa.STATION
                    , fa.VISIT_DATE
                    , fa.VISIT_TYPE
                    , 'DEP' AS FISH_LEADER1
                    , 'DEP' AS FISH_LEADER2
                    , 'DEP' AS FISH_TAXONOMIST1
                    , 'DEP' AS FISH_TAXONOMIST2
                    , 'DEP' AS DATA_ENTERER
                    , CURRENT_DATE AS DATA_ENTER_DATE
                    , COALESCE(ve.DATA_NOTES, '') AS DATA_NOTES
                    , COALESCE(ve.geom, s.geom) AS GEOM
                    , ST_X(COALESCE(ve.geom, s.geom)) AS x
                    , ST_Y(COALESCE(ve.geom, s.geom)) AS y
                    , ve.GlobalID
                FROM fish_agg fa
                LEFT JOIN visits_existing ve
                    ON fa.STATION = ve.STATION
                    AND DATE(fa.VISIT_DATE) = DATE(ve.VISIT_DATE)
                    AND fa.VISIT_TYPE = ve.VISIT_TYPE
                LEFT JOIN stations s
                    ON fa.STATION = s.STATION
                WHERE ve.GlobalID IS NULL
                    AND hash(fa.STATION, fa.VISIT_DATE, fa.VISIT_TYPE) NOT IN (
                        SELECT hash(v.STATION, v.VISIT_DATE, v.VISIT_TYPE) FROM visits v WHERE IMPORT_STATUS != 'ERROR'
                    );

                CREATE OR REPLACE TABLE bc_visits AS
                WITH benthic_agg AS (
                    SELECT
                        SOURCE_FILE
                        , STATION
                        , VISIT_DATE
                        , 'BIB' AS VISIT_TYPE
                    FROM raw_benthics_counts
                    GROUP BY SOURCE_FILE, STATION, VISIT_DATE
                )
                SELECT
                    ba.SOURCE_FILE
                    , ba.STATION
                    , ba.VISIT_DATE
                    , ba.VISIT_TYPE
                    , 'DEP' AS BEN_SAMPLER
                    , 'DEP' AS BEN_SUBSAMPLER
                    , 'DEP' AS DATA_ENTERER
                    , CURRENT_DATE AS DATA_ENTER_DATE
                    , COALESCE(ve.DATA_NOTES, '') AS DATA_NOTES
                    , COALESCE(ve.geom, s.geom) AS GEOM
                    , ST_X(COALESCE(ve.geom, s.geom)) AS x
                    , ST_Y(COALESCE(ve.geom, s.geom)) AS y
                    , ve.GlobalID
                FROM benthic_agg ba
                LEFT JOIN visits_existing ve
                    ON ba.STATION = ve.STATION
                    AND DATE(ba.VISIT_DATE) = DATE(ve.VISIT_DATE)
                    AND ba.VISIT_TYPE = ve.VISIT_TYPE
                LEFT JOIN stations s
                    ON ba.STATION = s.STATION
                WHERE ve.GlobalID IS NULL
                    AND hash(ba.STATION, ba.VISIT_DATE, ba.VISIT_TYPE) NOT IN (
                        SELECT hash(v.STATION, v.VISIT_DATE, v.VISIT_TYPE) FROM visits v WHERE IMPORT_STATUS != 'ERROR'
                    );
            """)

            df_visits_insert_fc = con.sql(
                "SELECT * EXCLUDE (GlobalID) FROM fc_visits WHERE GlobalID IS NULL AND GEOM IS NOT NULL"
            ).df()
            df_visits_insert_bc = con.sql(
                "SELECT * EXCLUDE (GlobalID) FROM bc_visits WHERE GlobalID IS NULL AND GEOM IS NOT NULL"
            ).df()

            if not validate_only:
                if len(df_visits_insert_fc) > 0:
                    run_apply_edits(
                        URL_BIOMON_EDIT_FS,
                        LAYERS["visits"],
                        df_to_esri_features(
                            df_visits_insert_fc, True, False, "GlobalID", DEFAULT_WKID
                        ),
                        token,
                        False,
                    )
                if len(df_visits_insert_bc) > 0:
                    run_apply_edits(
                        URL_BIOMON_EDIT_FS,
                        LAYERS["visits"],
                        df_to_esri_features(
                            df_visits_insert_bc, True, False, "GlobalID", DEFAULT_WKID
                        ),
                        token,
                        False,
                    )
                if len(df_visits_insert_fc) > 0 or len(df_visits_insert_bc) > 0:
                    _refresh_visits_existing(con, token)
            else:
                if len(df_visits_insert_fc) > 0:
                    con.execute("""
                        INSERT INTO visits_existing (GlobalID, STATION, VISIT_TYPE, VISIT_DATE, DATA_NOTES, geom)
                        SELECT
                            'PREVIEW_NEW_VISIT_' || STATION || '_' || STRFTIME(VISIT_DATE, '%Y%m%d')
                            , STATION
                            , VISIT_TYPE
                            , CAST(VISIT_DATE AS TIMESTAMP) + INTERVAL 12 HOUR
                            , DATA_NOTES
                            , GEOM
                        FROM fc_visits
                        WHERE GlobalID IS NULL AND GEOM IS NOT NULL;
                    """)

            arcpy.SetProgressorPosition(5)

            df_fishcounts = featureservice_to_df(
                f"{URL_BIOMON_EDIT_FS}/{LAYERS['fish_counts']}",
                out_fields=["GlobalID", "GUID", "FC_SPECIES"],
                token=token,
            )
            if not df_fishcounts.empty:
                con.register("df_fishcounts", df_fishcounts)
                con.execute(
                    "CREATE OR REPLACE TABLE fish_counts_existing AS SELECT * FROM df_fishcounts;"
                )
            else:
                con.execute(
                    "CREATE OR REPLACE TABLE fish_counts_existing (GlobalID VARCHAR, GUID VARCHAR, FC_SPECIES VARCHAR);"
                )

            con.execute("""
                CREATE OR REPLACE TABLE transformed_fish_counts AS
                WITH split_anomalies AS (
                    SELECT
                        SOURCE_FILE
                        , row_num
                        , UNNEST(STRING_SPLIT(REPLACE(UPPER(ANOMALIES), '/', ','), ',')) AS token
                        , TOTAL_ANOMALIES
                    FROM raw_fish_counts
                    WHERE ANOMALIES IS NOT NULL
                ),
                parse_anomalies AS (
                    SELECT
                        row_num
                        , TRIM(token) AS token_clean
                        , REGEXP_EXTRACT(TRIM(token), '^[0-9]+') AS num_part
                        , REGEXP_EXTRACT(TRIM(token), '[A-Z_]+') AS code_part
                        , TOTAL_ANOMALIES
                    FROM split_anomalies
                    WHERE TRIM(token) != ''
                ),
                agg_anomalies AS (
                    SELECT
                        row_num
                        , code_part
                        , COALESCE(CAST(NULLIF(num_part, '') AS INT), TOTAL_ANOMALIES) AS count_val
                    FROM parse_anomalies
                    WHERE code_part != ''
                ),
                pivot_anomalies AS (
                    SELECT
                        row_num
                        , SUM(CASE WHEN code_part = 'BL' THEN count_val ELSE 0 END) AS BL
                        , SUM(CASE WHEN code_part = 'RE' THEN count_val ELSE 0 END) AS RE
                        , SUM(CASE WHEN code_part = 'LE' THEN count_val ELSE 0 END) AS LE
                        , SUM(CASE WHEN code_part = 'GR' THEN count_val ELSE 0 END) AS GR
                        , SUM(CASE WHEN code_part = 'UL' THEN count_val ELSE 0 END) AS UL
                        , SUM(CASE WHEN code_part = 'DV' THEN count_val ELSE 0 END) AS DV
                        , SUM(CASE WHEN code_part = 'DM' THEN count_val ELSE 0 END) AS DM
                        , SUM(CASE WHEN code_part = 'SC' THEN count_val ELSE 0 END) AS SC
                        , SUM(CASE WHEN code_part = 'RS' THEN count_val ELSE 0 END) AS RS
                        , SUM(CASE WHEN code_part = 'DI' THEN count_val ELSE 0 END) AS DI
                        , SUM(CASE WHEN code_part = 'HM' THEN count_val ELSE 0 END) AS HM
                        , SUM(CASE WHEN code_part = 'BS' THEN count_val ELSE 0 END) AS BS
                        , SUM(CASE WHEN code_part = 'AW' THEN count_val ELSE 0 END) AS AW
                        , SUM(CASE WHEN code_part = 'EP' THEN count_val ELSE 0 END) AS EP
                        , SUM(CASE WHEN code_part = 'IK' THEN count_val ELSE 0 END) AS IK
                        , SUM(CASE WHEN code_part = 'FU' THEN count_val ELSE 0 END) AS FU
                        , SUM(CASE WHEN code_part = 'FI' THEN count_val ELSE 0 END) AS FI
                        , SUM(CASE WHEN code_part = 'CL' THEN count_val ELSE 0 END) AS CL
                        , SUM(CASE WHEN code_part = 'FD' THEN count_val ELSE 0 END) AS FD
                        , SUM(CASE WHEN code_part = 'HK' THEN count_val ELSE 0 END) AS HK
                        , SUM(CASE WHEN code_part = 'CT' THEN count_val ELSE 0 END) AS CT
                        , SUM(CASE WHEN code_part = 'AN' THEN count_val ELSE 0 END) AS AN
                        , SUM(CASE WHEN code_part = 'EC' THEN count_val ELSE 0 END) AS EC
                        , SUM(CASE WHEN code_part = 'EH' THEN count_val ELSE 0 END) AS EH
                        , SUM(CASE WHEN code_part = 'PO' THEN count_val ELSE 0 END) AS PO
                        , SUM(CASE WHEN code_part = 'OR_' THEN count_val ELSE 0 END) AS OR_
                        , SUM(CASE WHEN code_part = 'NO' THEN count_val ELSE 0 END) AS NO
                        , SUM(CASE WHEN code_part = 'CA' THEN count_val ELSE 0 END) AS CA
                        , SUM(CASE WHEN code_part = 'OT' THEN count_val ELSE 0 END) AS OT
                    FROM agg_anomalies
                    GROUP BY row_num
                )
                SELECT
                    r.SOURCE_FILE
                    , r.STATION
                    , r.VISIT_DATE
                    , r.FC_SPECIES
                    , r.PASS_1
                    , r.PASS_2
                    , r.TOTAL_COUNT
                    , r.TOTAL_ANOMALIES
                    , r.ANOMALIES
                    , COALESCE(p.BL, 0) AS BL
                    , COALESCE(p.RE, 0) AS RE
                    , COALESCE(p.LE, 0) AS LE
                    , COALESCE(p.GR, 0) AS GR
                    , COALESCE(p.UL, 0) AS UL
                    , COALESCE(p.DV, 0) AS DV
                    , COALESCE(p.DM, 0) AS DM
                    , COALESCE(p.SC, 0) AS SC
                    , COALESCE(p.RS, 0) AS RS
                    , COALESCE(p.DI, 0) AS DI
                    , COALESCE(p.HM, 0) AS HM
                    , COALESCE(p.BS, 0) AS BS
                    , COALESCE(p.AW, 0) AS AW
                    , COALESCE(p.EP, 0) AS EP
                    , COALESCE(p.IK, 0) AS IK
                    , COALESCE(p.FU, 0) AS FU
                    , COALESCE(p.FI, 0) AS FI
                    , COALESCE(p.CL, 0) AS CL
                    , COALESCE(p.FD, 0) AS FD
                    , COALESCE(p.HK, 0) AS HK
                    , COALESCE(p.CT, 0) AS CT
                    , COALESCE(p.AN, 0) AS AN
                    , COALESCE(p.EC, 0) AS EC
                    , COALESCE(p.EH, 0) AS EH
                    , COALESCE(p.PO, 0) AS PO
                    , COALESCE(p.OR_, 0) AS OR_
                    , COALESCE(p.NO, 0) AS NO
                    , COALESCE(p.CA, 0) AS CA
                    , COALESCE(p.OT, 0) AS OT
                    , 'Yes' AS IND_ANOMALIES
                    , ve.GlobalID AS GUID
                    , fc.GlobalID
                FROM raw_fish_counts r
                LEFT JOIN pivot_anomalies p
                    ON r.row_num = p.row_num
                LEFT JOIN visits_existing ve
                    ON r.STATION = ve.STATION
                    AND DATE(r.VISIT_DATE) = DATE(ve.VISIT_DATE)
                LEFT JOIN fish_counts_existing fc
                    ON ve.GlobalID = fc.GUID
                    AND r.FC_SPECIES = fc.FC_SPECIES
                ORDER BY r.row_num;
            """)

            df_fish_upd = con.sql(
                "SELECT * FROM transformed_fish_counts WHERE GlobalID IS NOT NULL"
            ).df()
            df_fish_ins = con.sql(
                "SELECT * EXCLUDE (GlobalID) FROM transformed_fish_counts WHERE GlobalID IS NULL"
            ).df()

            if not validate_only:
                if len(df_fish_upd) > 0:
                    run_apply_edits(
                        URL_BIOMON_EDIT_FS,
                        LAYERS["fish_counts"],
                        df_to_esri_features(df_fish_upd, False, True, "GlobalID"),
                        token,
                        True,
                    )
                if len(df_fish_ins) > 0:
                    run_apply_edits(
                        URL_BIOMON_EDIT_FS,
                        LAYERS["fish_counts"],
                        df_to_esri_features(df_fish_ins, False, False, "GlobalID"),
                        token,
                        False,
                    )
                logger.info(
                    "Biological Monitoring Excel import completed successfully."
                )
            else:
                preview_outputs = []
                exclude_cols = {"x", "y", "row_num"}

                path_v_upd = duckdb_to_memory(
                    con,
                    "SELECT * FROM visits WHERE GlobalID IS NOT NULL",
                    "preview_visits_update",
                    DEFAULT_WKID,
                    active_map,
                    exclude_cols=exclude_cols,
                )
                if path_v_upd:
                    preview_outputs.append(path_v_upd)

                visits_union = """
                    SELECT
                        'PhysChem / Rhab' AS PREVIEW_SOURCE
                        , IMPORT_STATUS
                        , IMPORT_MESSAGE
                        , * EXCLUDE (GlobalID, IMPORT_STATUS, IMPORT_MESSAGE)
                    FROM visits
                    WHERE GlobalID IS NULL AND GEOM IS NOT NULL
                    UNION ALL BY NAME
                    SELECT
                        'Supplemental Fish (BIF)' AS PREVIEW_SOURCE
                        , 'OK' AS IMPORT_STATUS
                        , 'Supplemental visit required for fish counts.' AS IMPORT_MESSAGE
                        , * EXCLUDE (GlobalID)
                    FROM fc_visits
                    WHERE GlobalID IS NULL AND GEOM IS NOT NULL
                    UNION ALL BY NAME
                    SELECT
                        'Supplemental Benthics (BIB)' AS PREVIEW_SOURCE
                        , 'OK' AS IMPORT_STATUS
                        , 'Supplemental visit required for benthics.' AS IMPORT_MESSAGE
                        , * EXCLUDE (GlobalID)
                    FROM bc_visits
                    WHERE GlobalID IS NULL AND GEOM IS NOT NULL
                """
                path_v_ins = duckdb_to_memory(
                    con,
                    visits_union,
                    "preview_visits_insert",
                    DEFAULT_WKID,
                    active_map,
                    exclude_cols=exclude_cols,
                )
                if path_v_ins:
                    preview_outputs.append(path_v_ins)

                path_fc_upd = duckdb_to_memory(
                    con,
                    "SELECT 'OK' AS IMPORT_STATUS, 'Ready' AS IMPORT_MESSAGE, * FROM transformed_fish_counts WHERE GlobalID IS NOT NULL",
                    "preview_fish_counts_update",
                    DEFAULT_WKID,
                    active_map,
                    exclude_cols=exclude_cols,
                )
                if path_fc_upd:
                    preview_outputs.append(path_fc_upd)

                path_fc_ins = duckdb_to_memory(
                    con,
                    "SELECT 'OK' AS IMPORT_STATUS, 'Ready' AS IMPORT_MESSAGE, * EXCLUDE (GlobalID) FROM transformed_fish_counts WHERE GlobalID IS NULL",
                    "preview_fish_counts_insert",
                    DEFAULT_WKID,
                    active_map,
                    exclude_cols=exclude_cols,
                )
                if path_fc_ins:
                    preview_outputs.append(path_fc_ins)

                if preview_outputs:
                    parameters[2].value = preview_outputs
                    if has_validation_errors:
                        if not (
                            upd_errs := con.execute(
                                "SELECT COUNT(*) FROM visits WHERE GlobalID IS NOT NULL AND IMPORT_STATUS = 'ERROR'"
                            ).fetchone()
                        ):
                            upd_errs = 0

                        if not (
                            ins_errs := con.execute(
                                "SELECT COUNT(*) FROM visits WHERE GlobalID IS NULL AND IMPORT_STATUS = 'ERROR'"
                            ).fetchone()
                        ):
                            ins_errs = 0

                        err_details = []
                        if path_v_upd:
                            err_details.append(
                                f"preview_visits_update: {upd_errs} error(s)"
                            )
                        if path_v_ins:
                            err_details.append(
                                f"preview_visits_insert: {ins_errs} error(s)"
                            )
                        if path_fc_upd:
                            err_details.append("preview_fish_counts_update: 0 error(s)")
                        if path_fc_ins:
                            err_details.append("preview_fish_counts_insert: 0 error(s)")

                        details_str = "\n - ".join(err_details)
                        logger.warning(
                            f"Validation complete. Please review the {len(preview_outputs)} preview dataset(s) in the active map for highlighted schema or domain errors:\n"
                            f" - {details_str}"
                        )
                    else:
                        logger.info(
                            f"Validation complete. Added {len(preview_outputs)} preview dataset(s) to the active map."
                        )

            if has_validation_errors:
                arcpy.AddError(
                    "Import failed due to one or more data validation errors detected during processing."
                )
                return

        except Exception as e:
            err_trace = traceback.format_exc()
            logger.error(f"Execution Error encountered: {e}\n{err_trace}")

            try:
                tables = [row[0] for row in con.execute("SHOW TABLES;").fetchall()]
                if "visits" in tables:
                    con.execute(f"""
                        ALTER TABLE visits ADD COLUMN IF NOT EXISTS IMPORT_STATUS VARCHAR;
                        ALTER TABLE visits ADD COLUMN IF NOT EXISTS IMPORT_MESSAGE VARCHAR;
                        UPDATE visits
                        SET IMPORT_STATUS = 'ERROR'
                            , IMPORT_MESSAGE = CONCAT_WS(' | ', NULLIF(IMPORT_MESSAGE, ''), '{str(e).replace("'", "''")}')
                        WHERE IMPORT_STATUS != 'ERROR';
                    """)
                    error_preview = duckdb_to_memory(
                        con,
                        "SELECT * FROM visits WHERE GlobalID IS NULL",
                        "import_error_visits",
                        DEFAULT_WKID,
                        active_map,
                        exclude_cols={"x", "y", "row_num"},
                    )
                    if error_preview:
                        parameters[2].value = [error_preview]
                        logger.warning(
                            f"An error occurred mid-processing. Partial inspection layer written to memory: {error_preview}"
                        )
            except (duckdb.Error, RuntimeError, ValueError) as mem_err:
                logger.error(f"Could not write error state to memory: {mem_err}")

            arcpy.AddError(f"Import failed: {e}")
            return

        arcpy.SetProgressorPosition(6)
        arcpy.ResetProgressor()
