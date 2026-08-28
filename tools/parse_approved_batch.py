from __future__ import annotations

import argparse
import json
import os
import re
import sys
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
import xml.etree.ElementTree as ET

import duckdb
from pypdf import PdfReader

PARSER_VERSION = "2.0.0"
NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
NS_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
NS_PKG_REL = "http://schemas.openxmlformats.org/package/2006/relationships"
NS = {"m": NS_MAIN, "r": NS_REL, "pr": NS_PKG_REL}
MONTH_NAMES = {
    1: "JANUARY", 2: "FEBRUARY", 3: "MARCH", 4: "APRIL",
    5: "MAY", 6: "JUNE", 7: "JULY", 8: "AUGUST",
    9: "SEPTEMBER", 10: "OCTOBER", 11: "NOVEMBER", 12: "DECEMBER",
}
MONTH_ALIASES = {
    "JAN": 1, "JANUARY": 1,
    "FEB": 2, "FEBRUARY": 2,
    "MAR": 3, "MARCH": 3,
    "APR": 4, "APRIL": 4,
    "MAY": 5,
    "JUN": 6, "JUNE": 6,
    "JUL": 7, "JULY": 7,
    "AUG": 8, "AUGUST": 8,
    "SEP": 9, "SEPT": 9, "SEPTEMBER": 9,
    "OCT": 10, "OCTOBER": 10,
    "NOV": 11, "NOVEMBER": 11,
    "DEC": 12, "DECEMBER": 12,
}
DIVISION_ALIASES = {
    "BW": "BEARWELL",
    "BEARWELL": "BEARWELL",
    "BG": "BELGRAVIA",
    "BELGRAVIA": "BELGRAVIA",
    "FF": "FAIRFIELD",
    "FAIRFIELD": "FAIRFIELD",
    "WA": "WALAHA",
    "WH": "WALAHA",
    "WALAHA": "WALAHA",
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\u00a0", " ").strip()
    return re.sub(r"\s+", " ", text)


def clean_label(value: Any) -> str:
    text = clean_text(value).strip(":- ")
    return text


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = clean_text(value).replace(",", "")
    if not text or text.startswith("#"):
        return None
    if text.endswith("%"):
        text = text[:-1]
    try:
        return float(text)
    except ValueError:
        match = re.fullmatch(r"[-+]?\d+(?:\.\d+)?", text)
        return float(match.group(0)) if match else None


def to_int(value: Any) -> int | None:
    number = to_float(value)
    if number is None:
        return None
    return int(number)


def excel_serial_to_date(value: Any) -> str | None:
    number = to_float(value)
    if number is None or number < 1 or number > 100000:
        return None
    try:
        result = datetime(1899, 12, 30) + timedelta(days=number)
        return result.date().isoformat()
    except (OverflowError, ValueError):
        return None


def col_to_num(letters: str) -> int:
    result = 0
    for char in letters:
        result = result * 26 + (ord(char) - 64)
    return result


def num_to_col(number: int) -> str:
    result = ""
    while number:
        number, remainder = divmod(number - 1, 26)
        result = chr(65 + remainder) + result
    return result


def split_ref(cell_ref: str) -> tuple[str, int]:
    match = re.fullmatch(r"([A-Z]+)(\d+)", cell_ref)
    if not match:
        return "", 0
    return match.group(1), int(match.group(2))


@dataclass
class Cell:
    ref: str
    value: str
    formula: str | None
    cell_type: str | None

    @property
    def number(self) -> float | None:
        return to_float(self.value)


class XlsxReader:
    def __init__(self, path: Path):
        self.path = path
        self.archive = zipfile.ZipFile(path)
        self.shared_strings = self._load_shared_strings()
        self.sheets = self._load_sheets()

    def close(self) -> None:
        self.archive.close()

    def __enter__(self) -> "XlsxReader":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def _load_shared_strings(self) -> list[str]:
        if "xl/sharedStrings.xml" not in self.archive.namelist():
            return []
        root = ET.fromstring(self.archive.read("xl/sharedStrings.xml"))
        result: list[str] = []
        for item in root.findall("m:si", NS):
            result.append(
                "".join(
                    node.text or ""
                    for node in item.iter(f"{{{NS_MAIN}}}t")
                )
            )
        return result

    def _load_sheets(self) -> dict[str, str]:
        workbook = ET.fromstring(self.archive.read("xl/workbook.xml"))
        relationships = ET.fromstring(
            self.archive.read("xl/_rels/workbook.xml.rels")
        )
        relationship_map = {
            item.attrib["Id"]: item.attrib["Target"]
            for item in relationships
        }
        result: dict[str, str] = {}
        sheets = workbook.find("m:sheets", NS)
        if sheets is None:
            return result
        for sheet in sheets:
            name = sheet.attrib["name"]
            relation_id = sheet.attrib[f"{{{NS_REL}}}id"]
            target = relationship_map[relation_id]
            if not target.startswith("xl/"):
                target = "xl/" + target.lstrip("/")
            result[name] = target
        return result

    def sheet_names(self) -> list[str]:
        return list(self.sheets)

    def iter_rows(self, sheet_name: str) -> Iterable[tuple[int, dict[str, Cell]]]:
        target = self.sheets[sheet_name]
        with self.archive.open(target) as source:
            for event, element in ET.iterparse(source, events=("end",)):
                if element.tag != f"{{{NS_MAIN}}}row":
                    continue
                row_number = int(element.attrib.get("r", "0") or "0")
                row: dict[str, Cell] = {}
                for cell_element in element.findall("m:c", NS):
                    ref = cell_element.attrib.get("r", "")
                    column, _ = split_ref(ref)
                    cell_type = cell_element.attrib.get("t")
                    value_node = cell_element.find("m:v", NS)
                    formula_node = cell_element.find("m:f", NS)
                    inline_node = cell_element.find("m:is", NS)
                    value = ""
                    if cell_type == "s" and value_node is not None:
                        try:
                            value = self.shared_strings[int(value_node.text or "0")]
                        except (ValueError, IndexError):
                            value = value_node.text or ""
                    elif cell_type == "inlineStr" and inline_node is not None:
                        value = "".join(
                            node.text or ""
                            for node in inline_node.iter(f"{{{NS_MAIN}}}t")
                        )
                    elif value_node is not None:
                        value = value_node.text or ""
                    formula = formula_node.text if formula_node is not None else None
                    if column and (value != "" or formula is not None):
                        row[column] = Cell(
                            ref=ref,
                            value=value,
                            formula=formula,
                            cell_type=cell_type,
                        )
                if row:
                    yield row_number, row
                element.clear()

    def read_rows(self, sheet_name: str) -> dict[int, dict[str, Cell]]:
        return {row_number: row for row_number, row in self.iter_rows(sheet_name)}


def cell_value(row: dict[str, Cell], column: str) -> str:
    cell = row.get(column)
    return clean_text(cell.value) if cell else ""


def cell_number(row: dict[str, Cell], column: str) -> float | None:
    cell = row.get(column)
    return cell.number if cell else None


def find_row(
    rows: dict[int, dict[str, Cell]],
    predicate,
) -> int | None:
    for row_number in sorted(rows):
        if predicate(rows[row_number]):
            return row_number
    return None


def normalize_division(value: Any) -> str | None:
    text = clean_label(value).upper().replace("DIVISION", "").strip()
    text = re.sub(r"[^A-Z]+", " ", text).strip()
    for token, division in DIVISION_ALIASES.items():
        if text == token or token in text.split():
            return division
    for division in ("BEARWELL", "BELGRAVIA", "FAIRFIELD", "WALAHA"):
        if division in text:
            return division
    return None


def reporting_month_parts(reporting_month: str) -> tuple[int, int]:
    year, month = reporting_month.split("-")
    return int(year), int(month)


def ensure_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS batch_analytics (
            batch_id VARCHAR PRIMARY KEY,
            estate_name VARCHAR NOT NULL,
            reporting_month VARCHAR NOT NULL,
            analytics_status VARCHAR NOT NULL,
            parser_version VARCHAR NOT NULL,
            parsed_at TIMESTAMP,
            metrics_approved_at TIMESTAMP,
            source_file_count INTEGER NOT NULL,
            parsed_record_count BIGINT NOT NULL,
            error_count INTEGER NOT NULL,
            warning_count INTEGER NOT NULL,
            summary_json VARCHAR NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS parse_issues (
            batch_id VARCHAR,
            severity VARCHAR,
            category VARCHAR,
            file_name VARCHAR,
            sheet_name VARCHAR,
            message VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS source_sheet_inventory (
            batch_id VARCHAR,
            category VARCHAR,
            file_name VARCHAR,
            sheet_name VARCHAR,
            non_empty_row_count BIGINT,
            non_empty_cell_count BIGINT,
            max_row BIGINT,
            max_column BIGINT
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_sheet_rows (
            batch_id VARCHAR,
            category VARCHAR,
            file_name VARCHAR,
            sheet_name VARCHAR,
            row_number BIGINT,
            row_json VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS source_pdf_text (
            batch_id VARCHAR,
            category VARCHAR,
            file_name VARCHAR,
            division_name VARCHAR,
            page_count INTEGER,
            text_content VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS g_report_metrics (
            batch_id VARCHAR,
            estate_name VARCHAR,
            reporting_month VARCHAR,
            division_name VARCHAR,
            vp_extent_ha DOUBLE,
            ost_extent_ha DOUBLE,
            total_division_extent_ha DOUBLE,
            total_estate_extent_ha DOUBLE,
            estate_share_pct DOUBLE,
            total_green_leaf_kg DOUBLE,
            total_checkroll_names DOUBLE,
            average_plucking_kg DOUBLE,
            total_vp_hectare_plucked DOUBLE,
            total_hectare_plucked DOUBLE,
            source_file VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS plucking_plan_fields (
            batch_id VARCHAR,
            estate_name VARCHAR,
            reporting_month VARCHAR,
            division_name VARCHAR,
            field_no VARCHAR,
            extent_ha DOUBLE,
            vp_sd VARCHAR,
            field_age VARCHAR,
            anticipated_rounds DOUBLE,
            anticipated_pph DOUBLE,
            anticipated_plucking_average DOUBLE,
            anticipated_total_pluckers DOUBLE,
            anticipated_green_leaf_kg DOUBLE,
            anticipated_green_leaf_per_ha DOUBLE,
            actual_rounds DOUBLE,
            actual_lph DOUBLE,
            actual_plucking_average DOUBLE,
            actual_total_pluckers DOUBLE,
            actual_green_leaf_kg DOUBLE,
            actual_green_leaf_per_ha DOUBLE,
            source_file VARCHAR,
            source_sheet VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS non_wage_plucking_fields (
            batch_id VARCHAR,
            estate_name VARCHAR,
            reporting_month VARCHAR,
            division_name VARCHAR,
            field_no VARCHAR,
            extent_ha DOUBLE,
            field_age VARCHAR,
            division_pct DOUBLE,
            bm_yph DOUBLE,
            green_leaf_year DOUBLE,
            round1_estimate DOUBLE,
            round1_actual DOUBLE,
            round2_estimate DOUBLE,
            round2_actual DOUBLE,
            round3_estimate DOUBLE,
            round3_actual DOUBLE,
            round4_estimate DOUBLE,
            round4_actual DOUBLE,
            total_estimate DOUBLE,
            total_actual DOUBLE,
            source_file VARCHAR,
            source_sheet VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS agricultural_activity_records (
            batch_id VARCHAR,
            estate_name VARCHAR,
            reporting_month VARCHAR,
            division_name VARCHAR,
            field_no VARCHAR,
            extent_ha DOUBLE,
            vp_sd VARCHAR,
            field_age VARCHAR,
            budget_yph DOUBLE,
            actual_yph DOUBLE,
            activity_type VARCHAR,
            activity_month VARCHAR,
            activity_value_text VARCHAR,
            activity_value_number DOUBLE,
            source_file VARCHAR,
            source_sheet VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS contract_records (
            batch_id VARCHAR,
            estate_name VARCHAR,
            reporting_month VARCHAR,
            division_name VARCHAR,
            period_label VARCHAR,
            field_no VARCHAR,
            work_type VARCHAR,
            labour_allocated DOUBLE,
            labour_total_cost_rs DOUBLE,
            converted_lph DOUBLE,
            hectares_awarded DOUBLE,
            contractor_name VARCHAR,
            contract_value DOUBLE,
            field_officer VARCHAR,
            assistant_manager VARCHAR,
            manager VARCHAR,
            last_date VARCHAR,
            source_file VARCHAR,
            source_sheet VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS crop_field_performance (
            batch_id VARCHAR,
            estate_name VARCHAR,
            reporting_month VARCHAR,
            division_name VARCHAR,
            field_no VARCHAR,
            vp_sd VARCHAR,
            extent_ha DOUBLE,
            made_tea_crop_kg DOUBLE,
            yield_kg_per_ha DOUBLE,
            source_file VARCHAR,
            source_sheet VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS crop_division_summary (
            batch_id VARCHAR,
            estate_name VARCHAR,
            reporting_month VARCHAR,
            division_name VARCHAR,
            extent_ha DOUBLE,
            made_tea_crop_kg DOUBLE,
            yield_kg_per_ha DOUBLE,
            source_file VARCHAR,
            source_sheet VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS plucker_performance (
            batch_id VARCHAR,
            estate_name VARCHAR,
            reporting_month VARCHAR,
            division_name VARCHAR,
            employee_name VARCHAR,
            employee_code VARCHAR,
            days_offered DOUBLE,
            days_attended DOUBLE,
            green_leaf_kg DOUBLE,
            average_kg DOUBLE,
            source_file VARCHAR,
            source_sheet VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS cop_line_items (
            batch_id VARCHAR,
            estate_name VARCHAR,
            reporting_month VARCHAR,
            line_number BIGINT,
            description VARCHAR,
            rupees_per_kg DOUBLE,
            cost_share DOUBLE,
            cost_per_hectare DOUBLE,
            source_file VARCHAR,
            source_sheet VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS revenue_grade_sales (
            batch_id VARCHAR,
            estate_name VARCHAR,
            reporting_month VARCHAR,
            broker VARCHAR,
            invoice_no VARCHAR,
            grade VARCHAR,
            quantity_kg DOUBLE,
            month_manufactured VARCHAR,
            month_sold VARCHAR,
            price_rs_per_kg DOUBLE,
            total_proceeds_rs DOUBLE,
            buyer VARCHAR,
            source_file VARCHAR,
            source_sheet VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS invoice_sales (
            batch_id VARCHAR,
            estate_name VARCHAR,
            reporting_month VARCHAR,
            invoice_date VARCHAR,
            invoice_no VARCHAR,
            grade VARCHAR,
            sold_quantity_kg DOUBLE,
            net_proceeds_rs DOUBLE,
            despatch_document_no VARCHAR,
            despatch_date VARCHAR,
            source_file VARCHAR,
            source_sheet VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS break_even_metrics (
            batch_id VARCHAR,
            estate_name VARCHAR,
            reporting_month VARCHAR,
            metric_period VARCHAR,
            total_fixed_cost_rs DOUBLE,
            elevation_average_rs_per_kg DOUBLE,
            variable_cost_rs_per_kg DOUBLE,
            contribution_rs_per_kg DOUBLE,
            quantity_to_cover_fixed_cost_kg DOUBLE,
            budgeted_nsa_rs_per_kg DOUBLE,
            budget_contribution_rs_per_kg DOUBLE,
            break_even_quantity_kg DOUBLE,
            budget_crop_kg DOUBLE,
            budget_cop_rs_per_kg DOUBLE,
            tea_profit_rs DOUBLE,
            break_even_pct_of_budget DOUBLE,
            monthly_break_even_quantity_kg DOUBLE,
            source_file VARCHAR,
            source_sheet VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS m_format_monthly_yph (
            batch_id VARCHAR,
            estate_name VARCHAR,
            reporting_month VARCHAR,
            division_name VARCHAR,
            field_no VARCHAR,
            extent_ha DOUBLE,
            metric_year INTEGER,
            metric_month INTEGER,
            yph DOUBLE,
            nitrogen DOUBLE,
            source_file VARCHAR,
            source_sheet VARCHAR
        )
        """
    )


def clear_batch(connection: duckdb.DuckDBPyConnection, batch_id: str) -> None:
    tables = [
        "parse_issues", "source_sheet_inventory", "raw_sheet_rows",
        "source_pdf_text", "g_report_metrics", "plucking_plan_fields",
        "non_wage_plucking_fields", "agricultural_activity_records",
        "contract_records", "crop_field_performance", "crop_division_summary",
        "plucker_performance", "cop_line_items", "revenue_grade_sales",
        "invoice_sales", "break_even_metrics", "m_format_monthly_yph",
        "batch_analytics",
    ]
    for table in tables:
        connection.execute(f"DELETE FROM {table} WHERE batch_id = ?", [batch_id])


def add_issue(
    issues: list[dict[str, Any]],
    severity: str,
    category: str,
    file_name: str,
    message: str,
    sheet_name: str | None = None,
) -> None:
    issues.append({
        "severity": severity,
        "category": category,
        "file_name": file_name,
        "sheet_name": sheet_name,
        "message": message,
    })


def capture_workbook_rows(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
    category: str,
    file_name: str,
    book: XlsxReader,
) -> dict[str, dict[int, dict[str, Cell]]]:
    loaded: dict[str, dict[int, dict[str, Cell]]] = {}
    for sheet_name in book.sheet_names():
        rows: dict[int, dict[str, Cell]] = {}
        row_records: list[list[Any]] = []
        non_empty_cells = 0
        max_row = 0
        max_column = 0
        for row_number, row in book.iter_rows(sheet_name):
            rows[row_number] = row
            max_row = max(max_row, row_number)
            non_empty_cells += len(row)
            for column in row:
                max_column = max(max_column, col_to_num(column))
            row_json = {
                column: {
                    "value": cell.value,
                    "formula": cell.formula,
                    "type": cell.cell_type,
                }
                for column, cell in row.items()
            }
            row_records.append([
                batch_id, category, file_name, sheet_name,
                row_number, json.dumps(row_json, ensure_ascii=False),
            ])
            if len(row_records) >= 1000:
                connection.executemany(
                    "INSERT INTO raw_sheet_rows VALUES (?, ?, ?, ?, ?, ?)",
                    row_records,
                )
                row_records.clear()
        if row_records:
            connection.executemany(
                "INSERT INTO raw_sheet_rows VALUES (?, ?, ?, ?, ?, ?)",
                row_records,
            )
        connection.execute(
            "INSERT INTO source_sheet_inventory VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                batch_id, category, file_name, sheet_name,
                len(rows), non_empty_cells, max_row, max_column,
            ],
        )
        loaded[sheet_name] = rows
    return loaded


def parse_g_report(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
    estate_name: str,
    reporting_month: str,
    file_name: str,
    division_name: str | None,
    path: Path,
    issues: list[dict[str, Any]],
) -> int:
    reader = PdfReader(str(path))
    page_texts: list[str] = []
    for page in reader.pages:
        try:
            page_texts.append(page.extract_text(extraction_mode="layout") or "")
        except TypeError:
            page_texts.append(page.extract_text() or "")
    text = "\n".join(page_texts)
    connection.execute(
        "INSERT INTO source_pdf_text VALUES (?, ?, ?, ?, ?, ?)",
        [batch_id, "G_REPORT_PDF", file_name, division_name, len(reader.pages), text],
    )

    def regex_number(pattern: str) -> float | None:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        return to_float(match.group(1)) if match else None

    def last_number_on_line(label: str) -> float | None:
        line = next(
            (item for item in text.splitlines() if label in item.upper()),
            None,
        )
        if not line:
            return None
        values = re.findall(
            r"(?<![\d.])\d+(?:\.\d+)?(?![\d.])",
            line,
        )
        return to_float(values[-1]) if values else None

    total_line = next(
        (
            item
            for item in text.splitlines()
            if re.match(r"^\s*TOTAL\s", item, flags=re.IGNORECASE)
            and "%" in item
        ),
        None,
    )
    total_green_leaf = None
    if total_line:
        numbers = [
            float(value)
            for value in re.findall(
                r"(?<![\d.])\d+(?:\.\d+)?(?![\d.])",
                total_line,
            )
        ]
        if len(numbers) > 2:
            total_green_leaf = max(numbers[2:])
    if total_green_leaf is None:
        add_issue(
            issues, "WARNING", "G_REPORT_PDF", file_name,
            "Could not identify total green leaf from the G report.",
        )

    connection.execute(
        """
        INSERT INTO g_report_metrics VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        [
            batch_id, estate_name, reporting_month, division_name,
            regex_number(r"VP EXTENT\s+([0-9.]+)"),
            regex_number(r"OST EXTENT\s+([0-9.]+)"),
            regex_number(r"TOTAL DIV EXTENT\s+([0-9.]+)"),
            regex_number(r"TOTAL ESTATE EXTENT\s+([0-9.]+)"),
            regex_number(r"% OF ESTATE\s+([0-9.]+)"),
            total_green_leaf,
            last_number_on_line("TOTAL CHECKROLL NAMES"),
            last_number_on_line("PLUCKING AVERAGE"),
            last_number_on_line("TOTAL VP HECTARE PLUCKED"),
            last_number_on_line("TOTAL HECTARE PLUCKED"),
            file_name,
        ],
    )
    return 2


def parse_plucking_plan(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
    estate_name: str,
    reporting_month: str,
    file_name: str,
    sheets: dict[str, dict[int, dict[str, Cell]]],
) -> int:
    records: list[list[Any]] = []
    for sheet_name, rows in sheets.items():
        if "MD1" not in sheet_name.upper():
            continue
        division = None
        for row_number in sorted(rows):
            row = rows[row_number]
            for column in ("E", "A"):
                candidate = normalize_division(cell_value(row, column))
                if candidate:
                    division = candidate
                    break
            if division:
                break
        division = division or normalize_division(sheet_name)
        for row_number in sorted(rows):
            row = rows[row_number]
            field_no = clean_label(cell_value(row, "A"))
            extent = cell_number(row, "B")
            vp_sd = cell_value(row, "C").upper()
            if not field_no or extent is None or vp_sd not in {"VP", "SD"}:
                continue
            records.append([
                batch_id, estate_name, reporting_month, division,
                field_no, extent, vp_sd, cell_value(row, "E"),
                cell_number(row, "F"), cell_number(row, "G"),
                cell_number(row, "H"), cell_number(row, "I"),
                cell_number(row, "J"), cell_number(row, "K"),
                cell_number(row, "L"), cell_number(row, "M"),
                cell_number(row, "N"), cell_number(row, "O"),
                cell_number(row, "P"), cell_number(row, "Q"),
                file_name, sheet_name,
            ])
    if records:
        connection.executemany(
            """
            INSERT INTO plucking_plan_fields VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            records,
        )
    return len(records)


def parse_non_wage(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
    estate_name: str,
    reporting_month: str,
    file_name: str,
    sheets: dict[str, dict[int, dict[str, Cell]]],
) -> int:
    records: list[list[Any]] = []
    for sheet_name, rows in sheets.items():
        division = normalize_division(sheet_name)
        if not division:
            title = cell_value(rows.get(2, {}), "B")
            division = normalize_division(title)
        for row_number in sorted(rows):
            row = rows[row_number]
            field_no = clean_label(cell_value(row, "B"))
            extent = cell_number(row, "C")
            if not field_no or field_no.upper() == "TOTAL" or extent is None:
                continue
            if row_number < 6:
                continue
            records.append([
                batch_id, estate_name, reporting_month, division,
                field_no, extent, cell_value(row, "D"),
                cell_number(row, "E"), cell_number(row, "F"),
                cell_number(row, "G"), cell_number(row, "H"),
                cell_number(row, "I"), cell_number(row, "J"),
                cell_number(row, "K"), cell_number(row, "L"),
                cell_number(row, "M"), cell_number(row, "N"),
                cell_number(row, "O"), cell_number(row, "P"),
                cell_number(row, "Q"), file_name, sheet_name,
            ])
    if records:
        connection.executemany(
            """
            INSERT INTO non_wage_plucking_fields VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            records,
        )
    return len(records)


def parse_agricultural_activity(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
    estate_name: str,
    reporting_month: str,
    file_name: str,
    sheets: dict[str, dict[int, dict[str, Cell]]],
) -> int:
    selected = None
    for candidate in ("Fertilizer Weed", "Sheet1"):
        if candidate in sheets:
            selected = candidate
            break
    if selected is None:
        return 0
    rows = sheets[selected]
    header_row = find_row(
        rows,
        lambda row: cell_value(row, "B").upper() == "FIELD NO"
        and cell_value(row, "J").upper() == "ITEM",
    )
    if header_row is None:
        return 0
    month_columns: dict[str, str] = {}
    for column, cell in rows[header_row].items():
        month_token = clean_text(cell.value).upper()
        if month_token in MONTH_ALIASES:
            month_columns[column] = f"{MONTH_ALIASES[month_token]:02d}"
    division = None
    for row_number in range(1, header_row):
        for column in ("H", "C"):
            candidate = normalize_division(cell_value(rows.get(row_number, {}), column))
            if candidate:
                division = candidate
    current: dict[str, Any] = {}
    records: list[list[Any]] = []
    for row_number in sorted(rows):
        if row_number <= header_row:
            continue
        row = rows[row_number]
        if cell_value(row, "B"):
            current = {
                "field_no": clean_label(cell_value(row, "B")),
                "extent": cell_number(row, "C"),
                "vp_sd": cell_value(row, "D"),
                "field_age": cell_value(row, "E"),
                "budget_yph": cell_number(row, "G"),
                "actual_yph": cell_number(row, "H"),
            }
        activity_type = cell_value(row, "J")
        if not current or not activity_type:
            continue
        for column, month_number in month_columns.items():
            value_text = cell_value(row, column)
            if not value_text:
                continue
            records.append([
                batch_id, estate_name, reporting_month, division,
                current.get("field_no"), current.get("extent"),
                current.get("vp_sd"), current.get("field_age"),
                current.get("budget_yph"), current.get("actual_yph"),
                activity_type, month_number, value_text, to_float(value_text),
                file_name, selected,
            ])
    if records:
        connection.executemany(
            """
            INSERT INTO agricultural_activity_records VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            records,
        )
    return len(records)


def parse_contracts(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
    estate_name: str,
    reporting_month: str,
    file_name: str,
    sheets: dict[str, dict[int, dict[str, Cell]]],
) -> int:
    records: list[list[Any]] = []
    for sheet_name, rows in sheets.items():
        division = normalize_division(sheet_name)
        if not division:
            continue
        header_row = find_row(
            rows,
            lambda row: "FIELD NO" in cell_value(row, "C").upper()
            and "TYPE OF WORK" in cell_value(row, "D").upper(),
        )
        if header_row is None:
            continue
        title = ""
        for row_number in range(max(1, header_row - 3), header_row):
            title += " " + " ".join(
                cell_value(rows.get(row_number, {}), column)
                for column in ("A", "B", "C", "D")
            )
        title = clean_text(title)
        for row_number in sorted(rows):
            if row_number <= header_row:
                continue
            row = rows[row_number]
            field_no = clean_label(cell_value(row, "C"))
            work_type = cell_value(row, "D")
            if not field_no or not work_type:
                continue
            if "TOTAL" in field_no.upper() or "TOTAL" in work_type.upper():
                continue
            last_date_value = cell_value(row, "N")
            last_date = excel_serial_to_date(last_date_value) or last_date_value
            records.append([
                batch_id, estate_name, reporting_month, division, title,
                field_no, work_type, cell_number(row, "E"),
                cell_number(row, "F"), cell_number(row, "G"),
                cell_number(row, "H"), cell_value(row, "I"),
                cell_number(row, "J"), cell_value(row, "K"),
                cell_value(row, "L"), cell_value(row, "M"),
                last_date, file_name, sheet_name,
            ])
    if records:
        connection.executemany(
            """
            INSERT INTO contract_records VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            records,
        )
    return len(records)


def parse_crop_sheet(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
    default_estate: str,
    reporting_month: str,
    file_name: str,
    rows: dict[int, dict[str, Cell]],
    sheet_name: str,
) -> int:
    inserted = 0
    row_numbers = sorted(rows)
    starts = [
        row_number
        for row_number in row_numbers
        if cell_value(rows[row_number], "B").upper() == "ESTATE"
    ]
    for start in starts:
        estate_name = clean_label(cell_value(rows.get(start, {}), "C")) or default_estate
        division = normalize_division(cell_value(rows.get(start + 1, {}), "C"))
        if not division:
            continue
        header = start + 3
        total_row = None
        for row_number in row_numbers:
            if row_number <= header:
                continue
            if row_number > header + 60:
                break
            if cell_value(rows[row_number], "A").upper() == "TOTAL":
                total_row = row_number
                break
            field_no = clean_label(cell_value(rows[row_number], "B"))
            extent = cell_number(rows[row_number], "D")
            crop = cell_number(rows[row_number], "E")
            if not field_no or extent is None or crop is None:
                continue
            connection.execute(
                "INSERT INTO crop_field_performance VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    batch_id, estate_name, reporting_month, division,
                    field_no, cell_value(rows[row_number], "C"), extent,
                    crop, cell_number(rows[row_number], "H"), file_name, sheet_name,
                ],
            )
            inserted += 1
        if total_row is not None:
            connection.execute(
                "INSERT INTO crop_division_summary VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    batch_id, estate_name, reporting_month, division,
                    cell_number(rows[total_row], "D"),
                    cell_number(rows[total_row], "E"),
                    cell_number(rows[total_row], "H"),
                    file_name, sheet_name,
                ],
            )
            inserted += 1
    return inserted


def parse_pluckers_sheet(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
    default_estate: str,
    reporting_month: str,
    file_name: str,
    rows: dict[int, dict[str, Cell]],
    sheet_name: str,
) -> int:
    inserted = 0
    headers = [
        row_number
        for row_number in sorted(rows)
        if cell_value(rows[row_number], "C").upper() == "NAME"
        and "EMP" in cell_value(rows[row_number], "D").upper()
        and "GREEN LEAF" in cell_value(rows[row_number], "G").upper()
    ]
    for header in headers:
        estate_name = default_estate
        division = None
        for offset in range(1, 7):
            metadata = rows.get(header - offset, {})
            label = cell_value(metadata, "C").upper()
            if label == "ESTATE":
                estate_name = clean_label(cell_value(metadata, "E")) or default_estate
            elif label == "DIVISION":
                division = normalize_division(cell_value(metadata, "E"))
        if not division:
            continue
        for row_number in range(header + 1, header + 150):
            row = rows.get(row_number, {})
            employee_name = cell_value(row, "C")
            employee_code = cell_value(row, "D")
            green_leaf = cell_number(row, "G")
            if not employee_name and not employee_code:
                if row_number > header + 5:
                    break
                continue
            if "TOTAL" in employee_name.upper():
                break
            if green_leaf is None:
                continue
            connection.execute(
                "INSERT INTO plucker_performance VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    batch_id, estate_name, reporting_month, division,
                    employee_name, employee_code, cell_number(row, "E"),
                    cell_number(row, "F"), green_leaf, cell_number(row, "H"),
                    file_name, sheet_name,
                ],
            )
            inserted += 1
    return inserted


def parse_cop_sheet(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
    estate_name: str,
    reporting_month: str,
    file_name: str,
    rows: dict[int, dict[str, Cell]],
    sheet_name: str,
) -> int:
    records: list[list[Any]] = []
    for row_number in sorted(rows):
        row = rows[row_number]
        description = cell_value(row, "B")
        if not description:
            continue
        rs_per_kg = cell_number(row, "C")
        cost_share = cell_number(row, "D")
        cost_per_hectare = cell_number(row, "E")
        if rs_per_kg is None and cost_share is None and cost_per_hectare is None:
            continue
        records.append([
            batch_id, estate_name, reporting_month, row_number,
            description, rs_per_kg, cost_share, cost_per_hectare,
            file_name, sheet_name,
        ])
    if records:
        connection.executemany(
            "INSERT INTO cop_line_items VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            records,
        )
    return len(records)


def parse_revenue_sheet(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
    estate_name: str,
    reporting_month: str,
    file_name: str,
    rows: dict[int, dict[str, Cell]],
    sheet_name: str,
) -> int:
    header = find_row(
        rows,
        lambda row: cell_value(row, "A").upper() == "BROKER"
        and cell_value(row, "C").upper() == "GRADE"
        and cell_value(row, "D").upper() == "QUANTITY",
    )
    if header is None:
        return 0
    records: list[list[Any]] = []
    for row_number in sorted(rows):
        if row_number <= header:
            continue
        row = rows[row_number]
        quantity = cell_number(row, "D")
        grade = cell_value(row, "C")
        if not grade or quantity is None:
            continue
        if "TOTAL" in grade.upper():
            continue
        records.append([
            batch_id, estate_name, reporting_month,
            cell_value(row, "A"), cell_value(row, "B"), grade,
            quantity, cell_value(row, "E"), cell_value(row, "F"),
            cell_number(row, "G"), cell_number(row, "H"),
            cell_value(row, "K"), file_name, sheet_name,
        ])
    if records:
        connection.executemany(
            "INSERT INTO revenue_grade_sales VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            records,
        )
    return len(records)


def parse_invoice_sheet(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
    estate_name: str,
    reporting_month: str,
    file_name: str,
    rows: dict[int, dict[str, Cell]],
    sheet_name: str,
) -> int:
    header = find_row(
        rows,
        lambda row: "INVOICE DATE" in cell_value(row, "A").upper()
        and "INVOICE" in cell_value(row, "B").upper()
        and "GRADE" in cell_value(row, "C").upper(),
    )
    if header is None:
        return 0
    records: list[list[Any]] = []
    for row_number in sorted(rows):
        if row_number <= header:
            continue
        row = rows[row_number]
        invoice_no = cell_value(row, "B")
        grade = cell_value(row, "C")
        sold_qty = cell_number(row, "D")
        if not invoice_no or not grade or sold_qty is None:
            continue
        invoice_date_value = cell_value(row, "A")
        despatch_date_value = cell_value(row, "G")
        records.append([
            batch_id, estate_name, reporting_month,
            excel_serial_to_date(invoice_date_value) or invoice_date_value,
            invoice_no, grade, sold_qty, cell_number(row, "E"),
            cell_value(row, "F"),
            excel_serial_to_date(despatch_date_value) or despatch_date_value,
            file_name, sheet_name,
        ])
    if records:
        connection.executemany(
            "INSERT INTO invoice_sales VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            records,
        )
    return len(records)


def parse_break_even(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
    estate_name: str,
    reporting_month: str,
    file_name: str,
    sheets: dict[str, dict[int, dict[str, Cell]]],
) -> int:
    year, month = reporting_month_parts(reporting_month)
    target = MONTH_NAMES[month]
    selected_sheet = "Sheet2" if "Sheet2" in sheets else next(iter(sheets), None)
    if selected_sheet is None:
        return 0
    rows = sheets[selected_sheet]
    records: list[list[Any]] = []
    for row_number in sorted(rows):
        period = cell_value(rows[row_number], "B")
        normalized = period.upper().replace("TO DATE", "TODATE").strip()
        if normalized not in {target, f"{target} TODATE"}:
            continue
        row = rows[row_number]
        records.append([
            batch_id, estate_name, reporting_month, period,
            cell_number(row, "F"), cell_number(row, "G"),
            cell_number(row, "H"), cell_number(row, "I"),
            cell_number(row, "M"), cell_number(row, "N"),
            cell_number(row, "P"), cell_number(row, "T"),
            cell_number(row, "V"), cell_number(row, "W"),
            cell_number(row, "X"), cell_number(row, "Z"),
            cell_number(row, "AB"), file_name, selected_sheet,
        ])
    if records:
        connection.executemany(
            """
            INSERT INTO break_even_metrics VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            records,
        )
    return len(records)


def parse_m_format(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
    estate_name: str,
    reporting_month: str,
    file_name: str,
    sheets: dict[str, dict[int, dict[str, Cell]]],
) -> int:
    inserted = 0

    def month_year_from_headers(
        *header_values: str,
    ) -> tuple[int | None, int | None]:
        values = [clean_text(value) for value in header_values]
        metric_year: int | None = None
        metric_month: int | None = None
        for value in values:
            number = to_float(value)
            if number is not None and 1900 <= number <= 2100:
                metric_year = int(number)
                continue
            if number is not None and 30000 <= number <= 70000:
                converted = excel_serial_to_date(number)
                if converted:
                    date_value = datetime.strptime(converted, "%Y-%m-%d")
                    metric_year = date_value.year
                    metric_month = date_value.month
                    continue
            normalized = re.sub(r"[^A-Z]", "", value.upper())
            if normalized in MONTH_ALIASES:
                metric_month = MONTH_ALIASES[normalized]
            elif normalized[:3] in MONTH_ALIASES:
                metric_month = MONTH_ALIASES[normalized[:3]]
        return metric_year, metric_month

    for sheet_name, rows in sheets.items():
        candidate_header = None
        for row_number in sorted(rows):
            if row_number > 12:
                break
            values = " ".join(
                cell_value(rows[row_number], col)
                for col in ("A", "B", "C")
            )
            if "F/ NO" in values.upper() or "F/NO" in values.upper():
                candidate_header = row_number
                break
        if candidate_header is None:
            continue

        division = None
        for row_number in range(1, candidate_header + 1):
            for column in ("A", "B", "C", "D"):
                division = division or normalize_division(
                    cell_value(rows.get(row_number, {}), column)
                )
        if not division:
            division = normalize_division(file_name)

        metric_row = None
        for row_number in range(candidate_header, candidate_header + 6):
            row = rows.get(row_number, {})
            if sum(
                1
                for cell in row.values()
                if clean_text(cell.value).upper() == "YPH"
            ) >= 2:
                metric_row = row_number
                break
        if metric_row is None:
            continue

        yph_columns: list[tuple[str, int, int, str | None]] = []
        metric_cells = rows.get(metric_row, {})
        for column, cell in metric_cells.items():
            if clean_text(cell.value).upper() != "YPH":
                continue
            col_number = col_to_num(column)
            previous_col = num_to_col(max(1, col_number - 1))
            next_col = num_to_col(col_number + 1)
            metric_year, metric_month = month_year_from_headers(
                cell_value(rows.get(metric_row - 1, {}), column),
                cell_value(rows.get(metric_row - 1, {}), previous_col),
                cell_value(rows.get(metric_row - 1, {}), next_col),
                cell_value(rows.get(metric_row - 2, {}), column),
                cell_value(rows.get(metric_row - 2, {}), previous_col),
                cell_value(rows.get(metric_row - 2, {}), next_col),
            )
            if not metric_month or not metric_year:
                continue

            previous_label = cell_value(metric_cells, previous_col).upper()
            next_label = cell_value(metric_cells, next_col).upper()
            nitrogen_col = None
            if previous_label in {"N", "NIT", "NITROGEN"}:
                nitrogen_col = previous_col
            elif next_label in {"N", "NIT", "NITROGEN"}:
                nitrogen_col = next_col
            yph_columns.append(
                (column, metric_year, metric_month, nitrogen_col)
            )

        if not yph_columns:
            continue

        field_col = "A"
        header_a = cell_value(rows.get(candidate_header, {}), "A").upper()
        header_b = cell_value(rows.get(candidate_header, {}), "B").upper()
        if "F/" not in header_a and "F/" in header_b:
            field_col = "B"
        extent_col = "B" if field_col == "A" else "A"

        for row_number in sorted(rows):
            if row_number <= metric_row:
                continue
            row = rows[row_number]
            field_no = clean_label(cell_value(row, field_col))
            if not field_no or field_no.upper() == "TOTAL":
                continue
            if field_no.upper().startswith(("ESTATE", "DIVISION", "MONTH")):
                continue
            extent = cell_number(row, extent_col)
            for column, metric_year, metric_month, nitrogen_col in yph_columns:
                yph = cell_number(row, column)
                nitrogen = (
                    cell_number(row, nitrogen_col)
                    if nitrogen_col else None
                )
                if yph is None and nitrogen is None:
                    continue
                connection.execute(
                    "INSERT INTO m_format_monthly_yph VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        batch_id, estate_name, reporting_month, division,
                        field_no, extent, metric_year, metric_month,
                        yph, nitrogen, file_name, sheet_name,
                    ],
                )
                inserted += 1
    return inserted

def parse_workbook_category(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
    estate_name: str,
    reporting_month: str,
    category: str,
    file_name: str,
    path: Path,
    issues: list[dict[str, Any]],
) -> int:
    with XlsxReader(path) as book:
        sheets = capture_workbook_rows(
            connection, batch_id, category, file_name, book
        )
    inserted = 0
    if category == "PLUCKING_PLAN":
        inserted += parse_plucking_plan(
            connection, batch_id, estate_name, reporting_month, file_name, sheets
        )
    elif category == "NON_WAGE_PLUCKING":
        inserted += parse_non_wage(
            connection, batch_id, estate_name, reporting_month, file_name, sheets
        )
    elif category == "AGRICULTURAL_ACTIVITY":
        inserted += parse_agricultural_activity(
            connection, batch_id, estate_name, reporting_month, file_name, sheets
        )
    elif category == "CONTRACT_MONITORING":
        inserted += parse_contracts(
            connection, batch_id, estate_name, reporting_month, file_name, sheets
        )
    elif category == "BREAK_EVEN":
        inserted += parse_break_even(
            connection, batch_id, estate_name, reporting_month, file_name, sheets
        )
    elif category == "M_FORMAT":
        inserted += parse_m_format(
            connection, batch_id, estate_name, reporting_month, file_name, sheets
        )
    elif category == "CROP_COP_REVENUE":
        if "CROP" in sheets:
            inserted += parse_crop_sheet(
                connection, batch_id, estate_name, reporting_month,
                file_name, sheets["CROP"], "CROP",
            )
        if "PLUCKERS" in sheets:
            inserted += parse_pluckers_sheet(
                connection, batch_id, estate_name, reporting_month,
                file_name, sheets["PLUCKERS"], "PLUCKERS",
            )
        if "COP TEA" in sheets:
            inserted += parse_cop_sheet(
                connection, batch_id, estate_name, reporting_month,
                file_name, sheets["COP TEA"], "COP TEA",
            )
        if "Revenue 2" in sheets:
            inserted += parse_revenue_sheet(
                connection, batch_id, estate_name, reporting_month,
                file_name, sheets["Revenue 2"], "Revenue 2",
            )
        if "INV" in sheets:
            inserted += parse_invoice_sheet(
                connection, batch_id, estate_name, reporting_month,
                file_name, sheets["INV"], "INV",
            )
    return inserted


def fetch_one_dict(
    connection: duckdb.DuckDBPyConnection,
    sql: str,
    parameters: list[Any],
) -> dict[str, Any] | None:
    result = connection.execute(sql, parameters)
    row = result.fetchone()
    if row is None:
        return None
    return dict(zip([item[0] for item in result.description], row))


def fetch_dicts(
    connection: duckdb.DuckDBPyConnection,
    sql: str,
    parameters: list[Any],
) -> list[dict[str, Any]]:
    result = connection.execute(sql, parameters)
    columns = [item[0] for item in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def build_preview(
    connection: duckdb.DuckDBPyConnection,
    batch_id: str,
    estate_name: str,
    reporting_month: str,
    issues: list[dict[str, Any]],
) -> dict[str, Any]:
    counts = {}
    for table in [
        "raw_sheet_rows", "g_report_metrics", "plucking_plan_fields",
        "non_wage_plucking_fields", "agricultural_activity_records",
        "contract_records", "crop_field_performance", "crop_division_summary",
        "plucker_performance", "cop_line_items", "revenue_grade_sales",
        "invoice_sales", "break_even_metrics", "m_format_monthly_yph",
    ]:
        counts[table] = connection.execute(
            f"SELECT COUNT(*) FROM {table} WHERE batch_id = ?",
            [batch_id],
        ).fetchone()[0]

    g_reports = fetch_dicts(
        connection,
        """
        SELECT division_name, total_green_leaf_kg, total_checkroll_names,
               average_plucking_kg, total_hectare_plucked
        FROM g_report_metrics
        WHERE batch_id = ?
        ORDER BY division_name
        """,
        [batch_id],
    )
    plucking_plan = fetch_dicts(
        connection,
        """
        SELECT division_name,
               COUNT(*) AS field_count,
               ROUND(SUM(actual_green_leaf_kg), 2) AS actual_green_leaf_kg,
               ROUND(SUM(actual_total_pluckers), 2) AS actual_total_pluckers
        FROM plucking_plan_fields
        WHERE batch_id = ?
        GROUP BY division_name
        ORDER BY division_name
        """,
        [batch_id],
    )
    crop = fetch_dicts(
        connection,
        """
        SELECT division_name, extent_ha, made_tea_crop_kg, yield_kg_per_ha
        FROM crop_division_summary
        WHERE batch_id = ?
        ORDER BY division_name
        """,
        [batch_id],
    )
    cop_total = fetch_one_dict(
        connection,
        """
        SELECT description, rupees_per_kg, cost_per_hectare
        FROM cop_line_items
        WHERE batch_id = ? AND upper(description) LIKE '%TOTAL C.O.P%'
        ORDER BY line_number
        LIMIT 1
        """,
        [batch_id],
    )
    break_even = fetch_dicts(
        connection,
        """
        SELECT metric_period, total_fixed_cost_rs, variable_cost_rs_per_kg,
               contribution_rs_per_kg, break_even_quantity_kg,
               budget_crop_kg, budget_cop_rs_per_kg, tea_profit_rs,
               break_even_pct_of_budget
        FROM break_even_metrics
        WHERE batch_id = ?
        ORDER BY metric_period
        """,
        [batch_id],
    )
    made_tea_total = connection.execute(
        "SELECT SUM(made_tea_crop_kg) FROM crop_division_summary WHERE batch_id = ?",
        [batch_id],
    ).fetchone()[0]
    g_leaf_total = connection.execute(
        "SELECT SUM(total_green_leaf_kg) FROM g_report_metrics WHERE batch_id = ?",
        [batch_id],
    ).fetchone()[0]
    plan_leaf_total = connection.execute(
        "SELECT SUM(actual_green_leaf_kg) FROM plucking_plan_fields WHERE batch_id = ?",
        [batch_id],
    ).fetchone()[0]

    warnings = [item for item in issues if item["severity"] == "WARNING"]
    errors = [item for item in issues if item["severity"] == "ERROR"]
    return {
        "batch_id": batch_id,
        "estate_name": estate_name,
        "reporting_month": reporting_month,
        "analytics_status": "PARSED_PENDING_APPROVAL" if not errors else "PARSED_WITH_ERRORS",
        "parser_version": PARSER_VERSION,
        "record_counts": counts,
        "key_totals_pending_confirmation": {
            "g_report_green_leaf_kg": g_leaf_total,
            "plucking_plan_actual_green_leaf_kg": plan_leaf_total,
            "made_tea_crop_kg": made_tea_total,
        },
        "g_report_by_division": g_reports,
        "plucking_plan_by_division": plucking_plan,
        "made_tea_crop_by_division": crop,
        "cop_total": cop_total,
        "break_even": break_even,
        "errors": errors,
        "warnings": warnings,
        "next_step": (
            "Review these parsed figures. Do not enable chatbot reporting until metrics are separately approved."
        ),
    }


def parse_batch(db_path: Path, batch_id: str, preview_path: Path | None) -> dict[str, Any]:
    connection = duckdb.connect(str(db_path))
    ensure_tables(connection)
    batch = fetch_one_dict(
        connection,
        """
        SELECT batch_id, estate_name, reporting_month, status,
               approved_path, manifest_json
        FROM upload_batches
        WHERE batch_id = ?
        """,
        [batch_id],
    )
    if batch is None:
        raise RuntimeError(f"Upload batch not found: {batch_id}")
    if batch["status"] != "APPROVED_SOURCE":
        raise RuntimeError(
            f"Batch must be APPROVED_SOURCE before parsing. Current status: {batch['status']}"
        )
    approved_path = Path(batch["approved_path"])
    if not approved_path.exists():
        raise RuntimeError(f"Approved source folder does not exist: {approved_path}")

    clear_batch(connection, batch_id)
    issues: list[dict[str, Any]] = []
    parsed_record_count = 0
    files = fetch_dicts(
        connection,
        """
        SELECT file_name, relative_path, category, division_name, extension
        FROM uploaded_files
        WHERE batch_id = ?
        ORDER BY category, file_name
        """,
        [batch_id],
    )
    for item in files:
        path = approved_path / item["relative_path"]
        file_name = item["file_name"]
        category = item["category"]
        try:
            if item["extension"].lower() == ".pdf":
                parsed_record_count += parse_g_report(
                    connection, batch_id, batch["estate_name"],
                    batch["reporting_month"], file_name,
                    item["division_name"], path, issues,
                )
            elif item["extension"].lower() in {".xlsx", ".xlsm"}:
                parsed_record_count += parse_workbook_category(
                    connection, batch_id, batch["estate_name"],
                    batch["reporting_month"], category, file_name,
                    path, issues,
                )
            else:
                add_issue(
                    issues, "WARNING", category, file_name,
                    f"Parser does not yet process extension {item['extension']}.",
                )
        except Exception as exc:
            add_issue(
                issues, "ERROR", category, file_name,
                f"Parser error: {type(exc).__name__}: {exc}",
            )

    for issue in issues:
        connection.execute(
            "INSERT INTO parse_issues VALUES (?, ?, ?, ?, ?, ?)",
            [
                batch_id, issue["severity"], issue["category"],
                issue["file_name"], issue.get("sheet_name"), issue["message"],
            ],
        )

    preview = build_preview(
        connection, batch_id, batch["estate_name"], batch["reporting_month"], issues
    )
    connection.execute(
        """
        INSERT INTO batch_analytics VALUES (
            ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?
        )
        """,
        [
            batch_id, batch["estate_name"], batch["reporting_month"],
            preview["analytics_status"], PARSER_VERSION, utc_now(),
            len(files), parsed_record_count,
            len(preview["errors"]), len(preview["warnings"]),
            json.dumps(preview, default=json_default, ensure_ascii=False),
        ],
    )
    connection.execute(
        """
        INSERT OR REPLACE INTO production_metadata VALUES
        ('analytics_state', 'PARSED_PENDING_APPROVAL'),
        ('last_parsed_batch_id', ?),
        ('updated_at_utc', ?)
        """,
        [batch_id, utc_now().isoformat()],
    )
    connection.close()

    if preview_path:
        preview_path.parent.mkdir(parents=True, exist_ok=True)
        preview_path.write_text(
            json.dumps(preview, default=json_default, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return preview


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--preview")
    args = parser.parse_args()
    preview = parse_batch(
        Path(args.db),
        args.batch_id,
        Path(args.preview) if args.preview else None,
    )
    print(json.dumps(preview, default=json_default, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

