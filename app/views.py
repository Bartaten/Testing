from __future__ import annotations

import io
import os
import uuid
from typing import Dict, List, Optional

from flask import (
    Blueprint,
    Response,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)

from .utils.merge import read_table, normalize_records, infer_key_columns, combine_records

main_bp = Blueprint("main", __name__)

# Simple in-memory store keyed by session id
IN_MEMORY_STORE: Dict[str, dict] = {}


def _get_session_id() -> str:
    sid = session.get("sid")
    if not sid:
        sid = uuid.uuid4().hex
        session["sid"] = sid
    return sid


def _get_store() -> Dict[str, dict]:
    sid = _get_session_id()
    store = IN_MEMORY_STORE.setdefault(sid, {"frames": [], "combined": None, "meta": []})
    return store


@main_bp.route("/")
def index():
    store = _get_store()
    frames_meta = store.get("meta", [])
    has_combined = store.get("combined") is not None
    return render_template("index.html", frames_meta=frames_meta, has_combined=has_combined)


@main_bp.route("/upload", methods=["POST"])
def upload():
    files = request.files.getlist("files")
    if not files:
        flash("Please select at least one Excel file.", "warning")
        return redirect(url_for("main.index"))

    store = _get_store()

    for f in files:
        if not f.filename:
            continue
        original_name = f.filename
        filename = uuid.uuid4().hex + os.path.splitext(original_name)[1]
        path = os.path.join(os.environ.get("UPLOAD_FOLDER", "/workspace/app/uploads"), filename)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        f.save(path)

        # Read and normalize
        try:
            records = read_table(path)
        except Exception as e:
            flash(f"Failed to read {original_name}: {e}", "danger")
            continue

        normalized, col_map = normalize_records(records)
        store["frames"].append(normalized)
        all_cols = sorted({c for r in normalized for c in r.keys()})
        store["meta"].append({
            "original_name": original_name,
            "stored_as": filename,
            "columns": all_cols,
            "col_map": col_map,
        })

    session.modified = True
    flash("Files uploaded successfully.", "success")
    return redirect(url_for("main.index"))


@main_bp.route("/combine", methods=["POST"])
def combine():
    store = _get_store()
    frames: List[list[dict]] = store.get("frames", [])
    if not frames:
        flash("Upload files before combining.", "warning")
        return redirect(url_for("main.index"))

    # Determine keys
    # Try organisation_id first, then customer + name
    key_options = [
        ["organisation_id"],
        ["customer", "name"],
    ]

    keys = infer_key_columns(frames, key_options)
    if not keys:
        flash("Could not infer matching keys from uploaded files.", "danger")
        return redirect(url_for("main.index"))

    combined_records = combine_records(frames, keys)

    # Select/ensure display fields
    # Ensure display fields exist
    display_fields = [
        "organisation_id",
        "customer",
        "name",
        "product_status",
        "engagement_status",
        "carbon_factor",
        "next_activity_due_date",
    ]
    for rec in combined_records:
        for field in display_fields:
            rec.setdefault(field, None)

    # Compute derived fields for dashboard
    for rec in combined_records:
        nad = rec.get("next_activity_due_date")
        rec["has_next_activity_due"] = bool(nad not in (None, ""))

    store["combined"] = combined_records
    session.modified = True
    flash(f"Combined {len(frames)} files on keys: {', '.join(keys)}.", "success")
    return redirect(url_for("main.dashboard"))


@main_bp.route("/dashboard")
def dashboard():
    store = _get_store()
    combined: Optional[list[dict]] = store.get("combined")
    if not combined:
        flash("No combined data available. Upload and combine files.", "info")
        return redirect(url_for("main.index"))

    # KPIs
    total_contractors = len(combined)
    def _vc(records: list[dict], field: str) -> dict:
        counts: Dict[str, int] = {}
        for r in records:
            key = str(r.get(field) or "Unknown")
            counts[key] = counts.get(key, 0) + 1
        return counts

    engagement_breakdown = _vc(combined, "engagement_status")
    product_breakdown = _vc(combined, "product_status")

    def _to_float(v) -> Optional[float]:
        try:
            if v is None or str(v).strip() == "":
                return None
            return float(str(v).replace(",", ""))
        except Exception:
            return None

    cf_values = [x for x in (_to_float(r.get("carbon_factor")) for r in combined) if x is not None]
    carbon_factor_stats = {
        "avg": float(sum(cf_values) / len(cf_values)) if cf_values else 0.0,
        "min": float(min(cf_values)) if cf_values else 0.0,
        "max": float(max(cf_values)) if cf_values else 0.0,
    }

    due_soon_count = sum(1 for r in combined if r.get("has_next_activity_due"))

    # Sample table rows
    sample_rows = [
        {
            "organisation_id": r.get("organisation_id"),
            "customer": r.get("customer"),
            "name": r.get("name"),
            "product_status": r.get("product_status"),
            "engagement_status": r.get("engagement_status"),
            "carbon_factor": r.get("carbon_factor"),
            "next_activity_due_date": r.get("next_activity_due_date"),
        }
        for r in combined[:200]
    ]

    return render_template(
        "dashboard.html",
        total_contractors=total_contractors,
        engagement_breakdown=engagement_breakdown,
        product_breakdown=product_breakdown,
        carbon_factor_stats=carbon_factor_stats,
        due_soon_count=due_soon_count,
        table=sample_rows,
    )


@main_bp.route("/export/csv")
def export_csv():
    store = _get_store()
    combined: Optional[list[dict]] = store.get("combined")
    if not combined:
        flash("No combined data to export.", "warning")
        return redirect(url_for("main.index"))

    output = io.StringIO()
    # Build CSV headers from union of keys
    headers = sorted({k for r in combined for k in r.keys()})
    import csv as _csv
    writer = _csv.DictWriter(output, fieldnames=headers)
    writer.writeheader()
    for row in combined:
        writer.writerow({k: row.get(k) for k in headers})
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=combined.csv"},
    )


@main_bp.route("/export/xlsx")
def export_xlsx():
    store = _get_store()
    combined: Optional[list[dict]] = store.get("combined")
    if not combined:
        flash("No combined data to export.", "warning")
        return redirect(url_for("main.index"))

    output = io.BytesIO()
    try:
        from openpyxl import Workbook
        wb = Workbook()
        ws = wb.active
        headers = sorted({k for r in combined for k in r.keys()})
        ws.append(headers)
        for row in combined:
            ws.append([row.get(h) for h in headers])
        wb.save(output)
    except Exception as e:
        return Response(f"Failed to export XLSX: {e}", status=500)
    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name="combined.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
