from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from urllib import error as urlerror
from urllib import request as urlrequest

import duckdb
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel, Field


router = APIRouter()
ROOT = Path(r"G:\TTE_Local_Intelligence")
DB = ROOT / "production_data" / "tte_production.duckdb"
REPORT_DIR = ROOT / "production_data" / "generated_reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)
STATIC = Path(__file__).resolve().parent / "static"
OLLAMA_URL = os.environ.get("OLLAMA_BASE_URL", "http://127.0.0.1:11434").rstrip("/")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen3:4b")
COOKIE = "tte_pro_session"

ROLES = {
    "system_admin": {"*"},
    "management": {"all"},
    "regional": {"field","crop","agriculture","contracts","plucker","cop","break_even","issues","evidence"},
    "estate": {"field","crop","agriculture","contracts","plucker","issues","evidence"},
    "agriculture": {"field","crop","agriculture","contracts","issues","evidence"},
    "finance": {"crop","cop","break_even","revenue","issues","evidence"},
    "sustainability": {"field","crop","agriculture","issues","evidence"},
    "viewer": {"crop","issues","evidence"},
}
SENSITIVE = {"employee_name","employee_code","contractor_name","field_officer","assistant_manager","manager","buyer"}
FINANCIAL = {"rupees_per_kg","cost_per_hectare","tea_profit_rs","total_fixed_cost_rs","total_proceeds_rs","net_proceeds_rs","contract_value","labour_total_cost_rs"}

OPERATIONS = {
    "summary","compare_divisions","compare_months","compare_estates",
    "rank_fields","low_yield_high_fertilizer","actual_below_plan",
    "cop_drivers","contractor_cost_per_hectare","crop_trend",
    "estate_region_comparison","data_quality","source_evidence","catalog",
}
OP_TABLES = {
    "summary":["g_report_metrics","plucking_plan_fields","crop_division_summary","cop_line_items"],
    "compare_divisions":["g_report_metrics","plucking_plan_fields","crop_division_summary"],
    "compare_months":["metric_facts"],
    "compare_estates":["metric_facts"],
    "rank_fields":["crop_field_performance"],
    "low_yield_high_fertilizer":["crop_field_performance","agricultural_activity_records"],
    "actual_below_plan":["plucking_plan_fields"],
    "cop_drivers":["cop_line_items"],
    "contractor_cost_per_hectare":["contract_records"],
    "crop_trend":["metric_facts"],
    "estate_region_comparison":["metric_facts","estate_master"],
    "data_quality":["data_quality_issues"],
    "source_evidence":["metric_evidence"],
    "catalog":["metric_facts"],
}


class LoginIn(BaseModel):
    username: str
    password: str

class UserIn(BaseModel):
    username: str = Field(min_length=3)
    display_name: str = Field(min_length=2)
    password: str = Field(min_length=8)
    role: str
    estate_scope: list[str] = []

class ChatIn(BaseModel):
    question: str = Field(min_length=2, max_length=1500)
    estate_name: str | None = None
    reporting_month: str | None = None
    division_name: str | None = None
    previous_plan: dict[str, Any] | None = None
    history: list[dict[str, str]] = []

class IssueActionIn(BaseModel):
    action: Literal["APPROVE_OFFICIAL_METRIC","MARK_REVIEWED","DISMISS_WITH_REASON","RETURN_FOR_CORRECTION"]
    note: str = Field(min_length=3)
    confirm_text: str
    official_source: str | None = None
    manual_value: float | None = None

class MonthActionIn(BaseModel):
    action: Literal["APPROVE_MONTH","REJECT_MONTH","RETURN_FOR_CORRECTION","MARK_REPLACEMENT_REQUIRED","LOCK_MONTH","UNLOCK_MONTH","ADD_NOTE"]
    note: str = Field(min_length=3)
    confirm_text: str

class ReportIn(BaseModel):
    estate_name: str
    reporting_month: str
    division_name: str | None = None
    kind: Literal["management_summary","division_comparison","data_quality","cop_analysis"] = "management_summary"
    format: Literal["xlsx","docx","pdf","pptx"]


def con(read_only: bool = True):
    if not DB.exists():
        raise HTTPException(503, f"Production database not found: {DB}")
    return duckdb.connect(str(DB), read_only=read_only)

def table_exists(c, name: str) -> bool:
    return bool(c.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='main' AND table_type='BASE TABLE' AND table_name=?",
        [name],
    ).fetchone()[0])

def cols(c, name: str) -> set[str]:
    return {r[0] for r in c.execute(f'DESCRIBE "{name}"').fetchall()} if table_exists(c,name) else set()

def rows(c, sql: str, params=None):
    cur = c.execute(sql, params or [])
    names = [d[0] for d in cur.description]
    out=[]
    for row in cur.fetchall():
        item={}
        for k,v in zip(names,row):
            if hasattr(v,"isoformat"):
                try:v=v.isoformat()
                except Exception:pass
            item[k]=v
        out.append(item)
    return out

def fmt(v, d=2):
    try:return f"{float(v):,.{d}f}"
    except (TypeError,ValueError):return "not available"

def ensure_tables(c):
    c.execute("""CREATE TABLE IF NOT EXISTS app_users(
        user_id VARCHAR PRIMARY KEY,username VARCHAR UNIQUE,display_name VARCHAR,
        salt VARCHAR,password_hash VARCHAR,role VARCHAR,estate_scope_json VARCHAR,
        is_active BOOLEAN,created_at TIMESTAMP,updated_at TIMESTAMP)""")
    c.execute("""CREATE TABLE IF NOT EXISTS app_sessions(
        session_id VARCHAR PRIMARY KEY,token_hash VARCHAR UNIQUE,user_id VARCHAR,
        expires_at TIMESTAMP,revoked_at TIMESTAMP)""")
    c.execute("""CREATE TABLE IF NOT EXISTS data_quality_issues(
        issue_id VARCHAR PRIMARY KEY,batch_id VARCHAR,estate_name VARCHAR,reporting_month VARCHAR,
        issue_type VARCHAR,severity VARCHAR,metric_name VARCHAR,source_a_name VARCHAR,
        source_a_value DOUBLE,source_b_name VARCHAR,source_b_value DOUBLE,difference_value DOUBLE,
        difference_pct DOUBLE,status VARCHAR,manager_message VARCHAR,detected_at TIMESTAMP,
        resolved_at TIMESTAMP,resolved_by VARCHAR,resolution_note VARCHAR,
        official_source VARCHAR,official_value DOUBLE)""")
    c.execute("""CREATE TABLE IF NOT EXISTS metric_approvals(
        approval_id VARCHAR PRIMARY KEY,issue_id VARCHAR,batch_id VARCHAR,estate_name VARCHAR,
        reporting_month VARCHAR,metric_name VARCHAR,official_source VARCHAR,official_value DOUBLE,
        approved_by VARCHAR,approval_note VARCHAR,approved_at TIMESTAMP)""")
    c.execute("""CREATE TABLE IF NOT EXISTS governance_audit_log(
        audit_id VARCHAR PRIMARY KEY,action_type VARCHAR,issue_id VARCHAR,batch_id VARCHAR,
        performed_by VARCHAR,action_note VARCHAR,action_at TIMESTAMP)""")
    c.execute("""CREATE TABLE IF NOT EXISTS month_controls(
        batch_id VARCHAR PRIMARY KEY,estate_name VARCHAR,reporting_month VARCHAR,
        workflow_status VARCHAR,is_locked BOOLEAN,control_note VARCHAR,
        controlled_by VARCHAR,controlled_at TIMESTAMP)""")
    c.execute("""CREATE TABLE IF NOT EXISTS estate_master(
        estate_name VARCHAR PRIMARY KEY,elevation_region VARCHAR,regional_scope VARCHAR,
        data_owner VARCHAR,is_active BOOLEAN,updated_at TIMESTAMP)""")
    c.execute("""CREATE TABLE IF NOT EXISTS metric_facts(
        fact_id VARCHAR PRIMARY KEY,batch_id VARCHAR,estate_name VARCHAR,division_name VARCHAR,
        reporting_month VARCHAR,field_no VARCHAR,metric_name VARCHAR,actual_value DOUBLE,
        budget_value DOUBLE,unit VARCHAR,source_table VARCHAR,source_file VARCHAR,
        source_sheet VARCHAR,formula_used VARCHAR,data_owner VARCHAR,parsed_at TIMESTAMP,
        approval_status VARCHAR,official_metric_source VARCHAR,approved_value DOUBLE,
        approved_by VARCHAR,approved_date TIMESTAMP,reconciliation_status VARCHAR)""")
    c.execute("""CREATE TABLE IF NOT EXISTS metric_evidence(
        evidence_id VARCHAR PRIMARY KEY,batch_id VARCHAR,estate_name VARCHAR,reporting_month VARCHAR,
        division_name VARCHAR,field_no VARCHAR,metric_name VARCHAR,source_table VARCHAR,
        source_workbook VARCHAR,source_sheet VARCHAR,source_line VARCHAR,formula_used VARCHAR,
        data_owner VARCHAR,parsed_at TIMESTAMP,approval_status VARCHAR,evidence_note VARCHAR)""")

def phash(password: str, salt=None):
    raw = bytes.fromhex(salt) if salt else secrets.token_bytes(16)
    dig = hashlib.pbkdf2_hmac("sha256",password.encode(),raw,200000)
    return raw.hex(),dig.hex()

def add_user(c, username, display, password, role, scope):
    if role not in ROLES: raise HTTPException(400,"Invalid role")
    salt,dig=phash(password); now=datetime.now()
    c.execute("""INSERT INTO app_users VALUES(?,?,?,?,?,?,?,?,?,?)""",[
        str(uuid.uuid4()),username.lower().strip(),display.strip(),salt,dig,role,
        json.dumps(scope),True,now,now])
    return {"username":username.lower().strip(),"display_name":display,"role":role,"estate_scope":scope}

def current_user(request: Request):
    token=request.cookies.get(COOKIE)
    if not token: raise HTTPException(401,"Login required")
    h=hashlib.sha256(token.encode()).hexdigest()
    c=con(False)
    try:
        ensure_tables(c)
        r=rows(c,"""SELECT u.user_id,u.username,u.display_name,u.role,u.estate_scope_json
          FROM app_sessions s JOIN app_users u ON u.user_id=s.user_id
          WHERE s.token_hash=? AND s.revoked_at IS NULL AND s.expires_at>? AND u.is_active=TRUE LIMIT 1""",[h,datetime.now()])
        if not r: raise HTTPException(401,"Session expired")
        u=r[0];u["estate_scope"]=json.loads(u.pop("estate_scope_json") or "[]");return u
    finally:c.close()

def estate_access(user, estate):
    return user["role"] in {"system_admin","management","finance","agriculture","sustainability"} or estate.lower() in {x.lower() for x in user["estate_scope"]}

def role_access(user, domain):
    allowed=ROLES.get(user["role"],set())
    if "*" in allowed or "all" in allowed:return
    if domain not in allowed:raise HTTPException(403,f"Role cannot access {domain}")

def mask(data,user):
    out=[]
    for r in data:
        z={}
        for k,v in r.items():
            if k in SENSITIVE and user["role"] not in {"system_admin","management","estate"}:z[k]="[RESTRICTED]"
            elif k in FINANCIAL and user["role"] not in {"system_admin","management","finance"}:z[k]="[RESTRICTED]"
            else:z[k]=v
        out.append(z)
    return out


def batches(c,user=None):
    data=rows(c,"""SELECT batch_id,estate_name,reporting_month,analytics_status,
        parser_version,parsed_at,metrics_approved_at FROM batch_analytics
        ORDER BY estate_name,reporting_month DESC""") if table_exists(c,"batch_analytics") else []
    return [r for r in data if not user or estate_access(user,r["estate_name"])]

def get_batch(c,user,estate,month):
    data=batches(c,user)
    if estate:data=[r for r in data if r["estate_name"].lower()==estate.lower()]
    if month:data=[r for r in data if r["reporting_month"]==month]
    if not data:raise HTTPException(404,"No accessible estate-month batch")
    return data[0]

def divisions(c,bid):
    out=set()
    for t in ("g_report_metrics","plucking_plan_fields","crop_division_summary","crop_field_performance","agricultural_activity_records","contract_records"):
        if table_exists(c,t) and "division_name" in cols(c,t):
            out|={str(x[0]) for x in c.execute(f"SELECT DISTINCT division_name FROM {t} WHERE batch_id=? AND division_name IS NOT NULL",[bid]).fetchall()}
    return sorted(out)

def infer_division(q,selected,estate,available):
    if selected:return next((d for d in available if d.lower()==selected.lower()),None)
    norm=re.sub(r"[^a-z0-9]+"," ",q.lower())
    et=re.sub(r"[^a-z0-9]+"," ",estate.lower()).strip()
    for d in available:
        t=re.sub(r"[^a-z0-9]+"," ",d.lower()).strip()
        if t==et:
            if re.search(rf"\b{re.escape(t)}\s+division\b|\bdivision\s+{re.escape(t)}\b",norm):return d
        elif re.search(rf"\b{re.escape(t)}\b",norm):return d
    return None

def source_for(c,bid,metric):
    hint={
      "green_leaf_kg":("G_REPORT_PDF","G Report"),
      "actual_green_leaf_kg":("PLUCKING_PLAN","Plucking Plan"),
      "made_tea_crop_kg":("CROP_COP_REVENUE","CROP"),
      "yield_kg_per_ha":("CROP_COP_REVENUE","CROP"),
      "cop_rs_per_kg":("CROP_COP_REVENUE","COP TEA"),
      "break_even_quantity_kg":("BREAK_EVEN","Break Even"),
    }.get(metric,(None,None))
    if table_exists(c,"source_sheet_inventory"):
        cs=cols(c,"source_sheet_inventory")
        fc=next((x for x in ("file_name","source_file","workbook_name") if x in cs),None)
        sc=next((x for x in ("sheet_name","worksheet_name") if x in cs),None)
        if fc:
            cond=["batch_id=?"];p=[bid]
            if "category" in cs and hint[0]:cond.append("UPPER(category)=UPPER(?)");p.append(hint[0])
            if sc and hint[1]:cond.append(f"UPPER({sc}) LIKE UPPER(?)");p.append(f"%{hint[1]}%")
            r=c.execute(f"SELECT {fc}{','+sc if sc else ''} FROM source_sheet_inventory WHERE {' AND '.join(cond)} LIMIT 1",p).fetchone()
            if r:return str(r[0]),str(r[1]) if len(r)>1 else hint[1]
    return None,hint[1]

def refresh_facts():
    c=con(False)
    try:
        ensure_tables(c);now=datetime.now()
        c.execute("""INSERT OR REPLACE INTO estate_master VALUES
          ('Bearwell','HG',NULL,'Estate Management',TRUE,?)""",[now])
        c.execute("DELETE FROM metric_facts");c.execute("DELETE FROM metric_evidence")
        inserted=0
        for b in batches(c):
            owner=(c.execute("SELECT data_owner FROM estate_master WHERE LOWER(estate_name)=LOWER(?)",[b["estate_name"]]).fetchone() or ["Estate Management"])[0]
            facts=[]
            if table_exists(c,"g_report_metrics"):
                for r in rows(c,"SELECT division_name,SUM(total_green_leaf_kg) v FROM g_report_metrics WHERE batch_id=? GROUP BY division_name",[b["batch_id"]]):
                    facts.append((r["division_name"],None,"green_leaf_kg",r["v"],None,"kg","g_report_metrics","SUM(total_green_leaf_kg)"))
            if table_exists(c,"plucking_plan_fields"):
                for r in rows(c,"SELECT division_name,CAST(field_no AS VARCHAR) field_no,actual_green_leaf_kg a,anticipated_green_leaf_kg p FROM plucking_plan_fields WHERE batch_id=?",[b["batch_id"]]):
                    facts.append((r["division_name"],r["field_no"],"actual_green_leaf_kg",r["a"],r["p"],"kg","plucking_plan_fields","actual; budget=anticipated"))
            if table_exists(c,"crop_division_summary"):
                for r in rows(c,"SELECT division_name,made_tea_crop_kg,yield_kg_per_ha FROM crop_division_summary WHERE batch_id=?",[b["batch_id"]]):
                    facts.extend([(r["division_name"],None,"made_tea_crop_kg",r["made_tea_crop_kg"],None,"kg","crop_division_summary","made_tea_crop_kg"),
                                  (r["division_name"],None,"yield_kg_per_ha",r["yield_kg_per_ha"],None,"kg/ha","crop_division_summary","made_tea_crop_kg / extent_ha")])
            if table_exists(c,"crop_field_performance"):
                for r in rows(c,"SELECT division_name,CAST(field_no AS VARCHAR) field_no,made_tea_crop_kg,yield_kg_per_ha FROM crop_field_performance WHERE batch_id=?",[b["batch_id"]]):
                    facts.extend([(r["division_name"],r["field_no"],"made_tea_crop_kg",r["made_tea_crop_kg"],None,"kg","crop_field_performance","made_tea_crop_kg"),
                                  (r["division_name"],r["field_no"],"yield_kg_per_ha",r["yield_kg_per_ha"],None,"kg/ha","crop_field_performance","made_tea_crop_kg / extent_ha")])
            if table_exists(c,"cop_line_items"):
                x=c.execute("SELECT rupees_per_kg FROM cop_line_items WHERE batch_id=? AND UPPER(description) LIKE '%TOTAL%C.O.P%' ORDER BY line_number DESC LIMIT 1",[b["batch_id"]]).fetchone()
                if x:facts.append((None,None,"cop_rs_per_kg",x[0],None,"Rs/kg","cop_line_items","TOTAL C.O.P"))
            if table_exists(c,"break_even_metrics"):
                for r in rows(c,"SELECT metric_period,break_even_quantity_kg,budget_crop_kg,tea_profit_rs FROM break_even_metrics WHERE batch_id=?",[b["batch_id"]]):
                    facts.extend([(r["metric_period"],None,"break_even_quantity_kg",r["break_even_quantity_kg"],r["budget_crop_kg"],"kg","break_even_metrics","fixed cost / contribution"),
                                  (r["metric_period"],None,"tea_profit_rs",r["tea_profit_rs"],None,"Rs","break_even_metrics","source tea profit")])
            for div,field,metric,actual,budget,unit,table,formula in facts:
                f,s=source_for(c,b["batch_id"],metric)
                ap=rows(c,"SELECT * FROM metric_approvals WHERE batch_id=? AND metric_name=? ORDER BY approved_at DESC LIMIT 1",[b["batch_id"],metric]) if table_exists(c,"metric_approvals") else []
                ap=ap[0] if ap else {}
                status=b["analytics_status"]
                c.execute("""INSERT INTO metric_facts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",[
                    str(uuid.uuid4()),b["batch_id"],b["estate_name"],div,b["reporting_month"],field,metric,actual,budget,unit,table,f,s,formula,owner,b.get("parsed_at"),status,
                    ap.get("official_source"),ap.get("official_value"),ap.get("approved_by"),ap.get("approved_at"),
                    "RESOLVED" if ap else ("PENDING" if metric in {"green_leaf_kg","actual_green_leaf_kg"} else "NOT_REQUIRED")])
                c.execute("""INSERT INTO metric_evidence VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",[
                    str(uuid.uuid4()),b["batch_id"],b["estate_name"],b["reporting_month"],div,field,metric,table,f,s,None,formula,owner,b.get("parsed_at"),status,
                    "Evidence linked from parsed table and source inventory."])
                inserted+=1
        return {"status":"completed","metric_facts_inserted":inserted}
    finally:c.close()

def issue_exists(c,bid,itype,metric):
    return bool(c.execute("SELECT COUNT(*) FROM data_quality_issues WHERE batch_id=? AND issue_type=? AND metric_name=?",[bid,itype,metric]).fetchone()[0])

def new_issue(c,b,itype,severity,metric,message,a_name=None,a=None,b_name=None,bv=None,diff=None,pct=None):
    if issue_exists(c,b["batch_id"],itype,metric):return 0
    c.execute("""INSERT INTO data_quality_issues VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,'OPEN',?,?,NULL,NULL,NULL,NULL,NULL)""",[
      str(uuid.uuid4()),b["batch_id"],b["estate_name"],b["reporting_month"],itype,severity,metric,a_name,a,b_name,bv,diff,pct,message,datetime.now()])
    return 1

def scan_all():
    c=con(False)
    try:
        ensure_tables(c);created=0
        for b in batches(c):
            bid=b["batch_id"];estate=b["estate_name"];month=b["reporting_month"]
            if table_exists(c,"g_report_metrics") and table_exists(c,"plucking_plan_fields"):
                g=c.execute("SELECT SUM(total_green_leaf_kg) FROM g_report_metrics WHERE batch_id=?",[bid]).fetchone()[0]
                p=c.execute("SELECT SUM(actual_green_leaf_kg) FROM plucking_plan_fields WHERE batch_id=?",[bid]).fetchone()[0]
                if g is not None and p is not None and abs(float(p)-float(g))>.01:
                    d=float(p)-float(g);pct=d/float(g)*100 if g else None
                    created+=new_issue(c,b,"SOURCE_RECONCILIATION","HIGH","green_leaf_kg",
                      f"{estate} {month}: G Report {float(g):,.0f} kg vs Plucking Plan {float(p):,.0f} kg; difference {d:,.0f} kg"+(f" ({pct:,.2f}%)." if pct is not None else ".")+" Management must approve the official source.",
                      "G_REPORT",float(g),"PLUCKING_PLAN",float(p),d,pct)
            if table_exists(c,"agricultural_activity_records"):
                x=c.execute("SELECT COUNT(*),MAX(budget_yph) FROM agricultural_activity_records WHERE batch_id=? AND budget_yph>10000",[bid]).fetchone()
                if x and x[0]:created+=new_issue(c,b,"POTENTIAL_OUTLIER","MEDIUM","budget_yph_above_10000",f"{estate} {month}: {x[0]} rows have budget YPH above 10,000; maximum {float(x[1]):,.0f}.","AGRICULTURAL_ACTIVITY",float(x[1]),None,None,float(x[0]),None)
                x=c.execute("SELECT COUNT(*),MAX(activity_value_number) FROM agricultural_activity_records WHERE batch_id=? AND UPPER(activity_type) LIKE '%FERTIL%' AND activity_value_number>100",[bid]).fetchone()
                if x and x[0]:created+=new_issue(c,b,"UNUSUAL_VALUE","MEDIUM","fertilizer_above_100",f"{estate} {month}: {x[0]} fertilizer rows exceed 100 units; maximum {float(x[1]):,.2f}. Confirm unit/rate.","AGRICULTURAL_ACTIVITY",float(x[1]),None,None,float(x[0]),None)
            if table_exists(c,"crop_field_performance"):
                x=rows(c,"""SELECT division_name,field_no,COUNT(DISTINCT extent_ha) n FROM crop_field_performance
                  WHERE batch_id=? AND field_no IS NOT NULL GROUP BY division_name,field_no HAVING COUNT(DISTINCT extent_ha)>1""",[bid])
                if x:created+=new_issue(c,b,"DUPLICATE_CONFLICT","MEDIUM","duplicate_field_extent",f"{estate} {month}: {len(x)} field number(s) have different extents.","CROP_FIELD_PERFORMANCE",float(len(x)),None,None,float(len(x)),None)
            if table_exists(c,"cop_line_items"):
                x=c.execute("SELECT COUNT(*),MIN(rupees_per_kg) FROM cop_line_items WHERE batch_id=? AND rupees_per_kg<0",[bid]).fetchone()
                if x and x[0]:created+=new_issue(c,b,"NEGATIVE_VALUE","LOW","negative_cop_lines",f"{estate} {month}: {x[0]} negative COP line item(s); minimum Rs. {float(x[1]):,.2f}/kg.","COP_LINE_ITEMS",float(x[1]),None,None,float(x[0]),None)
            if table_exists(c,"revenue_grade_sales"):
                x=c.execute("SELECT COUNT(*) FROM revenue_grade_sales WHERE batch_id=? AND (buyer IS NULL OR TRIM(buyer)='')",[bid]).fetchone()[0]
                if x:created+=new_issue(c,b,"MISSING_INFORMATION","LOW","missing_buyer",f"{estate} {month}: {x} revenue row(s) have no buyer.","REVENUE_GRADE_SALES",float(x),None,None,float(x),None)
            if table_exists(c,"g_report_metrics"):
                gd={str(r[0]).upper() for r in c.execute("SELECT DISTINCT division_name FROM g_report_metrics WHERE batch_id=?",[bid]).fetchall() if r[0]}
                for t in ("plucking_plan_fields","crop_division_summary"):
                    if table_exists(c,t):
                        od={str(r[0]).upper() for r in c.execute(f"SELECT DISTINCT division_name FROM {t} WHERE batch_id=?",[bid]).fetchall() if r[0]}
                        miss=sorted(gd-od)
                        if miss:created+=new_issue(c,b,"MISSING_DIVISION_DATA","HIGH",f"missing_{t}",f"{estate} {month}: {t} is missing G Report division(s): {', '.join(miss)}.","G_REPORT",float(len(gd)),t.upper(),float(len(od)),float(len(miss)),None)
        return {"status":"completed","issues_created":created}
    finally:c.close()


def fallback_plan(q,prev=None):
    lower=q.lower()
    rules=[
      (r"low yield.*high fert|high fert.*low yield","low_yield_high_fertilizer"),
      (r"below plan|below anticipated","actual_below_plan"),
      (r"contractor.*cost per hectare|highest contractor","contractor_cost_per_hectare"),
      (r"why.*cop|reason.*cop|high cop|cop driver","cop_drivers"),
      (r"compare.*month|june.*may|may.*june|july.*june","compare_months"),
      (r"last six months|crop trend","crop_trend"),
      (r"compare all estates|which estate|estate ranking","compare_estates"),
      (r"high grown|low grown|elevation","estate_region_comparison"),
      (r"compare all divisions|which division|division comparison","compare_divisions"),
      (r"top.*field|highest.*field|lowest.*field","rank_fields"),
      (r"problem|issue|warning|anomaly|outlier|reconciliation","data_quality"),
      (r"source|evidence|workbook|sheet|formula","source_evidence"),
      (r"what data|available|what can i ask|catalog","catalog")]
    op="summary"
    for pat,x in rules:
        if re.search(pat,lower):op=x;break
    if op=="summary" and prev and re.search(r"\bit\b|\bthat\b|\bthose\b|more detail|show fields",lower):op=prev.get("operation","summary")
    return {"operation":op,"limit":20,"explanation_request":bool(re.search(r"why|reason|explain|insight",lower))}

def llm_plan(q,context,prev):
    prompt=("Return JSON only. Never SQL. Select operation from: "+", ".join(sorted(OPERATIONS))+
      '. Schema: {"operation":"summary","limit":20,"explanation_request":false}. '
      "Use previous plan for follow-ups. Do not invent estate/month/division. /no_think")
    payload={"model":OLLAMA_MODEL,"messages":[{"role":"system","content":prompt},{"role":"user","content":json.dumps({"question":q,"context":context,"previous_plan":prev},default=str)}],
      "stream":False,"think":False,"format":"json","options":{"temperature":0,"num_predict":200}}
    req=urlrequest.Request(f"{OLLAMA_URL}/api/chat",data=json.dumps(payload).encode(),headers={"Content-Type":"application/json"},method="POST")
    try:
        with urlrequest.urlopen(req,timeout=90) as r:res=json.loads(r.read().decode())
        text=re.sub(r"<think>.*?</think>","",res.get("message",{}).get("content",""),flags=re.S|re.I).strip()
        if "```" in text:
            m=re.findall(r"```(?:json)?\s*(.*?)```",text,re.S);text=m[-1] if m else text
        a=text.find("{");b=text.rfind("}")
        return json.loads(text[a:b+1]) if a>=0 and b>a else None
    except Exception:return None

def make_plan(q,context,prev):
    fb=fallback_plan(q,prev);raw=llm_plan(q,context,prev) or {}
    op=raw.get("operation") if raw.get("operation") in OPERATIONS else fb["operation"]
    try:limit=max(1,min(int(raw.get("limit",fb["limit"])),100))
    except Exception:limit=20
    return {"operation":op,"limit":limit,"explanation_request":bool(raw.get("explanation_request",fb["explanation_request"])),"tables":OP_TABLES[op],"validated":True,"sql_generated_by_llm":False}

def governance_note(c,bid):
    if not table_exists(c,"data_quality_issues"):return None,[]
    rs=rows(c,"SELECT * FROM data_quality_issues WHERE batch_id=? ORDER BY status,severity",[bid])
    warnings=[r["manager_message"] for r in rs if r["status"]=="OPEN"]
    x=next((r for r in rs if r["issue_type"]=="SOURCE_RECONCILIATION" and r["metric_name"]=="green_leaf_kg"),None)
    if not x:return None,warnings
    if x["status"]=="RESOLVED":
        return f"[OFFICIAL METRIC APPROVED] Official green leaf is {fmt(x['official_value'],0)} kg according to {x['official_source']}. Alternative source difference: {fmt(x['difference_value'],0)} kg ({fmt(x['difference_pct'],2)}%). Approved by {x['resolved_by']}.",warnings
    return f"[MANAGEMENT ALERT - RECONCILIATION PENDING] G Report: {fmt(x['source_a_value'],0)} kg; Plucking Plan: {fmt(x['source_b_value'],0)} kg; difference: {fmt(x['difference_value'],0)} kg ({fmt(x['difference_pct'],2)}%). No official value approved.",warnings

def explain(q,answer,evidence,limitations):
    payload={"model":OLLAMA_MODEL,"messages":[{"role":"system","content":"Rewrite in clear management language using only supplied facts. Separate confirmed findings from possible factors. Never invent causes. Return final answer only. /no_think"},{"role":"user","content":json.dumps({"question":q,"answer":answer,"evidence":evidence[:25],"limitations":limitations},default=str)}],"stream":False,"think":False,"options":{"temperature":0,"num_predict":350}}
    req=urlrequest.Request(f"{OLLAMA_URL}/api/chat",data=json.dumps(payload).encode(),headers={"Content-Type":"application/json"},method="POST")
    try:
        with urlrequest.urlopen(req,timeout=90) as r:res=json.loads(r.read().decode())
        text=re.sub(r"<think>.*?</think>","",res.get("message",{}).get("content",""),flags=re.S|re.I).strip()
        if not text or re.search(r"we are given|rules to follow|the user asks",text,re.I):return answer
        return text
    except Exception:return answer

def execute(c,plan,b,division,user):
    op=plan["operation"];limit=plan["limit"];bid=b["batch_id"]
    if op=="summary":
        role_access(user,"crop");w="batch_id=?";p=[bid]
        if division:w+=" AND division_name=?";p.append(division)
        g=rows(c,f"SELECT SUM(total_green_leaf_kg) g,AVG(average_plucking_kg) ap FROM g_report_metrics WHERE {w}",p)[0]
        pp=rows(c,f"SELECT SUM(anticipated_green_leaf_kg) plan,SUM(actual_green_leaf_kg) actual FROM plucking_plan_fields WHERE {w}",p)[0]
        cr=rows(c,f"SELECT SUM(made_tea_crop_kg) crop,SUM(made_tea_crop_kg)/NULLIF(SUM(extent_ha),0) yph FROM crop_division_summary WHERE {w}",p)[0]
        cp=rows(c,"SELECT rupees_per_kg,cost_per_hectare FROM cop_line_items WHERE batch_id=? AND UPPER(description) LIKE '%TOTAL%C.O.P%' ORDER BY line_number DESC LIMIT 1",[bid]);cp=cp[0] if cp else {}
        d=float(pp["actual"])-float(g["g"]) if pp["actual"] is not None and g["g"] is not None else None
        ans=f"{division or b['estate_name']} summary for {b['reporting_month']}: G Report green leaf {fmt(g['g'],0)} kg; Plucking Plan actual {fmt(pp['actual'],0)} kg; made-tea crop {fmt(cr['crop'],0)} kg; weighted yield {fmt(cr['yph'],2)} kg/ha; COP Rs. {fmt(cp.get('rupees_per_kg'),2)}/kg."
        if d is not None:ans+=f" Source difference {fmt(d,0)} kg."
        data=[{"metric":"G Report green leaf","value":g["g"],"unit":"kg"},{"metric":"Plucking Plan actual","value":pp["actual"],"unit":"kg"},{"metric":"Difference","value":d,"unit":"kg"},{"metric":"Made tea","value":cr["crop"],"unit":"kg"},{"metric":"Yield","value":cr["yph"],"unit":"kg/ha"},{"metric":"COP","value":cp.get("rupees_per_kg"),"unit":"Rs/kg"}]
        return ans,["g_report_metrics","plucking_plan_fields","crop_division_summary","cop_line_items"],data,{"type":"bar","title":"Green Leaf Source Comparison","labels":["G Report","Plucking Plan"],"values":[g["g"],pp["actual"]],"unit":"kg"},["Cause cannot be confirmed from summary totals alone."]
    if op=="compare_divisions":
        role_access(user,"crop")
        data=rows(c,"""WITH g AS(SELECT division_name,SUM(total_green_leaf_kg) g FROM g_report_metrics WHERE batch_id=? GROUP BY division_name),
          p AS(SELECT division_name,SUM(anticipated_green_leaf_kg) plan,SUM(actual_green_leaf_kg) actual FROM plucking_plan_fields WHERE batch_id=? GROUP BY division_name),
          x AS(SELECT division_name,SUM(made_tea_crop_kg) crop,SUM(made_tea_crop_kg)/NULLIF(SUM(extent_ha),0) yph FROM crop_division_summary WHERE batch_id=? GROUP BY division_name)
          SELECT COALESCE(g.division_name,p.division_name,x.division_name) division_name,ROUND(g.g,2) g_report_green_leaf_kg,ROUND(p.plan,2) anticipated_green_leaf_kg,
          ROUND(p.actual,2) actual_green_leaf_kg,ROUND(p.actual-p.plan,2) plan_variance_kg,ROUND(x.crop,2) made_tea_crop_kg,ROUND(x.yph,2) yield_kg_per_ha
          FROM g FULL JOIN p ON p.division_name=g.division_name FULL JOIN x ON x.division_name=COALESCE(g.division_name,p.division_name) ORDER BY division_name""",[bid,bid,bid])
        valid=[r for r in data if r["yield_kg_per_ha"] is not None]
        if valid:
            hi=max(valid,key=lambda r:r["yield_kg_per_ha"]);lo=min(valid,key=lambda r:r["yield_kg_per_ha"])
            ans=f"{hi['division_name']} had the highest yield at {fmt(hi['yield_kg_per_ha'],2)} kg/ha; {lo['division_name']} had the lowest at {fmt(lo['yield_kg_per_ha'],2)} kg/ha. Difference {fmt(float(hi['yield_kg_per_ha'])-float(lo['yield_kg_per_ha']),2)} kg/ha."
        else:ans="No division comparison data."
        return ans,["g_report_metrics","plucking_plan_fields","crop_division_summary"],data,{"type":"bar","title":"Yield by Division","labels":[r["division_name"] for r in data],"values":[r["yield_kg_per_ha"] for r in data],"unit":"kg/ha"},["Performance difference is confirmed; operational cause is not proven."]
    if op in {"compare_months","crop_trend"}:
        role_access(user,"crop")
        data=rows(c,"SELECT reporting_month,SUM(COALESCE(approved_value,actual_value)) made_tea_crop_kg FROM metric_facts WHERE LOWER(estate_name)=LOWER(?) AND metric_name='made_tea_crop_kg' AND field_no IS NULL GROUP BY reporting_month ORDER BY reporting_month",[b["estate_name"]])
        ans=f"{len(data)} parsed month(s) available." if len(data)>=2 else "Only one parsed month is available. Upload another month to compare."
        return ans,["metric_facts"],data,{"type":"line","title":"Monthly Made-Tea Crop","labels":[r["reporting_month"] for r in data],"values":[r["made_tea_crop_kg"] for r in data],"unit":"kg"} if data else None,["Month comparison needs at least two consistent parsed months."]
    if op=="compare_estates":
        role_access(user,"crop")
        data=rows(c,"SELECT estate_name,SUM(COALESCE(approved_value,actual_value)) made_tea_crop_kg FROM metric_facts WHERE reporting_month=? AND metric_name='made_tea_crop_kg' AND field_no IS NULL GROUP BY estate_name ORDER BY made_tea_crop_kg DESC",[b["reporting_month"]])
        data=[r for r in data if estate_access(user,r["estate_name"])]
        ans=f"{data[0]['estate_name']} ranked highest at {fmt(data[0]['made_tea_crop_kg'],0)} kg." if len(data)>=2 else "Load at least two accessible estates for comparison."
        return ans,["metric_facts"],data,{"type":"bar","title":"Crop by Estate","labels":[r["estate_name"] for r in data],"values":[r["made_tea_crop_kg"] for r in data],"unit":"kg"} if data else None,[]


    if op=="rank_fields":
        role_access(user,"field");cond=["batch_id=?"];p=[bid]
        if division:cond.append("division_name=?");p.append(division)
        p.append(limit)
        data=rows(c,f"SELECT division_name,field_no,extent_ha,made_tea_crop_kg,ROUND(yield_kg_per_ha,2) yield_kg_per_ha FROM crop_field_performance WHERE {' AND '.join(cond)} ORDER BY yield_kg_per_ha DESC NULLS LAST LIMIT ?",p)
        ans=f"Top displayed field: {data[0]['division_name']} {data[0]['field_no']} at {fmt(data[0]['yield_kg_per_ha'],2)} kg/ha." if data else "No field data."
        return ans,["crop_field_performance"],data,{"type":"bar","title":"Top Field Yields","labels":[f"{r['division_name']} {r['field_no']}" for r in data],"values":[r["yield_kg_per_ha"] for r in data],"unit":"kg/ha"} if data else None,[]
    if op=="low_yield_high_fertilizer":
        role_access(user,"agriculture");cond=["x.batch_id=?"];p=[bid]
        if division:cond.append("x.division_name=?");p.append(division)
        p.append(limit)
        data=rows(c,f"""WITH f AS(SELECT batch_id,division_name,CAST(field_no AS VARCHAR) field_no,AVG(activity_value_number) fert
          FROM agricultural_activity_records WHERE UPPER(activity_type) LIKE '%FERTIL%' AND activity_value_number IS NOT NULL GROUP BY batch_id,division_name,CAST(field_no AS VARCHAR)),
          x AS(SELECT batch_id,division_name,CAST(field_no AS VARCHAR) field_no,extent_ha,yield_kg_per_ha FROM crop_field_performance),
          s AS(SELECT x.division_name,x.field_no,x.extent_ha,ROUND(x.yield_kg_per_ha,2) yield_kg_per_ha,ROUND(f.fert,2) avg_fertilizer_rate,
          PERCENT_RANK() OVER(ORDER BY x.yield_kg_per_ha) yr,PERCENT_RANK() OVER(ORDER BY f.fert) fr
          FROM x JOIN f ON f.batch_id=x.batch_id AND UPPER(f.division_name)=UPPER(x.division_name) AND f.field_no=x.field_no WHERE {' AND '.join(cond)})
          SELECT * FROM s WHERE yr<=.4 AND fr>=.6 ORDER BY yield_kg_per_ha,avg_fertilizer_rate DESC LIMIT ?""",p)
        ans=f"{len(data)} field(s) are in the lower 40% of yield and upper 40% of recorded fertilizer rate." if data else "No fields met the screening thresholds."
        return ans,["crop_field_performance","agricultural_activity_records"],data,{"type":"scatter","title":"Yield vs Fertilizer","labels":[f"{r['division_name']} {r['field_no']}" for r in data],"x_values":[r["avg_fertilizer_rate"] for r in data],"y_values":[r["yield_kg_per_ha"] for r in data],"x_unit":"fertilizer units","y_unit":"kg/ha"} if data else None,["Association does not prove fertilizer caused the yield result."]
    if op=="actual_below_plan":
        role_access(user,"crop");cond=["batch_id=?"];p=[bid]
        if division:cond.append("division_name=?");p.append(division)
        data=rows(c,f"""SELECT division_name,COUNT(*) field_count,SUM(anticipated_green_leaf_kg) anticipated_green_leaf_kg,
          SUM(actual_green_leaf_kg) actual_green_leaf_kg,SUM(actual_green_leaf_kg)-SUM(anticipated_green_leaf_kg) variance_kg
          FROM plucking_plan_fields WHERE {' AND '.join(cond)} GROUP BY division_name
          HAVING SUM(actual_green_leaf_kg)<SUM(anticipated_green_leaf_kg) ORDER BY variance_kg""",p)
        ans=f"{len(data)} division(s) recorded actual crop below plan." if data else "No division recorded actual crop below plan."
        return ans,["plucking_plan_fields"],data,{"type":"grouped_bar","title":"Plan vs Actual","labels":[r["division_name"] for r in data],"series":[{"name":"Plan","values":[r["anticipated_green_leaf_kg"] for r in data]},{"name":"Actual","values":[r["actual_green_leaf_kg"] for r in data]}],"unit":"kg"} if data else None,[]
    if op=="cop_drivers":
        role_access(user,"cop")
        data=rows(c,"SELECT description,ROUND(rupees_per_kg,2) rupees_per_kg,ROUND(cost_share*100,2) cost_share_pct,ROUND(cost_per_hectare,2) cost_per_hectare FROM cop_line_items WHERE batch_id=? AND rupees_per_kg>0 AND UPPER(description) NOT LIKE '%TOTAL%' AND UPPER(description) NOT LIKE '%SUB TOTAL%' ORDER BY rupees_per_kg DESC LIMIT ?",[bid,limit])
        ans="Largest COP drivers: "+", ".join(f"{r['description']} (Rs. {fmt(r['rupees_per_kg'],2)}/kg)" for r in data[:5])+"." if data else "No COP driver data."
        return ans,["cop_line_items"],mask(data,user),{"type":"bar","title":"Largest COP Drivers","labels":[r["description"] for r in data],"values":[r["rupees_per_kg"] for r in data],"unit":"Rs/kg"} if data else None,["The file identifies cost components but does not prove the operational reason they are high."]
    if op=="contractor_cost_per_hectare":
        role_access(user,"contracts");cond=["batch_id=?"];p=[bid]
        if division:cond.append("division_name=?");p.append(division)
        p.append(limit)
        data=rows(c,f"""SELECT contractor_name,division_name,COUNT(*) contract_records,SUM(hectares_awarded) hectares_awarded,
          SUM(COALESCE(contract_value,labour_total_cost_rs)) total_cost_rs,
          SUM(COALESCE(contract_value,labour_total_cost_rs))/NULLIF(SUM(hectares_awarded),0) cost_per_hectare_rs
          FROM contract_records WHERE {' AND '.join(cond)} AND contractor_name IS NOT NULL AND TRIM(contractor_name)<>''
          GROUP BY contractor_name,division_name HAVING SUM(hectares_awarded)>0 ORDER BY cost_per_hectare_rs DESC LIMIT ?""",p)
        ans=f"{data[0]['contractor_name']} has the highest displayed cost per hectare at Rs. {fmt(data[0]['cost_per_hectare_rs'],2)}." if data else "No contractor rows contain both cost and hectares."
        return ans,["contract_records"],mask(data,user),{"type":"bar","title":"Contractor Cost per Hectare","labels":[f"{r['contractor_name']} - {r['division_name']}" for r in data],"values":[r["cost_per_hectare_rs"] for r in data],"unit":"Rs/ha"} if data else None,["Calculated from available cost divided by awarded hectares."]
    if op=="estate_region_comparison":
        role_access(user,"crop")
        data=rows(c,"""SELECT COALESCE(e.elevation_region,'UNCLASSIFIED') elevation_region,
          COUNT(DISTINCT f.estate_name) estate_count,SUM(COALESCE(f.approved_value,f.actual_value)) made_tea_crop_kg
          FROM metric_facts f LEFT JOIN estate_master e ON LOWER(e.estate_name)=LOWER(f.estate_name)
          WHERE f.reporting_month=? AND f.metric_name='made_tea_crop_kg' AND f.field_no IS NULL
          GROUP BY COALESCE(e.elevation_region,'UNCLASSIFIED') ORDER BY made_tea_crop_kg DESC""",[b["reporting_month"]])
        ans=f"{len(data)} elevation group(s) available." if len(data)>=2 else "HG/LG comparison requires at least two classified estate groups."
        return ans,["metric_facts","estate_master"],data,{"type":"bar","title":"Crop by Elevation","labels":[r["elevation_region"] for r in data],"values":[r["made_tea_crop_kg"] for r in data],"unit":"kg"} if data else None,["Maintain elevation_region in estate_master."]
    if op=="data_quality":
        role_access(user,"issues")
        data=rows(c,"SELECT issue_type,severity,metric_name,status,manager_message,source_a_value,source_b_value,difference_value,difference_pct,official_source,official_value,resolved_by,resolution_note FROM data_quality_issues WHERE batch_id=? ORDER BY status,severity",[bid]) if table_exists(c,"data_quality_issues") else []
        ans=f"{sum(1 for r in data if r['status']=='OPEN')} open management/data-quality issue(s)."
        return ans,["data_quality_issues"],mask(data,user),None,[]
    if op=="source_evidence":
        role_access(user,"evidence")
        data=rows(c,"SELECT metric_name,source_workbook,source_sheet,source_line,formula_used,estate_name,reporting_month,division_name,field_no,approval_status,data_owner,parsed_at,evidence_note FROM metric_evidence WHERE batch_id=? ORDER BY metric_name LIMIT ?",[bid,limit]) if table_exists(c,"metric_evidence") else []
        return f"{len(data)} evidence row(s) displayed.",["metric_evidence"],mask(data,user),None,["Source line appears when captured by the parser."]
    role_access(user,"crop")
    data=[{"operation":x,"tables":", ".join(OP_TABLES[x])} for x in sorted(OPERATIONS)]
    return f"{len(data)} safe analytical operations are available.",["metric_facts"],data,None,[]

def report_file(fmt_type,title,answer,data,evidence,warnings):
    REPORT_DIR.mkdir(parents=True,exist_ok=True);name=re.sub(r"[^A-Za-z0-9_.-]+","_",title)+f"_{datetime.now():%Y%m%d_%H%M%S}.{fmt_type}";path=REPORT_DIR/name
    if fmt_type=="xlsx":
        from openpyxl import Workbook
        from openpyxl.styles import Font,PatternFill,Alignment
        wb=Workbook();ws=wb.active;ws.title="Report";ws["A1"]=title;ws["A1"].font=Font(size=16,bold=True);ws["A3"]="Management Answer";ws["A4"]=answer;ws["A4"].alignment=Alignment(wrap_text=True)
        rr=7
        for section,items in (("Warnings",[{"warning":w} for w in warnings]),("Supporting Data",data),("Evidence",evidence)):
            if not items:continue
            ws.cell(rr,1,section).font=Font(bold=True);rr+=1;headers=list(dict.fromkeys(k for x in items for k in x))[:12]
            for j,h in enumerate(headers,1):cell=ws.cell(rr,j,h);cell.font=Font(bold=True,color="FFFFFF");cell.fill=PatternFill("solid",fgColor="1E593F")
            for x in items[:100]:
                rr+=1
                for j,h in enumerate(headers,1):ws.cell(rr,j,x.get(h))
            rr+=2
        wb.save(path)
    elif fmt_type=="docx":
        from docx import Document
        d=Document();d.add_heading(title,0);d.add_heading("Management Answer",1);d.add_paragraph(answer)
        if warnings:d.add_heading("Warnings",1);[d.add_paragraph(w,style="List Bullet") for w in warnings]
        if data:
            h=list(dict.fromkeys(k for x in data for k in x))[:8];t=d.add_table(rows=1,cols=len(h));t.style="Table Grid"
            for j,x in enumerate(h):t.rows[0].cells[j].text=x
            for x in data[:40]:
                cs=t.add_row().cells
                for j,k in enumerate(h):cs[j].text=str(x.get(k,""))
        d.save(path)
    elif fmt_type=="pdf":
        from reportlab.lib.pagesizes import A4,landscape
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.platypus import SimpleDocTemplate,Paragraph,Spacer,Table,TableStyle
        from reportlab.lib import colors
        st=getSampleStyleSheet();story=[Paragraph(title,st["Title"]),Paragraph(answer,st["BodyText"]),Spacer(1,10)]
        if data:
            h=list(dict.fromkeys(k for x in data for k in x))[:7];tab=Table([h]+[[str(x.get(k,"")) for k in h] for x in data[:35]],repeatRows=1);tab.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.HexColor("#1E593F")),("TEXTCOLOR",(0,0),(-1,0),colors.white),("GRID",(0,0),(-1,-1),.5,colors.grey),("FONTSIZE",(0,0),(-1,-1),7)]));story.append(tab)
        SimpleDocTemplate(str(path),pagesize=landscape(A4)).build(story)
    else:
        from pptx import Presentation
        from pptx.util import Inches,Pt
        p=Presentation();s=p.slides.add_slide(p.slide_layouts[0]);s.shapes.title.text=title;s.placeholders[1].text=f"Generated {datetime.now():%Y-%m-%d %H:%M}"
        s=p.slides.add_slide(p.slide_layouts[1]);s.shapes.title.text="Management Answer";s.placeholders[1].text=answer
        if data:
            s=p.slides.add_slide(p.slide_layouts[5]);s.shapes.title.text="Supporting Data";h=list(dict.fromkeys(k for x in data for k in x))[:6];show=data[:12];t=s.shapes.add_table(len(show)+1,len(h),Inches(.4),Inches(1.2),Inches(12.5),Inches(5.4)).table
            for j,x in enumerate(h):t.cell(0,j).text=x
            for i,x in enumerate(show,1):
                for j,k in enumerate(h):t.cell(i,j).text=str(x.get(k,""))
        p.save(path)
    return path


def page(request,path):
    try:current_user(request)
    except HTTPException:return RedirectResponse("/pro-login")
    return FileResponse(STATIC/path)

@router.get("/pro-login",include_in_schema=False)
def login_page():return FileResponse(STATIC/"pro_login.html")
@router.get("/pro-assistant",include_in_schema=False)
def assistant_page(request:Request):return page(request,"pro_assistant.html")
@router.get("/pro-governance",include_in_schema=False)
def governance_page(request:Request):return page(request,"pro_governance.html")
@router.get("/pro-admin",include_in_schema=False)
def admin_page(request:Request):
    try:
        u=current_user(request)
        if u["role"]!="system_admin":raise HTTPException(403)
    except HTTPException:return RedirectResponse("/pro-login")
    return FileResponse(STATIC/"pro_admin.html")

@router.post("/api/pro/login")
def login(x:LoginIn,response:Response):
    c=con(False)
    try:
        ensure_tables(c);r=rows(c,"SELECT * FROM app_users WHERE username=? AND is_active=TRUE LIMIT 1",[x.username.lower().strip()])
        if not r:raise HTTPException(401,"Invalid username or password")
        u=r[0];_,dig=phash(x.password,u["salt"])
        if not hmac.compare_digest(dig,u["password_hash"]):raise HTTPException(401,"Invalid username or password")
        token=secrets.token_urlsafe(40);c.execute("INSERT INTO app_sessions VALUES(?,?,?,?,NULL)",[str(uuid.uuid4()),hashlib.sha256(token.encode()).hexdigest(),u["user_id"],datetime.now()+timedelta(hours=8)])
        response.set_cookie(COOKIE,token,httponly=True,samesite="strict",max_age=28800)
        return {"status":"authenticated","display_name":u["display_name"],"role":u["role"]}
    finally:c.close()

@router.post("/api/pro/logout")
def logout(request:Request,response:Response):
    token=request.cookies.get(COOKIE)
    if token:
        c=con(False)
        try:c.execute("UPDATE app_sessions SET revoked_at=? WHERE token_hash=?",[datetime.now(),hashlib.sha256(token.encode()).hexdigest()])
        finally:c.close()
    response.delete_cookie(COOKIE);return {"status":"logged_out"}

@router.get("/api/pro/options")
def options(request:Request):
    u=current_user(request);c=con()
    try:
        bs=batches(c,u)
        return {"user":u,"batches":[{**b,"data_mode":"APPROVED" if "APPROVED" in str(b["analytics_status"]).upper() and "PENDING" not in str(b["analytics_status"]).upper() else "PREVIEW","divisions":divisions(c,b["batch_id"])} for b in bs],
          "examples":["Why did Fairfield perform better than Walaha?","Compare June with May.","Which fields have low yield but high fertilizer usage?","Show divisions where actual crop was below plan.","What are the main reasons for the high COP?","Which contractor has the highest cost per hectare?","Compare all estates by made-tea crop.","Show the crop trend for the last six months.","Compare high-grown and low-grown estates.","Show all management problems.","Show source workbook, sheet, formula and approval status."]}
    finally:c.close()

@router.post("/api/pro/plan")
def plan_api(x:ChatIn,request:Request):
    u=current_user(request);c=con()
    try:
        b=get_batch(c,u,x.estate_name,x.reporting_month);ds=divisions(c,b["batch_id"])
        ctx={"selected_estate":b["estate_name"],"selected_month":b["reporting_month"],"selected_division":x.division_name,
             "available_estates":sorted({z["estate_name"] for z in batches(c,u)}),"available_months":sorted({z["reporting_month"] for z in batches(c,u)}),"available_divisions":ds}
        return {"plan":make_plan(x.question,ctx,x.previous_plan),"read_only":True,"llm_sql":False}
    finally:c.close()

@router.post("/api/pro/ask")
def ask_api(x:ChatIn,request:Request):
    u=current_user(request);c=con()
    try:
        b=get_batch(c,u,x.estate_name,x.reporting_month);ds=divisions(c,b["batch_id"]);div=infer_division(x.question,x.division_name,b["estate_name"],ds)
        ctx={"selected_estate":b["estate_name"],"selected_month":b["reporting_month"],"selected_division":div,
             "available_estates":sorted({z["estate_name"] for z in batches(c,u)}),"available_months":sorted({z["reporting_month"] for z in batches(c,u)}),"available_divisions":ds,"history":x.history[-8:]}
        pl=make_plan(x.question,ctx,x.previous_plan);ans,tables_used,data,chart,limitations=execute(c,pl,b,div,u);note,warnings=governance_note(c,b["batch_id"])
        if note and pl["operation"] in {"summary","compare_divisions","compare_months","compare_estates","data_quality"}:ans=note+"\n\n"+ans
        mode="APPROVED" if "APPROVED" in str(b["analytics_status"]).upper() and "PENDING" not in str(b["analytics_status"]).upper() else "PREVIEW"
        if mode=="PREVIEW":ans="[PREVIEW - NOT FINAL APPROVED] "+ans
        if pl["explanation_request"]:ans=explain(x.question,ans,data,limitations)
        evidence=rows(c,"SELECT metric_name,source_workbook,source_sheet,source_line,formula_used,estate_name,reporting_month,division_name,field_no,approval_status,data_owner,parsed_at,evidence_note FROM metric_evidence WHERE batch_id=? ORDER BY metric_name LIMIT 100",[b["batch_id"]]) if table_exists(c,"metric_evidence") else []
        return {"answer":ans,"plan":pl,"estate_name":b["estate_name"],"reporting_month":b["reporting_month"],"division_name":div,
          "data_mode":mode,"data_status":b["analytics_status"],"source_tables":tables_used,"supporting_rows":mask(data,u),"evidence":mask(evidence,u),
          "chart":chart,"warnings":warnings,"limitations":limitations,"user_role":u["role"],"sql_generated_by_llm":False}
    finally:c.close()

@router.post("/api/pro/refresh-metrics")
def refresh_api(request:Request):
    u=current_user(request)
    if u["role"] not in {"system_admin","management"}:raise HTTPException(403,"Access denied")
    return refresh_facts()

@router.post("/api/pro/governance/scan")
def scan_api(request:Request):
    u=current_user(request)
    if u["role"] not in {"system_admin","management","regional","estate"}:raise HTTPException(403,"Access denied")
    return scan_all()

@router.get("/api/pro/governance/issues")
def issues_api(request:Request):
    u=current_user(request);c=con(False)
    try:
        ensure_tables(c);data=rows(c,"SELECT * FROM data_quality_issues ORDER BY CASE severity WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END,status,detected_at DESC")
        return [r for r in mask(data,u) if estate_access(u,r["estate_name"])]
    finally:c.close()

@router.post("/api/pro/governance/issues/{issue_id}/action")
def issue_action(issue_id:str,x:IssueActionIn,request:Request):
    u=current_user(request)
    if u["role"] not in {"system_admin","management","regional","estate"}:raise HTTPException(403)
    if x.confirm_text.strip().upper()!="CONFIRM MANAGEMENT ACTION":raise HTTPException(400,'Type "CONFIRM MANAGEMENT ACTION"')
    c=con(False)
    try:
        ensure_tables(c);rr=rows(c,"SELECT * FROM data_quality_issues WHERE issue_id=? LIMIT 1",[issue_id])
        if not rr:raise HTTPException(404,"Issue not found")
        r=rr[0]
        if not estate_access(u,r["estate_name"]):raise HTTPException(403)
        if r["status"]=="RESOLVED":raise HTTPException(409,"Already resolved")
        source=value=None
        if x.action=="APPROVE_OFFICIAL_METRIC":
            if u["role"] not in {"system_admin","management"}:raise HTTPException(403)
            if r["issue_type"]!="SOURCE_RECONCILIATION":raise HTTPException(400,"Not a source reconciliation issue")
            source=(x.official_source or "").upper()
            if source=="G_REPORT":value=r["source_a_value"]
            elif source=="PLUCKING_PLAN":value=r["source_b_value"]
            elif source=="MANUAL" and x.manual_value is not None:value=x.manual_value
            else:raise HTTPException(400,"Select G_REPORT, PLUCKING_PLAN or MANUAL")
        now=datetime.now();c.execute("BEGIN")
        try:
            c.execute("UPDATE data_quality_issues SET status='RESOLVED',resolved_at=?,resolved_by=?,resolution_note=?,official_source=?,official_value=? WHERE issue_id=?",[now,u["display_name"],x.note,source,value,issue_id])
            if x.action=="APPROVE_OFFICIAL_METRIC":
                c.execute("INSERT INTO metric_approvals VALUES(?,?,?,?,?,?,?,?,?,?,?)",[str(uuid.uuid4()),issue_id,r["batch_id"],r["estate_name"],r["reporting_month"],r["metric_name"],source,value,u["display_name"],x.note,now])
            c.execute("INSERT INTO governance_audit_log VALUES(?,?,?,?,?,?,?)",[str(uuid.uuid4()),x.action,issue_id,r["batch_id"],u["display_name"],x.note,now])
            if x.action=="RETURN_FOR_CORRECTION":
                c.execute("INSERT OR REPLACE INTO month_controls VALUES(?,?,?,'RETURNED_FOR_CORRECTION',FALSE,?,?,?)",[r["batch_id"],r["estate_name"],r["reporting_month"],x.note,u["display_name"],now])
            c.execute("COMMIT")
        except Exception:c.execute("ROLLBACK");raise
    finally:c.close()
    refresh_facts();return {"status":"recorded","official_source":source,"official_value":value}

@router.post("/api/pro/governance/month/{batch_id}/control")
def month_action(batch_id:str,x:MonthActionIn,request:Request):
    u=current_user(request)
    if u["role"] not in {"system_admin","management"}:raise HTTPException(403)
    if x.confirm_text.strip().upper()!="CONFIRM MONTH CONTROL":raise HTTPException(400,'Type "CONFIRM MONTH CONTROL"')
    c=con(False)
    try:
        ensure_tables(c);rr=rows(c,"SELECT * FROM batch_analytics WHERE batch_id=? LIMIT 1",[batch_id])
        if not rr:raise HTTPException(404,"Batch not found")
        b=rr[0]
        status={"APPROVE_MONTH":"APPROVED_PRODUCTION_DATA","REJECT_MONTH":"REJECTED","RETURN_FOR_CORRECTION":"RETURNED_FOR_CORRECTION","MARK_REPLACEMENT_REQUIRED":"REPLACEMENT_REQUIRED","LOCK_MONTH":"LOCKED","UNLOCK_MONTH":"UNLOCKED","ADD_NOTE":"NOTE_ADDED"}[x.action]
        old=c.execute("SELECT is_locked FROM month_controls WHERE batch_id=?",[batch_id]).fetchone();locked=bool(old[0]) if old else False
        if x.action=="LOCK_MONTH":locked=True
        if x.action=="UNLOCK_MONTH":locked=False
        if x.action=="APPROVE_MONTH":
            n=c.execute("SELECT COUNT(*) FROM data_quality_issues WHERE batch_id=? AND status='OPEN' AND severity IN ('CRITICAL','HIGH')",[batch_id]).fetchone()[0]
            if n:raise HTTPException(409,f"{n} critical/high issue(s) remain")
        now=datetime.now();c.execute("INSERT OR REPLACE INTO month_controls VALUES(?,?,?,?,?,?,?,?)",[batch_id,b["estate_name"],b["reporting_month"],status,locked,x.note,u["display_name"],now])
        if x.action=="APPROVE_MONTH":c.execute("UPDATE batch_analytics SET analytics_status='APPROVED_PRODUCTION_DATA',metrics_approved_at=? WHERE batch_id=?",[now,batch_id])
        elif x.action in {"REJECT_MONTH","RETURN_FOR_CORRECTION","MARK_REPLACEMENT_REQUIRED"}:c.execute("UPDATE batch_analytics SET analytics_status=? WHERE batch_id=?",[status,batch_id])
        c.execute("INSERT INTO governance_audit_log VALUES(?,?,?,?,?,?,?)",[str(uuid.uuid4()),x.action,None,batch_id,u["display_name"],x.note,now])
    finally:c.close()
    refresh_facts();return {"status":status,"is_locked":locked}

@router.post("/api/pro/reports")
def report_api(x:ReportIn,request:Request):
    u=current_user(request);c=con()
    try:
        b=get_batch(c,u,x.estate_name,x.reporting_month);op={"management_summary":"summary","division_comparison":"compare_divisions","data_quality":"data_quality","cop_analysis":"cop_drivers"}[x.kind]
        ans,tables_used,data,chart,limitations=execute(c,{"operation":op,"limit":50},b,x.division_name,u);note,warnings=governance_note(c,b["batch_id"])
        if note:ans=note+"\n\n"+ans
        evidence=rows(c,"SELECT metric_name,source_workbook,source_sheet,source_line,formula_used,approval_status,data_owner,parsed_at FROM metric_evidence WHERE batch_id=? LIMIT 100",[b["batch_id"]]) if table_exists(c,"metric_evidence") else []
    finally:c.close()
    path=report_file(x.format,f"{x.estate_name} {x.reporting_month} {x.kind.replace('_',' ').title()}",ans,data,evidence,warnings)
    return {"status":"generated","download_url":f"/pro-reports/{path.name}","filename":path.name}

@router.get("/pro-reports/{filename}")
def download_report(filename:str,request:Request):
    current_user(request);path=REPORT_DIR/Path(filename).name
    if not path.exists():raise HTTPException(404)
    return FileResponse(path,filename=path.name)

@router.get("/api/pro/admin/users")
def users_api(request:Request):
    u=current_user(request)
    if u["role"]!="system_admin":raise HTTPException(403)
    c=con(False)
    try:ensure_tables(c);return rows(c,"SELECT user_id,username,display_name,role,estate_scope_json,is_active,created_at,updated_at FROM app_users ORDER BY username")
    finally:c.close()

@router.post("/api/pro/admin/users")
def create_user_api(x:UserIn,request:Request):
    u=current_user(request)
    if u["role"]!="system_admin":raise HTTPException(403)
    c=con(False)
    try:ensure_tables(c);return add_user(c,x.username,x.display_name,x.password,x.role,x.estate_scope)
    finally:c.close()

@router.get("/api/pro/model-status")
def model_status(request:Request):
    current_user(request);return {"ollama_url":OLLAMA_URL,"model":OLLAMA_MODEL,"architecture":"Validated JSON plan + prewritten read-only DuckDB queries + LLM explanation. The LLM never runs SQL."}
