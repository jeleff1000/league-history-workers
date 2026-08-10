"""Audit JSON/JSONL/CSV candidate files for directly promotable facts."""
from __future__ import annotations
import argparse, csv, json
from pathlib import Path
from typing import Any, Iterator

SIGNALS={"win","loss","tie","team_points","source_team_points","is_playoffs","champion","is_championship","final_playoff_seed","made_playoffs","clutch_equity","source_clutch_equity"}
SETTINGS={"pass_td","scoring_pass_td","pass_td_fill","playoff_teams","playoff_teams_fill","roster_flx","roster_flx_fill","roster_super_flex","roster_super_flex_fill","roster_idp","roster_idp_fill","best_ball","best_ball_fill","sleeper_best_ball"}

def records(value: Any, parent: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
    context=dict(parent or {})
    if isinstance(value,dict):
        own={str(k):v for k,v in value.items() if not isinstance(v,(dict,list))}; merged={**context,**own}
        if own: yield merged
        for child in value.values():
            if isinstance(child,(dict,list)): yield from records(child,merged)
    elif isinstance(value,list):
        for child in value: yield from records(child,context)

def load(path: Path) -> Iterator[dict[str,Any]]:
    if path.suffix.lower()=='.csv':
        with path.open(newline='',encoding='utf-8',errors='replace') as h: yield from records(list(csv.DictReader(h)))
    elif path.suffix.lower()=='.jsonl':
        for line in path.read_text(encoding='utf-8',errors='replace').splitlines():
            if line.strip():
                try: yield from records(json.loads(line))
                except json.JSONDecodeError: pass
    elif path.suffix.lower()=='.json':
        try: yield from records(json.loads(path.read_text(encoding='utf-8',errors='replace')))
        except json.JSONDecodeError: pass

def present(row): return {str(k).strip().lower() for k,v in row.items() if v not in (None,'',[],{})}

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--candidates',type=Path,required=True); ap.add_argument('--out',type=Path,required=True); a=ap.parse_args(); report=[]
    for path in sorted(a.candidates.rglob('*')):
        if path.suffix.lower() not in {'.json','.jsonl','.csv'}: continue
        row_count=signal_rows=settings_rows=0; fields=set()
        for row in load(path):
            keys=present(row); fields |= keys; identity='db_name' in keys and 'year' in keys; week='week' in keys and bool({'team_key','manager','nfl_player_id'} & keys); row_count+=1
            if identity and week and keys & SIGNALS: signal_rows+=1
            if identity and keys & SETTINGS: settings_rows+=1
        disposition='structured_direct_signal_payload' if signal_rows else ('structured_settings_payload' if settings_rows else 'structured_metadata_or_status')
        report.append({'file':str(path),'rows_seen':row_count,'signal_rows':signal_rows,'settings_rows':settings_rows,'fields':sorted(fields),'disposition':disposition})
    result={'files':len(report),'direct_signal_files':sum(r['signal_rows']>0 for r in report),'settings_payload_files':sum(r['settings_rows']>0 for r in report),'metadata_or_status_files':sum(r['disposition']=='structured_metadata_or_status' for r in report),'promotable_structured_rows':sum(r['signal_rows'] for r in report),'settings_rows':sum(r['settings_rows'] for r in report),'records':report}
    a.out.parent.mkdir(parents=True,exist_ok=True); a.out.write_text(json.dumps(result,indent=2,sort_keys=True)+'\n',encoding='utf-8'); print(json.dumps({k:v for k,v in result.items() if k!='records'},sort_keys=True))
if __name__=='__main__': main()
