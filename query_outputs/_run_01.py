import os, csv, datetime as dt, duckdb, lance

so = {'aws_access_key_id': os.environ['R2_ACCESS_KEY_ID'],
      'aws_secret_access_key': os.environ['R2_SECRET_ACCESS_KEY'],
      'endpoint': os.environ.get('R2_ENDPOINT') or f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
      'region': 'auto'}
ds = lance.dataset('s3://data-sink/active/govcon_active_awards_map_serving/', storage_options=so)
con = duckdb.connect(':memory:'); con.execute("PRAGMA threads=4")
con.register('r', ds.scanner(columns=['vertical', 'winner_uei', 'winner_name',
                                      'current_value', 'pop_current_end']).to_reader())
con.execute("CREATE TABLE t AS SELECT * FROM r")

today = dt.datetime.now(dt.timezone.utc).date()
hi = today + dt.timedelta(days=180)
WHERE = (f"vertical = 'Aerospace & Defense' "
         f"AND pop_current_end >= DATE '{today.isoformat()}' "
         f"AND pop_current_end <= DATE '{hi.isoformat()}'")

n_companies = con.execute(f"SELECT count(DISTINCT winner_uei) FROM t WHERE {WHERE}").fetchone()[0]
total_amt   = con.execute(f"SELECT coalesce(sum(current_value),0) FROM t WHERE {WHERE}").fetchone()[0]
n_awards    = con.execute(f"SELECT count(*) FROM t WHERE {WHERE}").fetchone()[0]
top = con.execute(f"""
    SELECT winner_uei, any_value(winner_name) AS company, sum(current_value) AS amt, count(*) AS awards
    FROM t WHERE {WHERE}
    GROUP BY winner_uei
    ORDER BY amt DESC NULLS LAST
    LIMIT 10
""").fetchall()

out = 'query_outputs/01_aerospace_up_for_recompete.csv'
with open(out, 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['query', 'Aerospace contracts up for recompete'])
    w.writerow(['filter', "vertical = 'Aerospace & Defense' AND days_until_expiry <= 180"])
    w.writerow(['as_of', today.isoformat()])
    w.writerow(['recompete_window', f"{today.isoformat()} .. {hi.isoformat()}"])
    w.writerow(['num_companies', n_companies])
    w.writerow(['federal_awards_amount_aggregate_usd', f"{total_amt:.2f}"])
    w.writerow(['num_awards', n_awards])
    w.writerow([])
    w.writerow(['rank', 'company', 'uei', 'federal_award_amount_usd', 'awards'])
    for i, (uei, name, amt, awards) in enumerate(top, 1):
        w.writerow([i, name, uei, f"{(amt or 0):.2f}", awards])

print(f"# of Companies:                  {n_companies:,}")
print(f"Federal Awards $ Amount (agg):   ${total_amt:,.0f}")
print(f"(# of awards in pool:            {n_awards:,})")
print(f"\nTop 10 companies by Federal Award amount:")
print(f"{'#':>2}  {'company':45s} {'amount':>18s}  awards")
for i, (uei, name, amt, awards) in enumerate(top, 1):
    nm = (name or '')[:45]
    print(f"{i:>2}  {nm:45s} {('$'+format(amt or 0, ',.0f')):>18s}  {awards}")
print(f"\nCSV -> {out}")
