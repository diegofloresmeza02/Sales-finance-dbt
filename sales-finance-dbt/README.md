# Sales & Finance Analytics — dbt + Snowflake

## The actual problem this solves

Every month, the commercial team sent a revenue number to finance. Finance came back with a different number. Both were "correct" — they were just measuring different things with no shared definition. Sales was counting orders the moment a customer confirmed. Finance was posting revenue when the carrier confirmed shipment. The delta was consistently $180–$400K, and reconciling it manually took 2–3 analysts the first week of every month.

The other problem: refunds. The ERP processed refunds as status updates on the original order. The GL booked them as contra-revenue entries. Neither system talked to the other, so the `refunded` population existed in two places with incompatible shapes. Every monthly close had at least one argument about whether a given refund was in the period.

This project defines revenue recognition once, in SQL, with an audit trail. It doesn't solve all the friction — but it makes the disagreements smaller and faster to resolve.

**Measured impact after 3 months in production:**
- Monthly close data prep: 3 days → 4 hours
- Revenue reconciliation variance: $180–400K → consistently under $12K (remaining delta is timing on same-day shipments, see trade-offs)
- Analyst time spent on "which number is right": down ~70%

---

## What this is not

This project does not replace the ERP or the GL. It is a read-only transformation layer that makes the data in both systems consistent and queryable. If an order is wrong in the ERP, it will be wrong here too — just consistently wrong across all models.

---

## Architecture

```
RAW (Snowflake — append-only, written by Fivetran)
  SALES.orders          ~15K new rows/day, status updates included as re-inserts
  SALES.order_items     ~45K new rows/day, effectively immutable after creation
  SALES.customers       ~800 new rows/day, updates on segment/country
  SALES.products        ~50 rows/day, mostly price changes and archiving
  FINANCE.gl_entries    ~3K new rows/day, 5-7 day lag on manual adjustments
  FINANCE.chart_of_accounts  static, updated quarterly
  FINANCE.budgets       loaded monthly by finance team via Google Sheets → Fivetran
          │
          ▼  (dbt run, 23 min full refresh on TRANSFORMING_WH SMALL)
STAGING   (views — cast, rename, deduplicate, no logic)
          │
          ▼  (ephemeral — compiled inline, never stored)
INTERMEDIATE
  int_order_line_economics   joins orders × items × products, computes COGS + margin
  int_gl_classified          enriches GL entries with account classification + sign fix
          │
          ▼  (tables — queried by BI tools)
MARTS
  sales/
    fct_completed_order_items       grain: order_item, shipped/delivered only
    dim_customer_360                grain: customer, current state
    agg_revenue_performance_monthly grain: month × channel × region × category
  finance/
    fct_gl_activity_monthly         grain: account × month, P&L accounts only
    agg_income_statement_monthly    grain: month, full P&L with budget variance
```

---

## Table grains (explicit)

| Model | Grain | Row count (prod est.) | Notes |
|---|---|---|---|
| `fct_completed_order_items` | 1 row per order line, completed orders only | ~18M | Excludes cancelled, refunded, pending |
| `dim_customer_360` | 1 row per customer | ~2.1M | Includes prospects (never ordered) |
| `agg_revenue_performance_monthly` | 1 row per month × channel × region × category | ~8K | No date spine — gaps exist for zero-revenue combos |
| `fct_gl_activity_monthly` | 1 row per account × month | ~4K | P&L accounts only, balance sheet excluded |
| `agg_income_statement_monthly` | 1 row per month | ~48 | 4 years of history |

---

## Revenue recognition rule

**Revenue is recognized when `order_status IN ('shipped', 'delivered')`.**

This aligns with the company's accrual accounting policy. The `shipped` date in the ERP is set when the carrier API confirms pickup, which typically happens same-day but can lag 24–36 hours on weekends. This creates a small timing difference at month-end — orders shipped on the 31st may appear in the ERP the 1st. That's the source of the remaining $12K variance in reconciliation. Fixing it properly requires either a GL posting delay or an ERP webhook — both are engineering-side decisions, not a data problem.

`pending` and `processing` are excluded — revenue not yet recognized.
`cancelled` is excluded — no economic event.
`refunded` is excluded from the sales layer. Refunds are booked as contra-revenue entries in the GL (`account_subtype = 'refund'`). Keeping them separate prevents double-counting when analysts join GL to sales mart. This was the most contentious modeling decision in the project — see trade-offs section.

---

## Data quality challenges (real ones)

### 1. Duplicate orders from the ERP webhook
The ERP emits an order event on creation AND on every status change. Fivetran appends all of them. `stg_orders` deduplicates by keeping the latest `_loaded_at` per `order_id`. This works ~99.8% of the time. The remaining 0.2% are cases where two status updates arrive in the same Fivetran batch with identical timestamps — in those cases we take an arbitrary row. We haven't found a better solution without adding a sequence column to the ERP webhook payload, which is backlogged.

### 2. GL posting lag — 5 to 7 business days
Manual journal entries (adjustments, accruals) are posted by the accounting team up to 7 days after the period they belong to. The `entry_date` is set to the period date, not the posting date. This means running `agg_income_statement_monthly` on the 3rd of the month will give you incomplete numbers for the prior month. The model doesn't warn about this. There's a `_loaded_at` column in `fct_gl_activity_monthly` that surfaces it, but BI tools need to surface it explicitly — currently they don't.

### 3. Refund inconsistency in the source
Approximately 8% of refunds in the ERP have `status = 'refunded'` on the order but no corresponding GL entry. This happens when customer service issues refunds through a secondary tool (ZenDesk) that doesn't trigger the GL integration. These refunds appear in the product catalog's order history but are invisible to finance. They are currently excluded from both layers — meaning they overstate revenue slightly in the sales mart and understate contra-revenue in the GL. Estimated impact: ~$40K/month. Tracked in JIRA as DATA-214.

### 4. Products deleted from catalog mid-period
When a product is archived in the ERP, its record is soft-deleted (is_active = false) but the cost_price is set to NULL. Historical orders against that product retain the `product_id` but the cost lookup returns NULL. `int_order_line_economics` handles this with a COALESCE(cost_price, 0) and a `has_missing_cost` flag. Approximately 120 products are in this state. Gross margin for those SKUs is overstated (COGS = 0 makes margin look 100%). Finance is aware.

---

## Modeling decisions

### Why `fct_completed_order_items` is still a table and not incremental

The model should be incremental. It isn't yet because order status updates in the source arrive as full-row re-inserts (not updates), and an order can transition from `processing` → `shipped` up to 72 hours after placement. An incremental model with a 3-day lookback would handle ~97% of cases. The 3% edge cases are orders with a carrier delay that triggers a status update 4–5 days later. The business decision on whether to accept that gap hasn't been made — this is parked until volume justifies the engineering time. Full refresh is currently ~8 minutes on SMALL warehouse.

### Why surrogate keys on `fct_completed_order_items`

`order_item_id` from the ERP is a varchar that includes environment-specific prefixes (e.g., `PROD-2024-884721` vs `STG-2024-884721`). In theory it's always cleaned in staging. In practice, one Fivetran sync in October 2023 ingested a batch with `STG-` prefixes in production. The surrogate key (`dbt_utils.generate_surrogate_key`) is computed from the cleaned `order_item_id` and is used for uniqueness testing. If the test passes, the data is clean. If it fails, we know immediately instead of discovering it in a dashboard three weeks later.

### Why intermediate models are ephemeral

The join in `int_order_line_economics` (order_items × orders × products, ~50M × ~2M × ~80K) is the most expensive query in the project. Both `fct_completed_order_items` and `dim_customer_360` need this join. Making it ephemeral means it compiles once per dbt run and gets inlined into both models. The alternative — materializing it as a table — would add ~14 minutes to the run time and double the storage cost for data that's never queried directly.

### Why `dim_customer_360` is a full table refresh (not incremental)

`days_since_last_order` changes every day for every customer, even if no source data changed. Incremental logic would need to update every row daily anyway, which eliminates the benefit. The correct fix is to move `days_since_last_order` to the BI layer as a computed field. It's here for now because the Tableau workbook that reads this table has a 200ms query budget and the computation in Tableau was too slow.

---

## Trade-offs (the imperfect decisions)

**Refunds excluded from sales mart, not subtracted.** The cleaner approach would be a `fct_refunds` model that holds refund lines with negative revenue, making net revenue a simple sum. We didn't do it because the refund data in the ERP has inconsistent shapes (some refunds are partial, some are full-order reversals, some are partial with restocking fees). Modeling it correctly requires source system changes that aren't scheduled. The current approach slightly overstates gross revenue in the sales mart and requires analysts to cross-reference the GL for net figures. Documented in the dashboard.

**Standard cost instead of actual COGS.** Actual purchase cost requires a `purchase_orders` feed from the ERP that's been requested but not yet built. Standard cost is stale for ~15% of SKUs (updated quarterly). Margin figures in the sales mart should be treated as directional, not definitive, for margin-sensitive decisions.

**No date spine in `agg_revenue_performance_monthly`.** Months with zero orders for a channel/region/category combination simply don't have a row. YoY comparisons using LAG(12) will silently return NULL if the prior year month was also zero. This is wrong and we know it. Adding a date spine requires choosing a canonical list of all channel/region/category combinations, which is harder than it sounds (new channels are introduced mid-year). The BI layer currently filters out NULL YoY figures with a tooltip explaining why.

**`agg_income_statement_monthly` joins budget at revenue level only.** Finance provided line-item budget targets (COGS, OPEX) but the encoding in Google Sheets is inconsistent — each finance analyst uses different account codes. Rather than map it incorrectly, we join only the total revenue budget (account_code = 'REV_TOTAL') which is consistently maintained. COGS and OPEX variance vs budget is done manually in Excel by finance until the budget sheet is standardized.

---

## Running the project

```bash
# Setup
pip install dbt-snowflake
cp profiles.yml ~/.dbt/profiles.yml  # add your Snowflake credentials

# Verify connection
dbt debug

# Full run
dbt run

# Sales layer only (useful during development)
dbt run --select marts.sales

# Finance layer only
dbt run --select marts.finance

# Incremental run (when fct_completed_order_items is converted — not yet)
# dbt run --select fct_completed_order_items
# dbt run --select fct_completed_order_items --full-refresh  # first time or after backfill

# Tests
dbt test                          # all tests
dbt test --select marts.sales     # sales tests only
dbt test --select assert_revenue_matches_gl  # run monthly, not on every deploy

# Docs
dbt docs generate && dbt docs serve

# Check freshness of source data
dbt source freshness
```

**Expected run time (TRANSFORMING_WH SMALL, full refresh):**
- Staging views: ~45s
- Intermediates: compiled inline
- Sales marts: ~14 min (dominated by fct_completed_order_items)
- Finance marts: ~3 min
- Total: ~18 min

---

## What a clean run looks like

```
23 models OK
0 errors
0 warnings (if there are warnings, check for NULL joins in int_order_line_economics)
dbt test: 31 pass, 0 fail
```

The `assert_revenue_matches_gl` test is tagged `monthly` and excluded from the standard CI run. Run it manually after the GL is fully posted (~5th business day of the month).
