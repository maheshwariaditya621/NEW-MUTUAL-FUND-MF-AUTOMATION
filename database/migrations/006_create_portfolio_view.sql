-- Create a human-readable view for portfolio holdings
CREATE OR REPLACE VIEW portfolio_details_view AS
SELECT p.year, p.month, a.amc_name, s.scheme_name, s.scheme_category, c.company_name, c.sector, c.industry, c.isin, h.quantity, h.market_value_inr, h.percent_of_nav
FROM
    equity_holdings h
    JOIN scheme_snapshots ss ON h.snapshot_id = ss.snapshot_id
    JOIN schemes s ON ss.scheme_id = s.scheme_id
    JOIN amcs a ON s.amc_id = a.amc_id
    JOIN periods p ON ss.period_id = p.period_id
    JOIN companies c ON h.company_id = c.company_id
ORDER BY p.year DESC, p.month DESC, a.amc_name, s.scheme_name, h.market_value_inr DESC;