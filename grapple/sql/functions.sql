CREATE OR REPLACE FUNCTION clean_data_tables()
RETURNS void AS $$
    -- cleanup ledger table
    DELETE FROM ripple_ledger 
    WHERE price1 = 0 
    OR price2 = 0 
    OR amount1 = 0 
    OR amount2 = 0;
    -- cleanup resampled transactions table
    DELETE FROM resampled_ledger
    WHERE high1 = 0 
    OR high2 = 0 
    OR low1 = 0 
    OR low2 = 0 
    OR close1 = 0 
    OR close2 = 0 
    OR open1 = 0 
    OR open2 = 0 
    OR volume1 = 0 
    OR volume2 = 0;
$$ LANGUAGE SQL VOLATILE;
