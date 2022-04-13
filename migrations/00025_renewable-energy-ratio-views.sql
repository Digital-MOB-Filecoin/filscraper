CREATE MATERIALIZED VIEW IF NOT EXISTS fil_renewable_energy_ratio_miner_view
AS
with cumulative_energy as( SELECT
                        date,
                        miner,
                        SUM((ROUND(AVG(total)) * 24 * 0.0000086973 + SUM(total_per_day) * 0.0601295421) * 1.93) OVER(PARTITION BY miner ORDER BY date) AS cumulative_energykWh
                    FROM fil_miner_view_days_v4
                    GROUP BY miner,date,total_per_day
                    ORDER BY date
                 ),
    cumulative_renewable_energy  as(
                   SELECT
                       date,
                       miner,
                       SUM(SUM(energyWh / 1000)) OVER (PARTITION BY miner ORDER BY date) as cumulative_renewable_energykWh
                       FROM fil_renewable_energy_view_v3
                       GROUP BY miner, date, energyWh
                       ORDER BY date
                 )

SELECT cumulative_energy.miner,
       cumulative_energy.date,
       cumulative_renewable_energy.cumulative_renewable_energykWh,
       cumulative_energy.cumulative_energykWh,
       COALESCE((cumulative_renewable_energy.cumulative_renewable_energykWh / NULLIF(cumulative_energy.cumulative_energykWh,0)),0) as ratio
       FROM cumulative_renewable_energy
                        FULL JOIN cumulative_energy ON cumulative_renewable_energy.miner = cumulative_energy.miner AND cumulative_renewable_energy.date = cumulative_energy.date
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_fil_renewable_energy_ratio_miner_view ON fil_renewable_energy_ratio_miner_view(miner,date);

CREATE MATERIALIZED VIEW IF NOT EXISTS fil_renewable_energy_ratio_network_view
AS
SELECT date_trunc('day', date) as date,
       COALESCE(SUM(cumulative_renewable_energykwh) / NULLIF(SUM(cumulative_energykwh),0),0) as ratio
       FROM fil_renewable_energy_ratio_miner_view
       GROUP BY date ORDER BY date
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_fil_renewable_energy_ratio_network_view ON fil_renewable_energy_ratio_network_view(date);