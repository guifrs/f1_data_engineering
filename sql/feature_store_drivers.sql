WITH tb_results AS (
    SELECT DriverNumber,
           DriverId,
           TeamId,
           COALESCE(INT(FLOAT(Position)), 99) AS Position,
           COALESCE(INT(FLOAT(GridPosition)), 99) AS GridPosition,
           Status,
           Points,
           Laps,
           identifier,
           TO_DATE(TO_TIMESTAMP(date)) AS dtEvent,
           year,
           RoundNumber,
           Location
    FROM results
    WHERE TO_DATE(TO_TIMESTAMP(date)) <= '{date}'
),

tb_event AS (
    SELECT DISTINCT dtEvent, RoundNumber
    FROM tb_results
),

tb_drivers AS (
    SELECT DISTINCT DriverId
    FROM tb_results
    WHERE dtEvent >= (TO_DATE('{date}') - INTERVAL 1 YEARS)
),

tb_agg_life AS (
    SELECT DriverId,
           MAX('{date}') AS dtRef,
           YEAR(TO_DATE('{date}')) AS dtYear,
           COUNT(*) AS qtdRuns,
           SUM(CASE WHEN identifier = 'race' THEN 1 ELSE 0 END) AS qtdRace,
           SUM(CASE WHEN identifier = 'sprint' THEN 1 ELSE 0 END) AS qtdSprint,
           AVG(Position) AS avgPosition,
           AVG(CASE WHEN identifier = 'race' THEN Position END) AS avgPositionRace,
           AVG(CASE WHEN identifier = 'sprint' THEN Position END) AS avgPositionSprint,
           AVG(GridPosition) AS avgGridPosition,
           AVG(CASE WHEN identifier = 'race' THEN GridPosition END) AS avgGridPositionRace,
           AVG(CASE WHEN identifier = 'sprint' THEN GridPosition END) AS avgGridPositionSprint,
           AVG(GridPosition - Position) AS avgPositionGain,
           AVG(CASE WHEN identifier = 'race' THEN GridPosition - Position END) AS avgPositionRaceGain,
           AVG(CASE WHEN identifier = 'sprint' THEN GridPosition - Position END) AS avgPositionSprintGain,
           PERCENTILE(Position, 0.5) AS medianPosition,
           PERCENTILE(CASE WHEN identifier = 'race' THEN Position END, 0.5) AS medianPositionRace,
           PERCENTILE(CASE WHEN identifier = 'sprint' THEN Position END, 0.5) AS medianPositionSprint,
           PERCENTILE(GridPosition, 0.5) AS medianGridPosition,
           PERCENTILE(CASE WHEN identifier = 'race' THEN GridPosition END, 0.5) AS medianGridPositionRace,
           PERCENTILE(CASE WHEN identifier = 'sprint' THEN GridPosition END, 0.5) AS medianGridPositionSprint,
           PERCENTILE(GridPosition - Position, 0.5) AS medianPositionGain,
           PERCENTILE(CASE WHEN identifier = 'race' THEN GridPosition - Position END, 0.5) AS medianPositionRaceGain,
           PERCENTILE(CASE WHEN identifier = 'sprint' THEN GridPosition - Position END, 0.5) AS medianPositionSprintGain,
           SUM(CASE WHEN Position = 1 THEN 1 ELSE 0 END) AS qtdeWins,
           SUM(CASE WHEN Position <= 3 THEN 1 ELSE 0 END) AS qtdePodiums,
           SUM(CASE WHEN GridPosition = 1 THEN 1 ELSE 0 END) AS qtdePoles
    FROM tb_results
    WHERE DriverId IN (SELECT DriverId FROM tb_drivers)
    GROUP BY DriverId
),

tb_agg_last_year AS (
    SELECT DriverId,
           AVG(Position) AS avgPosition1Year,
           AVG(CASE WHEN identifier = 'race' THEN Position END) AS avgPositionRace1Year,
           AVG(CASE WHEN identifier = 'sprint' THEN Position END) AS avgPositionSprint1Year,
           AVG(GridPosition) AS avgGridPosition1Year,
           AVG(CASE WHEN identifier = 'race' THEN GridPosition END) AS avgGridPositionRace1Year,
           AVG(CASE WHEN identifier = 'sprint' THEN GridPosition END) AS avgGridPositionSprint1Year,
           AVG(GridPosition - Position) AS avgPositionGain1Year,
           AVG(CASE WHEN identifier = 'race' THEN GridPosition - Position END) AS avgPositionRaceGain1Year,
           AVG(CASE WHEN identifier = 'sprint' THEN GridPosition - Position END) AS avgPositionSprintGain1Year,
           PERCENTILE(Position, 0.5) AS medianPosition1Year,
           PERCENTILE(CASE WHEN identifier = 'race' THEN Position END, 0.5) AS medianPositionRace1Year,
           PERCENTILE(CASE WHEN identifier = 'sprint' THEN Position END, 0.5) AS medianPositionSprint1Year,
           PERCENTILE(GridPosition, 0.5) AS medianGridPosition1Year,
           PERCENTILE(CASE WHEN identifier = 'race' THEN GridPosition END, 0.5) AS medianGridPositionRace1Year,
           PERCENTILE(CASE WHEN identifier = 'sprint' THEN GridPosition END, 0.5) AS medianGridPositionSprint1Year,
           PERCENTILE(GridPosition - Position, 0.5) AS medianPositionGain1Year,
           PERCENTILE(CASE WHEN identifier = 'race' THEN GridPosition - Position END, 0.5) AS medianPositionRaceGain1Year,
           PERCENTILE(CASE WHEN identifier = 'sprint' THEN GridPosition - Position END, 0.5) AS medianPositionSprintGain1Year,
           SUM(CASE WHEN Position = 1 THEN 1 ELSE 0 END) AS qtdeWins1Year,
           SUM(CASE WHEN Position <= 3 THEN 1 ELSE 0 END) AS qtdePodiums1Year,
           SUM(CASE WHEN GridPosition = 1 THEN 1 ELSE 0 END) AS qtdePoles1Year
    FROM tb_results
    WHERE DriverId IN (SELECT DriverId FROM tb_drivers)
      AND dtEvent >= (TO_DATE('{date}') - INTERVAL 1 YEARS)
    GROUP BY DriverId
),

tb_agg_current_temp AS (
    SELECT DriverId,
           AVG(Position) AS avgPositionCurrentTemp,
           AVG(CASE WHEN identifier = 'race' THEN Position END) AS avgPositionRaceCurrentTemp,
           AVG(CASE WHEN identifier = 'sprint' THEN Position END) AS avgPositionSprintCurrentTemp,
           AVG(GridPosition) AS avgGridPositionCurrentTemp,
           AVG(CASE WHEN identifier = 'race' THEN GridPosition END) AS avgGridPositionRaceCurrentTemp,
           AVG(CASE WHEN identifier = 'sprint' THEN GridPosition END) AS avgGridPositionSprintCurrentTemp,
           AVG(GridPosition - Position) AS avgPositioCurrentTemp,
           AVG(CASE WHEN identifier = 'race' THEN GridPosition - Position END) AS avgPositionRaceGainCurrentTemp,
           AVG(CASE WHEN identifier = 'sprint' THEN GridPosition - Position END) AS avgPositionSprintGainCurrentTemp,
           PERCENTILE(Position, 0.5) AS medianPositionCurrentTemp,
           PERCENTILE(CASE WHEN identifier = 'race' THEN Position END, 0.5) AS medianPositionRaceCurrentTemp,
           PERCENTILE(CASE WHEN identifier = 'sprint' THEN Position END, 0.5) AS medianPositionSprintCurrentTemp,
           PERCENTILE(GridPosition, 0.5) AS medianGridPositionCurrentTemp,
           PERCENTILE(CASE WHEN identifier = 'race' THEN GridPosition END, 0.5) AS medianGridPositionRaceCurrentTemp,
           PERCENTILE(CASE WHEN identifier = 'sprint' THEN GridPosition END, 0.5) AS medianGridPositionSprintCurrentTemp,
           PERCENTILE(GridPosition - Position, 0.5) AS medianPositionGainCurrentTemp,
           PERCENTILE(CASE WHEN identifier = 'race' THEN GridPosition - Position END, 0.5) AS medianPositionRaceGainCurrentTemp,
           PERCENTILE(CASE WHEN identifier = 'sprint' THEN GridPosition - Position END, 0.5) AS medianPositionSprintGainCurrentTemp,
           SUM(CASE WHEN Position = 1 THEN 1 ELSE 0 END) AS qtdeWinsCurrentTemp,
           SUM(CASE WHEN Position <= 3 THEN 1 ELSE 0 END) AS qtdePodiumsCurrentTemp,
           SUM(CASE WHEN GridPosition = 1 THEN 1 ELSE 0 END) AS qtdePolesCurrentTemp,
           SUM(Points) AS totalPointsCurrentTemp
    FROM tb_results
    WHERE DriverId IN (SELECT DriverId FROM tb_drivers)
      AND YEAR(dtEvent) >= YEAR(TO_DATE('{date}'))
    GROUP BY DriverId
)

SELECT
    t4.RoundNumber AS tempRoundNumber,
    t1.*,
    t2.avgPosition1Year,
    t2.avgPositionRace1Year,
    t2.avgPositionSprint1Year,
    t2.avgGridPosition1Year,
    t2.avgGridPositionRace1Year,
    t2.avgGridPositionSprint1Year,
    t2.avgPositionGain1Year,
    t2.avgPositionRaceGain1Year,
    t2.avgPositionSprintGain1Year,
    t2.medianPosition1Year,
    t2.medianPositionRace1Year,
    t2.medianPositionSprint1Year,
    t2.medianGridPosition1Year,
    t2.medianGridPositionRace1Year,
    t2.medianGridPositionSprint1Year,
    t2.medianPositionGain1Year,
    t2.medianPositionRaceGain1Year,
    t2.medianPositionSprintGain1Year,
    t2.qtdeWins1Year,
    t2.qtdePodiums1Year,
    t2.qtdePoles1Year,
    t3.avgPositionCurrentTemp,
    t3.avgPositionRaceCurrentTemp,
    t3.avgPositionSprintCurrentTemp,
    t3.avgGridPositionCurrentTemp,
    t3.avgGridPositionRaceCurrentTemp,
    t3.avgGridPositionSprintCurrentTemp,
    t3.avgPositioCurrentTemp,
    t3.avgPositionRaceGainCurrentTemp,
    t3.avgPositionSprintGainCurrentTemp,
    t3.medianPositionCurrentTemp,
    t3.medianPositionRaceCurrentTemp,
    t3.medianPositionSprintCurrentTemp,
    t3.medianGridPositionCurrentTemp,
    t3.medianGridPositionRaceCurrentTemp,
    t3.medianGridPositionSprintCurrentTemp,
    t3.medianPositionGainCurrentTemp,
    t3.medianPositionRaceGainCurrentTemp,
    t3.medianPositionSprintGainCurrentTemp,
    t3.qtdeWinsCurrentTemp,
    t3.qtdePodiumsCurrentTemp,
    t3.qtdePolesCurrentTemp,
    t3.totalPointsCurrentTemp
FROM tb_agg_life AS t1
LEFT JOIN tb_agg_last_year AS t2 ON t1.DriverId = t2.DriverId
LEFT JOIN tb_agg_current_temp AS t3 ON t1.DriverId = t3.DriverId
LEFT JOIN tb_event AS t4 ON TO_DATE(t1.dtRef) = TO_DATE(t4.dtEvent);