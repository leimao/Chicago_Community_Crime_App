
--In case Tez does not work
SET hive.execution.engine=mr;

SET hivevar:SCALE_CONST=1;

DROP TABLE IF EXISTS leimao_taxitrips_counts;

CREATE TABLE leimao_taxitrips_counts (
    pickup_community smallint,
    dropoff_community smallint,
    trip_duration_sum bigint,
    total_cost_sum bigint,
    num_trips bigint
);


INSERT OVERWRITE TABLE leimao_taxitrips_counts
    SELECT tx.pickupCommunity, tx.dropoffCommunity, SUM(tx.tripDuration), SUM(CAST (REGEXP_REPLACE(tx.tripTotal, "[$()]", "") AS DOUBLE)), COUNT(*) FROM leimao_taxitrips tx WHERE tx.pickupCommunity IS NOT NULL AND tx.dropoffCommunity IS NOT NULL AND tx.tripTotal IS NOT NULL GROUP BY tx.pickupCommunity, tx.dropoffCommunity;
