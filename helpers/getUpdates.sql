SELECT
  COALESCE("masterKey", "userKey") AS "key",
  COALESCE("masterForeignKey", "userForeignKey") AS "foreignKey",
  COALESCE("masterLastUpdated", "userLastUpdated") AS "lastUpdated",
  COALESCE("masterHash", "userHash") AS "hash",
  COALESCE("masterData", "userData") AS "data",
  CASE
    WHEN
      "masterKey" IS NOT NULL
      AND CAST("masterLastUpdated" AS INTEGER) <= {{lastUpdated}}
      AND CAST("userLastUpdated" AS INTEGER) > {{lastUpdated}}
    THEN 'updated'
    WHEN
      "masterKey" IS NULL
      AND CAST("userLastUpdated" AS INTEGER) > {{lastUpdated}}
    THEN 'created'
    WHEN "masterKey" IS NULL
      AND CAST("userLastUpdated" AS INTEGER) <= {{lastUpdated}}
    THEN 'missing'  -- uses the data field
    WHEN "masterKey" IS NOT NULL
      AND "userKey" IS NOT NULL
      AND "userHash" != "masterHash"
      THEN 'updated elsewhere'
    WHEN "masterKey" IS NOT NULL
      AND "userKey" IS NOT NULL
      -- AND "userHash" = "masterHash"
    THEN 'existing'
    WHEN "masterKey" IS NOT NULL
        -- This has to be before conflict in order
        AND CAST("masterLastUpdated" AS INTEGER) > {{lastUpdated}}
        AND CAST("userLastUpdated" AS INTEGER) > {{lastUpdated}}
        AND "userHash" = "masterHash"
    THEN 'merged conflict' --uses hash fields and the force field
    WHEN "masterKey" IS NOT NULL
        AND CAST("masterLastUpdated" AS INTEGER) > {{lastUpdated}}
        AND CAST("userLastUpdated" AS INTEGER) > {{lastUpdated}}
    THEN 'conflict' --uses hash fields and the force field
    WHEN "userKey" IS NULL
        AND CAST("masterLastUpdated" AS INTEGER) <= {{lastUpdated}}
    THEN 'removed'
    WHEN "userKey" IS NULL
        AND CAST("masterLastUpdated" AS INTEGER) > {{lastUpdated}}
    THEN 'created elsewhere' -- uses the data field
    ELSE 'unknown'
  END AS "action"
    FROM
    (
    SELECT
      "user"."key" AS "userKey",
      "user"."foreignKey" AS "userForeignKey",
      "user"."hash" AS "userHash",
      "user"."data" AS "userData",
      "user"."lastUpdated" AS "userLastUpdated",
      "master"."key" AS "masterKey",
      "master"."foreignKey" AS "masterForeignKey",
      "master"."hash" AS "masterHash",
      "master"."data" AS "masterData",
      "master"."lastUpdated" AS "masterLastUpdated"
    FROM
    (SELECT "key", "foreignKey", "hash", "data", "lastUpdated" FROM "{{tableName}}" WHERE "source" = 'user') AS "user"
    LEFT JOIN
    (SELECT "key", "foreignKey", "hash", "data", "lastUpdated" FROM "{{tableName}}" WHERE "source" = 'master') AS "master"
    ON "user"."key" = "master"."key"
    UNION ALL
    SELECT
      "user"."key" AS "userKey",
      "user"."foreignKey" AS "userForeignKey",
      "user"."hash" AS "userHash",
      "user"."data" AS "userData",
      "user"."lastUpdated" AS "userLastUpdated",
      "master"."key" AS "masterKey",
      "master"."foreignKey" AS "masterForeignKey",
      "master"."hash" AS "masterHash",
      "master"."data" AS "masterData",
      "master"."lastUpdated" AS "masterLastUpdated"
    FROM
    (SELECT "key", "foreignKey", "hash", "data", "lastUpdated" FROM "{{tableName}}" WHERE "source" = 'master') AS "master"
    LEFT JOIN
    (SELECT "key", "foreignKey", "hash", "data", "lastUpdated" FROM "{{tableName}}" WHERE "source" = 'user') AS "user"
    ON "user"."key" = "master"."key"
    WHERE "user"."key" IS NULL
  );
