CREATE TABLE WellData.SearchRes (
  `Keyword` String,
  `Total` String,
  `Items` String,
  `SR.Title` Array(String),
  `SR.Snippet` Array(String),
  `SR.Url` Array(String),
  `SR.SiteUrls` Array(String),
  `GR.Title` Array(String),
  `GR.Snippet` Array(String),
  `GR.Url` Array(String),
  `GR.SiteUrls` Array(String)
) ENGINE = MergeTree()
ORDER BY
  Keyword SETTINGS index_granularity = 8192

CREATE TABLE SearchRes
(
    Keyword String,
    Total String,
    Items String,
    SR Nested 
    (
        Title String,
        Snippet String,
        Url String,
        SiteUrls String
    ),
    GR Nested 
    (
        Title String,
        Snippet String,
        Url String,
        SiteUrls String
    )
)
ENGINE = MergeTree()
ORDER BY Keyword;
