CREATE TABLE WellData.SearchRes (
  `EventDate` DateTime,
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
  EventDate SETTINGS index_granularity = 8192

CREATE TABLE WellData.SearchRes
(
    EventDate DateTime,
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
ORDER BY EventDate;
