-- Drop any existing tables with the same name
DROP TABLE IF EXISTS comments CASCADE;
DROP TABLE IF EXISTS submissions CASCADE;
DROP TABLE IF EXISTS authors CASCADE;
DROP TABLE IF EXISTS subreddits CASCADE;

-- authors
CREATE TABLE authors (
  id              TEXT PRIMARY KEY, 
  retrieved_on    BIGINT,
  name            TEXT UNIQUE,       -- username
  created_utc     BIGINT,
  link_karma      INTEGER,
  comment_karma   INTEGER,
  profile_img     TEXT,
  profile_color   TEXT,
  profile_over_18 BOOLEAN
);

-- subreddits
CREATE TABLE subreddits (
  banner_background_image TEXT,
  created_utc             BIGINT,
  description             TEXT,
  display_name            TEXT UNIQUE,  -- unique constraint
  header_img              TEXT,
  hide_ads                BOOLEAN,
  id                      TEXT,
  over_18                 BOOLEAN,
  public_description      TEXT,
  retrieved_utc           BIGINT,
  name                    TEXT PRIMARY KEY,
  subreddit_type          TEXT,
  subscribers             BIGINT,
  title                   TEXT,
  whitelist_status        TEXT
);

-- submissions 
CREATE TABLE submissions (
  downs        INTEGER,
  url          TEXT,
  id           TEXT PRIMARY KEY,
  edited       BOOLEAN,
  num_reports  INTEGER,
  created_utc  BIGINT,
  name         TEXT UNIQUE,
  title        TEXT,
  author       TEXT,
  permalink    TEXT,
  num_comments INTEGER,
  likes        TEXT,
  subreddit_id TEXT,              -- submissions.subreddit_id - subreddits.name
  ups          INTEGER
);

-- comments
CREATE TABLE comments (
  distinguished            TEXT,
  downs                    INTEGER,
  created_utc              BIGINT,
  controversiality         INTEGER,
  edited                   BOOLEAN,
  gilded                   INTEGER,
  author_flair_css_class   TEXT,
  id                       TEXT PRIMARY KEY,
  author                   TEXT,   -- comments.author - authors.name
  retrieved_on             BIGINT,
  score_hidden             BOOLEAN,
  subreddit_id             TEXT,   --comments.subreddit_id - subreddits.name
  score                    INTEGER,
  name                     TEXT UNIQUE,
  author_flair_text        TEXT,
  link_id                  TEXT,
  archived                 BOOLEAN,
  ups                      INTEGER,
  parent_id                TEXT,
  subreddit                TEXT,   --comments.subreddit - subreddits.display_name
  body                     TEXT
);

CREATE INDEX ON submissions (subreddit_id);
CREATE INDEX ON submissions (author);      -- submissions.author - authors.name
CREATE INDEX ON comments (subreddit_id);
CREATE INDEX ON comments (author);
CREATE INDEX ON comments (link_id);