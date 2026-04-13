"""
Instagram migration tool for Meta backup exports.

What this script does:
- Reads Meta export post data from posts_1.json.
- Resolves media URIs (e.g. media/other/...) against the account export root.
- Uploads to the target account on a daily schedule (POSTS_PER_DAY), spread
    evenly between START_HOUR and END_HOUR.
- Stores progress in PROGRESS_FILE so restarts continue where they left off.

Input schema used (posts_1.json):
    [
        {
            "media": [
                {
                    "uri": "media/other/123.jpg",
                    "creation_timestamp": 1617979289,
                    "title": "Caption text"
                }
            ],
            "title": "Caption text"  # optional top-level caption
        }
    ]

Media handling:
- 1 image -> photo upload
- 1 video -> video upload
- 2+ media items -> album upload

Optional behavior:
- Dry run mode validates all media paths and prints schedule/results.
- Best-effort comment replay maps historical comments to nearest post by
    timestamp (not exact original thread reconstruction).
- Reposts are only uploaded if the export contains repost media files.
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from instagrapi import Client
import requests
import schedule

# ---------------------------------------------------------------------------
# Configuration (all overridable via .env)
# ---------------------------------------------------------------------------
load_dotenv(override=True)


def _env_flag(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).lower() in {"1", "true", "yes"}


def _env_path(name: str, default: str) -> str:
    return os.path.expandvars(os.getenv(name, default))


USERNAME    = os.getenv("INSTAGRAM_USERNAME")
PASSWORD    = os.getenv("INSTAGRAM_PASSWORD")
SOURCE_ACCOUNT_USERNAME = os.getenv("SOURCE_ACCOUNT_USERNAME", "").strip()
ACCOUNT_EXPORT_DIR = Path(_env_path("ACCOUNT_EXPORT_DIR", "JSON/michaelcuneophotography"))
ACCOUNT_SLUG = ACCOUNT_EXPORT_DIR.name or "account"

# Path to the Meta export JSON (posts_1.json or similar)
POSTS_FILE = _env_path(
    "POSTS_FILE",
    str(ACCOUNT_EXPORT_DIR / "your_instagram_activity" / "media" / "posts_1.json"),
)
# Root folder of the Meta export (media URIs are relative to this).
# If not set, it is inferred from POSTS_FILE.
EXPORT_ROOT_ENV = os.path.expandvars(os.getenv("EXPORT_ROOT", "")).strip()
EXPORT_ROOT = Path(EXPORT_ROOT_ENV) if EXPORT_ROOT_ENV else None
ACTIVE_EXPORT_ROOT: Path | None = EXPORT_ROOT
# Persists which posts have already been uploaded
PROGRESS_FILE = _env_path("PROGRESS_FILE", f"progress_{ACCOUNT_SLUG}.json")
SESSION_FILE = _env_path("SESSION_FILE", f"session_{ACCOUNT_SLUG}.json")

POSTS_PER_DAY = int(os.getenv("POSTS_PER_DAY", "25"))
# Posting window in 24-h local time  (default 08:00 – 22:00)
START_HOUR    = int(os.getenv("START_HOUR", "8"))
END_HOUR      = int(os.getenv("END_HOUR",   "22"))

# Feature toggles
DRY_RUN = _env_flag("DRY_RUN", "false")
AUTH_CHECK = _env_flag("AUTH_CHECK", "false")
MIGRATE_FEED_POSTS = _env_flag("MIGRATE_FEED_POSTS", "true")
MIGRATE_REPOSTS = _env_flag("MIGRATE_REPOSTS", "false")
MIGRATE_ARCHIVED_POSTS = _env_flag("MIGRATE_ARCHIVED_POSTS", "false")
MIGRATE_IGTV = _env_flag("MIGRATE_IGTV", "false")
MIGRATE_STORIES = _env_flag("MIGRATE_STORIES", "false")
MIGRATE_PROFILE = _env_flag("MIGRATE_PROFILE", "false")
MIGRATE_FOLLOWING = _env_flag("MIGRATE_FOLLOWING", "false")
REPLAY_COMMENTS = _env_flag("REPLAY_COMMENTS", "false")
STOP_ON_FEATURE_ERROR = _env_flag("STOP_ON_FEATURE_ERROR", "false")

COMMENT_WINDOW_DAYS = int(os.getenv("COMMENT_WINDOW_DAYS", "14"))
FOLLOW_MAX_PER_RUN = int(os.getenv("FOLLOW_MAX_PER_RUN", "50"))
FOLLOW_SLEEP_SECONDS = float(os.getenv("FOLLOW_SLEEP_SECONDS", "2.0"))

COMMENTS_FILE = _env_path(
    "COMMENTS_FILE",
    str(ACCOUNT_EXPORT_DIR / "your_instagram_activity" / "comments" / "post_comments_1.json"),
)
REPOSTS_FILE = _env_path(
    "REPOSTS_FILE",
    str(ACCOUNT_EXPORT_DIR / "your_instagram_activity" / "media" / "reposts.json"),
)
ARCHIVED_POSTS_FILE = _env_path(
    "ARCHIVED_POSTS_FILE",
    str(ACCOUNT_EXPORT_DIR / "your_instagram_activity" / "media" / "archived_posts.json"),
)
IGTV_FILE = _env_path(
    "IGTV_FILE",
    str(ACCOUNT_EXPORT_DIR / "your_instagram_activity" / "media" / "igtv_videos.json"),
)
STORIES_FILE = _env_path(
    "STORIES_FILE",
    str(ACCOUNT_EXPORT_DIR / "your_instagram_activity" / "media" / "stories.json"),
)
PROFILE_INFO_FILE = _env_path(
    "PROFILE_INFO_FILE",
    str(ACCOUNT_EXPORT_DIR / "personal_information" / "personal_information" / "personal_information.json"),
)
PROFILE_PHOTOS_FILE = _env_path(
    "PROFILE_PHOTOS_FILE",
    str(ACCOUNT_EXPORT_DIR / "your_instagram_activity" / "media" / "profile_photos.json"),
)
FOLLOWING_FILE = _env_path(
    "FOLLOWING_FILE",
    str(ACCOUNT_EXPORT_DIR / "connections" / "followers_and_following" / "following.json"),
)

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".heic"}
VIDEO_EXTS = {".mp4", ".mov"}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def login_user() -> Client | None:
    if not USERNAME or not PASSWORD:
        logging.error("Missing INSTAGRAM_USERNAME or INSTAGRAM_PASSWORD in .env")
        return None

    client = Client()
    session_path = Path(SESSION_FILE)

    # Prefer restoring a previously trusted session to avoid unnecessary challenges.
    if session_path.exists():
        try:
            client.load_settings(str(session_path))
            client.login(USERNAME, PASSWORD)
            client.get_timeline_feed()
            logging.info("Logged in using saved session settings.")
            return client
        except Exception as e:
            logging.warning(f"Saved session login failed, falling back to fresh login: {e}")

    try:
        client.login(USERNAME, PASSWORD)
        try:
            session_path.parent.mkdir(parents=True, exist_ok=True)
            client.dump_settings(str(session_path))
            logging.info(f"Session settings saved to {session_path}.")
        except Exception as session_err:
            logging.warning(f"Could not save session settings: {session_err}")
        logging.info("Logged in successfully.")
        return client
    except Exception as e:
        logging.error(f"Login failed: {e}")
        return None

# ---------------------------------------------------------------------------
# Meta backup schema parser
# ---------------------------------------------------------------------------

def _ext(uri: str) -> str:
    return Path(uri).suffix.lower()


def _resolve(uri: str, export_root: Path) -> Path:
    """Resolve a Meta-export relative URI to an absolute path."""
    return export_root / uri


def _infer_export_root(posts_path: Path) -> Path:
    """Infer the account export root that contains both media/ and your_instagram_activity/."""
    for parent in posts_path.parents:
        if (parent / "media").exists() and (parent / "your_instagram_activity").exists():
            return parent
    # Fallback to a common layout (.../<account>/your_instagram_activity/media/posts_1.json)
    if len(posts_path.parents) >= 3:
        return posts_path.parents[2]
    return posts_path.parent


def _extract_repost_url(repost: dict[str, Any]) -> str:
    """Extract a reel/post URL from the nested label_values structure in reposts.json."""
    label_values = repost.get("label_values", [])
    stack = list(label_values)
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            href = item.get("href")
            value = item.get("value")
            if isinstance(href, str) and href.startswith("http"):
                return href
            if isinstance(value, str) and value.startswith("http"):
                return value
            nested_dict = item.get("dict")
            if isinstance(nested_dict, list):
                stack.extend(nested_dict)
    return ""


def _repair_text(value: str) -> str:
    """Repair common mojibake patterns from Meta exports when UTF-8 became Latin-1 text."""
    if not value:
        return ""

    repaired = value
    suspicious_markers = ("â", "Ã", "ð", "€", "™", "œ", "ž")
    for _ in range(2):
        if not any(marker in repaired for marker in suspicious_markers):
            break
        try:
            candidate = repaired.encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            break
        if candidate == repaired:
            break
        repaired = candidate

    return repaired.replace("\u0000", "").strip()


def _normalise_entry(raw: dict[str, Any], export_root: Path) -> dict[str, Any] | None:
    """
    Convert one entry from Meta's posts_1.json into a flat internal dict:
      {
        "caption":     str,
        "media_type":  "photo" | "video" | "album",
        "media_paths": [Path, ...],            # always a list
        "original_timestamp": int | None,
      }
    Returns None if the entry has no usable media.
    """

    # Reverse media list to prefer the most recent item for caption/timestamp if there's a mismatch
    media_items: list[dict] = raw.get("media", [])
    media_items.reverse()
    if not media_items:
        return None

    # Caption: prefer top-level title, fall back to first media item title
    caption: str = _repair_text(raw.get("title") or media_items[0].get("title") or "")

    original_ts: int | None = media_items[0].get("creation_timestamp")

    resolved_paths: list[Path] = [_resolve(m["uri"], export_root) for m in media_items if m.get("uri")]
    if not resolved_paths:
        return None

    # Determine media type
    if len(resolved_paths) > 1:
        media_type = "album"
    elif _ext(resolved_paths[0].name) in VIDEO_EXTS:
        media_type = "video"
    else:
        media_type = "photo"

    return {
        "caption":            caption,
        "media_type":         media_type,
        "media_paths":        resolved_paths,
        "original_timestamp": original_ts,
    }


def load_posts(posts_file: str) -> list[dict[str, Any]]:
    global ACTIVE_EXPORT_ROOT

    posts_path = Path(posts_file)
    if not posts_path.exists():
        raise FileNotFoundError(f"Posts file not found: {posts_path}")

    export_root = EXPORT_ROOT if EXPORT_ROOT else _infer_export_root(posts_path)
    ACTIVE_EXPORT_ROOT = export_root
    logging.info(f"Using export root: {export_root}")

    with posts_path.open("r", encoding="utf-8") as f:
        raw_list = json.load(f)

    if isinstance(raw_list, dict):
        # Wrap if someone passes {"posts": [...]}
        raw_list = raw_list.get("posts", [])

    if not isinstance(raw_list, list):
        raise ValueError("posts JSON must be a top-level list.")

    normalised = []
    for idx, item in enumerate(raw_list, start=1):
        if not isinstance(item, dict):
            continue
        entry = _normalise_entry(item, export_root)
        if entry is None:
            logging.warning(f"Skipping entry #{idx}: no usable media URIs.")
            continue
        normalised.append(entry)

    logging.info(f"Loaded {len(normalised)} valid post(s) from {posts_path}.")
    return normalised


def load_reposts(export_root: Path) -> list[dict[str, Any]]:
    reposts_path = Path(REPOSTS_FILE)
    if not reposts_path.exists():
        logging.info(f"Reposts file not found, skipping: {reposts_path}")
        return []

    with reposts_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    raw_items: list[dict[str, Any]]
    if isinstance(payload, dict):
        raw_items = [payload]
    elif isinstance(payload, list):
        raw_items = [x for x in payload if isinstance(x, dict)]
    else:
        return []

    reposts: list[dict[str, Any]] = []
    skipped_without_media = 0
    for raw in raw_items:
        media_items = raw.get("media", [])
        media_paths = [
            _resolve(m["uri"], export_root)
            for m in media_items
            if isinstance(m, dict) and m.get("uri")
        ]
        if not media_paths:
            skipped_without_media += 1
            continue

        media_type = "album" if len(media_paths) > 1 else "photo"
        if len(media_paths) == 1 and _ext(media_paths[0].name) in VIDEO_EXTS:
            media_type = "video"

        source_url = _extract_repost_url(raw)
        caption = "Repost restored from Meta backup"
        if source_url:
            caption = f"{caption}\n\nOriginal URL: {source_url}"

        reposts.append(
            {
                "caption": caption,
                "media_type": media_type,
                "media_paths": media_paths,
                "original_timestamp": raw.get("timestamp"),
            }
        )

    if skipped_without_media:
        logging.warning(
            "Skipped %s repost item(s) without media payload in export. "
            "Meta usually exports repost links without media, which cannot be re-uploaded automatically.",
            skipped_without_media,
        )
    logging.info("Loaded %s repost item(s) with media.", len(reposts))
    return reposts


def _load_json(path_str: str) -> Any:
    p = Path(path_str)
    if not p.exists():
        raise FileNotFoundError(f"JSON file not found: {p}")
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def _extract_first_list(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for value in payload.values():
            if isinstance(value, list):
                return value
    return []


def load_media_collection(file_path: str, export_root: Path) -> list[dict[str, Any]]:
    """Load Meta media collections (stories, archived, igtv) into post-like dicts."""
    payload = _load_json(file_path)
    entries = _extract_first_list(payload)

    normalised: list[dict[str, Any]] = []
    for item in entries:
        if not isinstance(item, dict):
            continue

        # Some files store an inner "media" list, others are direct media records.
        media_items = item.get("media") if isinstance(item.get("media"), list) else [item]
        if not media_items:
            continue

        resolved_paths: list[Path] = []
        for media in media_items:
            if isinstance(media, dict) and media.get("uri"):
                resolved_paths.append(_resolve(str(media["uri"]), export_root))

        if not resolved_paths:
            continue

        media_type = "album" if len(resolved_paths) > 1 else "photo"
        if len(resolved_paths) == 1 and _ext(resolved_paths[0].name) in VIDEO_EXTS:
            media_type = "video"

        title = ""
        if isinstance(item.get("title"), str):
            title = _repair_text(item["title"])
        if not title and isinstance(media_items[0], dict):
            title = _repair_text(str(media_items[0].get("title") or ""))

        ts = None
        if isinstance(media_items[0], dict):
            ts = media_items[0].get("creation_timestamp")

        normalised.append(
            {
                "caption": title,
                "media_type": media_type,
                "media_paths": resolved_paths,
                "original_timestamp": ts,
            }
        )

    return normalised


def load_following_usernames() -> list[str]:
    payload = _load_json(FOLLOWING_FILE)
    items = _extract_first_list(payload)
    usernames: list[str] = []
    for entry in items:
        if not isinstance(entry, dict):
            continue
        username = str(entry.get("title") or "").strip()
        if username:
            usernames.append(username)
    return usernames


def migrate_profile(client: Client, export_root: Path) -> None:
    profile_payload = _load_json(PROFILE_INFO_FILE)
    profile_rows = _extract_first_list(profile_payload)
    if profile_rows:
        profile = profile_rows[0] if isinstance(profile_rows[0], dict) else {}
        smd = profile.get("string_map_data", {}) if isinstance(profile, dict) else {}
        full_name = _repair_text(str((smd.get("Name") or {}).get("value") or ""))
        biography = _repair_text(str((smd.get("Bio") or {}).get("value") or ""))
        external_url = str((smd.get("Website") or {}).get("value") or "").strip()

        kwargs: dict[str, str] = {}
        if full_name:
            kwargs["full_name"] = full_name
        if biography:
            kwargs["biography"] = biography
        if external_url:
            kwargs["external_url"] = external_url

        if kwargs:
            client.account_edit(**kwargs)
            logging.info("Profile text fields updated: %s", ", ".join(kwargs.keys()))

    photos_payload = _load_json(PROFILE_PHOTOS_FILE)
    photo_items = _extract_first_list(photos_payload)
    if photo_items:
        latest = photo_items[-1] if isinstance(photo_items[-1], dict) else {}
        uri = latest.get("uri") if isinstance(latest, dict) else None
        if isinstance(uri, str) and uri:
            pic_path = _resolve(uri, export_root)
            if pic_path.exists():
                client.account_change_picture(str(pic_path))
                logging.info("Profile photo updated from export.")


def migrate_following(client: Client) -> None:
    usernames = load_following_usernames()
    if not usernames:
        logging.info("No following usernames to replay.")
        return

    max_count = min(FOLLOW_MAX_PER_RUN, len(usernames))
    followed = 0
    failed = 0
    for username in usernames[:max_count]:
        try:
            user_id = client.user_id_from_username(username)
            client.user_follow(user_id)
            followed += 1
        except Exception as e:
            failed += 1
            logging.warning("Follow failed for %s: %s", username, e)
        time.sleep(FOLLOW_SLEEP_SECONDS)

    logging.info(
        "Following replay complete: attempted=%s followed=%s failed=%s",
        max_count,
        followed,
        failed,
    )


def _load_comment_candidates() -> list[dict[str, Any]]:
    comments_path = Path(COMMENTS_FILE)
    if not comments_path.exists():
        logging.info(f"Comments file not found, skipping: {comments_path}")
        return []

    with comments_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    if isinstance(payload, dict):
        # Handle wrapper keys like comments_reels_comments
        first_list = next((v for v in payload.values() if isinstance(v, list)), [])
        items = first_list
    elif isinstance(payload, list):
        items = payload
    else:
        return []

    comments: list[dict[str, Any]] = []
    for entry in items:
        if not isinstance(entry, dict):
            continue
        smd = entry.get("string_map_data", {})
        if not isinstance(smd, dict):
            continue

        text = _repair_text((smd.get("Comment") or {}).get("value") or "")
        owner = ((smd.get("Media Owner") or {}).get("value") or "").strip()
        ts = (smd.get("Time") or {}).get("timestamp")

        if not text or not isinstance(ts, int):
            continue
        comments.append({"text": text, "media_owner": owner, "timestamp": ts})

    comments.sort(key=lambda c: c["timestamp"])
    return comments


def attach_best_effort_comments(posts: list[dict[str, Any]]) -> None:
    """
    Best-effort mapping:
    - Uses comments from post_comments_1.json.
    - Keeps only comments where Media Owner == USERNAME (comments on your media).
    - Assigns each comment to the nearest post by original timestamp within COMMENT_WINDOW_DAYS.
    """
    source_username = SOURCE_ACCOUNT_USERNAME or (USERNAME or "")
    if not source_username:
        return

    candidates = _load_comment_candidates()
    if not candidates:
        return

    own_media_comments = [
        c for c in candidates if c["media_owner"].lower() == source_username.lower()
    ]
    if not own_media_comments:
        logging.warning(
            "No comments found that reference '%s' as Media Owner.",
            source_username,
        )
        return

    posts_with_ts = [
        (i, p) for i, p in enumerate(posts)
        if isinstance(p.get("original_timestamp"), int)
    ]
    if not posts_with_ts:
        logging.warning("No post timestamps available for comment mapping.")
        return

    window = COMMENT_WINDOW_DAYS * 24 * 60 * 60
    attached = 0
    for comment in own_media_comments:
        ts = comment["timestamp"]
        best_idx = None
        best_delta = None
        for i, post in posts_with_ts:
            delta = abs(ts - post["original_timestamp"])
            if best_delta is None or delta < best_delta:
                best_delta = delta
                best_idx = i
        if best_idx is None or best_delta is None or best_delta > window:
            continue
        posts[best_idx].setdefault("restored_comments", []).append(comment["text"])
        attached += 1

    logging.info(
        "Attached %s comment(s) to posts using best-effort timestamp matching.",
        attached,
    )

# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------

def load_progress() -> int:
    """Return the index of the next post to upload (0-based)."""
    p = Path(PROGRESS_FILE)
    if p.exists():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            return int(data.get("next_index", 0))
        except Exception:
            pass
    return 0


def save_progress(next_index: int) -> None:
    Path(PROGRESS_FILE).write_text(
        json.dumps({"next_index": next_index, "updated": datetime.now().isoformat()},
                   indent=2),
        encoding="utf-8",
    )

# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

def validate_post(idx: int, post: dict[str, Any]) -> list[str]:
    """
    Check a normalised post dict for problems.
    Returns a list of human-readable issue strings (empty == all good).
    """
    issues: list[str] = []

    media_type  = post.get("media_type", "?")
    media_paths = post.get("media_paths", [])

    if not media_paths:
        issues.append("no media paths resolved")
        return issues                          # nothing else to check

    for path in media_paths:
        p = Path(path)
        if not p.exists():
            issues.append(f"file not found: {p}")
        elif p.stat().st_size == 0:
            issues.append(f"file is empty (0 bytes): {p}")
        else:
            ext = p.suffix.lower()
            if media_type == "video" and ext not in VIDEO_EXTS:
                issues.append(f"unexpected extension for video: {ext}")
            elif media_type == "photo" and ext not in IMAGE_EXTS:
                issues.append(f"unexpected extension for photo: {ext}")

    if not post.get("caption") and not post.get("caption") == "":
        issues.append("caption is missing (will post blank caption)")

    return issues


def dry_run_report(posts: list[dict[str, Any]]) -> None:
    """
    Log a full validation report for every post without uploading anything.
    Prints a summary table at the end.
    """
    daily_times   = build_daily_times(POSTS_PER_DAY, START_HOUR, END_HOUR)
    total         = len(posts)
    pass_count    = 0
    fail_count    = 0
    warning_count = 0

    logging.info("="*60)
    logging.info("DRY RUN — JSON validation report")
    logging.info(f"  Posts file  : {POSTS_FILE}")
    logging.info(f"  Export root : {ACTIVE_EXPORT_ROOT}")
    logging.info(f"  Total posts : {total}")
    logging.info(
        f"  Schedule    : {POSTS_PER_DAY}/day  "
        f"{START_HOUR:02d}:00 – {END_HOUR:02d}:00  "
        f"→ slots: {', '.join(daily_times[:5])}"
        + (f" … (+{len(daily_times)-5} more)" if len(daily_times) > 5 else "")
    )
    days_needed = -(-total // POSTS_PER_DAY)  # ceiling division
    logging.info(f"  Days to complete upload at current rate: {days_needed}")
    logging.info("="*60)

    for idx, post in enumerate(posts, start=1):
        issues   = validate_post(idx, post)
        day_num  = ((idx - 1) // POSTS_PER_DAY) + 1
        slot_idx = (idx - 1) % POSTS_PER_DAY
        slot     = daily_times[slot_idx] if slot_idx < len(daily_times) else "?"

        caption_preview = (post.get("caption") or "")[:60].replace("\n", " ")
        media_type      = post.get("media_type", "?")
        paths           = post.get("media_paths", [])
        file_count      = len(paths)

        if issues:
            fail_count += 1
            logging.error(
                f"[FAIL] #{idx:>4}  day={day_num}  slot={slot}  "
                f"type={media_type}  files={file_count}  "
                f"caption='{caption_preview}'"
            )
            for issue in issues:
                logging.error(f"         ↳ {issue}")
        else:
            pass_count += 1
            # Warn about blank captions even on otherwise-valid posts
            blank_caption = not (post.get("caption") or "").strip()
            if blank_caption:
                warning_count += 1
                logging.warning(
                    f"[WARN] #{idx:>4}  day={day_num}  slot={slot}  "
                    f"type={media_type}  files={file_count}  "
                    f"(no caption)"
                )
            else:
                logging.info(
                    f"[OK]   #{idx:>4}  day={day_num}  slot={slot}  "
                    f"type={media_type}  files={file_count}  "
                    f"caption='{caption_preview}'"
                )

    logging.info("="*60)
    logging.info(
        f"SUMMARY  total={total}  "
        f"ok={pass_count}  warn={warning_count}  fail={fail_count}"
    )
    if fail_count == 0:
        logging.info("All posts look good — ready for a real run.")
    else:
        logging.warning(
            f"{fail_count} post(s) have errors. Fix missing files before running for real."
        )
    logging.info("="*60)


def upload_post(client: Client, post: dict[str, Any]) -> None:
    caption      = post["caption"]
    media_type   = post["media_type"]
    media_paths  = [str(p) for p in post["media_paths"]]

    if media_type == "photo":
        media = client.photo_upload(media_paths[0], caption)
    elif media_type == "video":
        media = client.video_upload(media_paths[0], caption)
    elif media_type == "album":
        media = client.album_upload(media_paths, caption)
    else:
        raise ValueError(f"Unknown media_type '{media_type}'.")

    logging.info(f"Uploaded [{media_type}] → media pk {media.pk}")

    # Best effort: if Instagram sets uploaded media as hidden/off-grid, try to make it visible.
    try:
        raw_info = client.private_request(f"media/{media.pk}/info/")
        item = (raw_info.get("items") or [{}])[0]
        visibility = item.get("visibility")
        in_grid = item.get("is_in_profile_grid")

        if visibility == "only_me" or in_grid is False:
            logging.warning(
                "Uploaded media %s hidden (visibility=%s, is_in_profile_grid=%s). Trying to make visible...",
                media.pk,
                visibility,
                in_grid,
            )

            # Step 1: undo "only me" when present.
            try:
                client.media_unarchive(str(media.pk))
            except Exception as unarchive_err:
                logging.warning("Undo-only-me failed for media %s: %s", media.pk, unarchive_err)

            # Step 2: nudge visibility/profile-grid flags via edit endpoint.
            try:
                client.private_request(
                    f"media/{media.pk}/edit_media/",
                    data={
                        "caption_text": caption,
                        "show_in_feed": "1",
                        "is_in_profile_grid": "1",
                        "_uid": str(client.user_id),
                        "_uuid": client.uuid,
                        "device_id": client.android_device_id,
                    },
                )
            except Exception as edit_err:
                logging.warning("Visibility edit failed for media %s: %s", media.pk, edit_err)

            # Re-check state and continue either way (do not block progress).
            raw_info_2 = client.private_request(f"media/{media.pk}/info/")
            item_2 = (raw_info_2.get("items") or [{}])[0]
            visibility_2 = item_2.get("visibility")
            in_grid_2 = item_2.get("is_in_profile_grid")
            if visibility_2 == "only_me" or in_grid_2 is False:
                logging.warning(
                    "Media %s still hidden after remediation (visibility=%s, is_in_profile_grid=%s).",
                    media.pk,
                    visibility_2,
                    in_grid_2,
                )
            else:
                logging.info("Media %s is now visible in profile/public context.", media.pk)
    except Exception as visibility_err:
        logging.warning("Could not verify or remediate visibility for media %s: %s", media.pk, visibility_err)

    if REPLAY_COMMENTS:
        restored_comments = post.get("restored_comments", [])
        for comment in restored_comments:
            try:
                client.media_comment(media.pk, comment)
            except Exception as e:
                logging.warning(f"Failed to replay comment on {media.pk}: {e}")
        if restored_comments:
            logging.info(f"Replayed {len(restored_comments)} comment(s) on media {media.pk}")


def upload_story(client: Client, post: dict[str, Any]) -> None:
    """Upload one normalised media item as story content."""
    media_type = post["media_type"]
    media_paths = [str(p) for p in post["media_paths"]]

    if media_type == "video":
        client.video_upload_to_story(media_paths[0])
        logging.info("Uploaded story video: %s", media_paths[0])
        return

    # Stories cannot be uploaded as albums in one API call; push each frame/item.
    for path in media_paths:
        client.photo_upload_to_story(path)
        logging.info("Uploaded story photo: %s", path)


def _run_feature(name: str, enabled: bool, fn):
    if not enabled:
        logging.info("Feature disabled: %s", name)
        return None
    try:
        return fn()
    except Exception as e:
        logging.error("Feature '%s' failed: %s", name, e)
        if STOP_ON_FEATURE_ERROR:
            raise
        return None


def _resolve_export_root_runtime() -> Path:
    global ACTIVE_EXPORT_ROOT

    if EXPORT_ROOT:
        ACTIVE_EXPORT_ROOT = EXPORT_ROOT
        return EXPORT_ROOT

    posts_path = Path(POSTS_FILE)
    if posts_path.exists():
        ACTIVE_EXPORT_ROOT = _infer_export_root(posts_path)
        return ACTIVE_EXPORT_ROOT

    ACTIVE_EXPORT_ROOT = ACCOUNT_EXPORT_DIR
    return ACCOUNT_EXPORT_DIR


def run_auth_check() -> int:
    """Run isolated authentication diagnostics with no migration side effects."""
    setup_logging()

    if not USERNAME:
        logging.error("AUTH_CHECK: INSTAGRAM_USERNAME is missing.")
        return 2
    if not PASSWORD:
        logging.error("AUTH_CHECK: INSTAGRAM_PASSWORD is missing.")
        return 2

    logging.info("AUTH_CHECK: starting for username '%s'", USERNAME)

    # Public web profile endpoint test (does not require private API login)
    web_ok = False
    web_url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={USERNAME}"
    try:
        response = requests.get(
            web_url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "X-IG-App-ID": "936619743392459",
            },
            timeout=20,
        )
        web_ok = response.status_code == 200
        logging.info("AUTH_CHECK: web endpoint status=%s", response.status_code)
    except Exception as e:
        logging.warning("AUTH_CHECK: web endpoint request failed: %s", e)

    # Private API login test (required for migration)
    client = Client()
    client.logger.setLevel("INFO")
    try:
        client.login(USERNAME, PASSWORD)
        logging.info("AUTH_CHECK: private API login PASS")
        if web_ok:
            logging.info("AUTH_CHECK: overall PASS (web + private API)")
        else:
            logging.info("AUTH_CHECK: overall PASS (private API), web endpoint had issues")
        return 0
    except Exception as e:
        logging.error("AUTH_CHECK: private API login FAIL")
        logging.error("AUTH_CHECK: exception_type=%s", type(e).__name__)
        logging.error("AUTH_CHECK: exception_message=%s", e)

        if hasattr(client, "last_response") and client.last_response is not None:
            logging.error(
                "AUTH_CHECK: private_api_status=%s",
                getattr(client.last_response, "status_code", None),
            )

        if hasattr(client, "last_json") and client.last_json:
            try:
                short_json = json.dumps(client.last_json)[:1200]
            except Exception:
                short_json = str(client.last_json)[:1200]
            logging.error("AUTH_CHECK: private_api_json=%s", short_json)

        if web_ok:
            logging.error(
                "AUTH_CHECK: web endpoint reachable but private API login failed. "
                "This is usually credentials/challenge/risk policy."
            )
        return 1

# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

def build_daily_times(posts_per_day: int, start_hour: int, end_hour: int) -> list[str]:
    """
    Return `posts_per_day` evenly-spaced HH:MM strings within [start_hour, end_hour).
    Example: 25 posts, 08:00–22:00 → one post every ~33 min.
    """
    window_minutes = (end_hour - start_hour) * 60
    if posts_per_day == 1:
        offsets = [0]
    else:
        step = window_minutes / posts_per_day
        offsets = [int(i * step) for i in range(posts_per_day)]

    times = []
    for offset in offsets:
        total = start_hour * 60 + offset
        hh, mm = divmod(total, 60)
        times.append(f"{hh:02d}:{mm:02d}")
    return times


def run_scheduler() -> None:
    setup_logging()

    client = login_user()
    if not client:
        return

    export_root = _resolve_export_root_runtime()
    logging.info(f"Using export root: {export_root}")

    scheduled_posts: list[dict[str, Any]] = []

    def _load_feed_posts() -> list[dict[str, Any]]:
        return load_posts(POSTS_FILE)

    def _load_reposts() -> list[dict[str, Any]]:
        return load_reposts(export_root)

    def _load_archived_posts() -> list[dict[str, Any]]:
        items = load_media_collection(ARCHIVED_POSTS_FILE, export_root)
        logging.info("Loaded %s archived post(s).", len(items))
        return items

    def _load_igtv_posts() -> list[dict[str, Any]]:
        items = load_media_collection(IGTV_FILE, export_root)
        logging.info("Loaded %s IGTV item(s).", len(items))
        return items

    for feature_name, enabled, loader in [
        ("feed_posts", MIGRATE_FEED_POSTS, _load_feed_posts),
        ("reposts", MIGRATE_REPOSTS, _load_reposts),
        ("archived_posts", MIGRATE_ARCHIVED_POSTS, _load_archived_posts),
        ("igtv", MIGRATE_IGTV, _load_igtv_posts),
    ]:
        items = _run_feature(feature_name, enabled, loader)
        if items:
            scheduled_posts.extend(items)

    if REPLAY_COMMENTS:
        _run_feature("comment_replay_mapping", True, lambda: attach_best_effort_comments(scheduled_posts))

    def _run_profile() -> None:
        migrate_profile(client, export_root)

    def _run_stories() -> None:
        stories = load_media_collection(STORIES_FILE, export_root)
        logging.info("Loaded %s story item(s).", len(stories))
        for story in stories:
            upload_story(client, story)

    def _run_following() -> None:
        migrate_following(client)

    _run_feature("profile", MIGRATE_PROFILE, _run_profile)
    _run_feature("stories", MIGRATE_STORIES, _run_stories)
    _run_feature("following", MIGRATE_FOLLOWING, _run_following)

    if not scheduled_posts:
        logging.warning("No scheduled feed/repost/archive/igtv posts to upload.")
        return

    next_index = load_progress()
    remaining  = scheduled_posts[next_index:]

    if not remaining:
        logging.info("All posts have already been uploaded. Nothing to do.")
        return

    logging.info(
        f"{len(remaining)} post(s) remaining "
        f"(starting at index {next_index} of {len(scheduled_posts)})."
    )

    daily_times = build_daily_times(POSTS_PER_DAY, START_HOUR, END_HOUR)
    logging.info(
        f"Will post {POSTS_PER_DAY} time(s) per day between "
        f"{START_HOUR:02d}:00 and {END_HOUR:02d}:00."
    )

    # Mutable cursor shared across scheduled jobs
    state = {"next_index": next_index, "last_date": date.today()}

    def post_next() -> schedule.CancelJob | None:
        # If all done, cancel
        if state["next_index"] >= len(scheduled_posts):
            logging.info("All posts uploaded — stopping scheduler.")
            return schedule.CancelJob

        post = scheduled_posts[state["next_index"]]
        try:
            upload_post(client, post)
            state["next_index"] += 1
            save_progress(state["next_index"])
        except Exception as e:
            logging.error(f"Upload failed for post #{state['next_index']}: {e}")
            # Don't advance; will retry at the next slot

    # Schedule the same time slots every day
    for t in daily_times:
        schedule.every().day.at(t).do(post_next)

    logging.info("Scheduler running. Press Ctrl-C to stop (progress is saved).")
    try:
        while state["next_index"] < len(scheduled_posts):
            schedule.run_pending()
            time.sleep(10)
    except KeyboardInterrupt:
        logging.info(f"Interrupted. Progress saved at index {state['next_index']}.")


if __name__ == "__main__":
    auth_check = AUTH_CHECK or "--auth-check" in sys.argv
    dry = DRY_RUN or "--dry-run" in sys.argv
    if auth_check:
        sys.exit(run_auth_check())
    elif dry:
        setup_logging()
        export_root = _resolve_export_root_runtime()
        logging.info(f"Using export root: {export_root}")

        all_posts: list[dict[str, Any]] = []
        for feature_name, enabled, loader in [
            ("feed_posts", MIGRATE_FEED_POSTS, lambda: load_posts(POSTS_FILE)),
            ("reposts", MIGRATE_REPOSTS, lambda: load_reposts(export_root)),
            ("archived_posts", MIGRATE_ARCHIVED_POSTS, lambda: load_media_collection(ARCHIVED_POSTS_FILE, export_root)),
            ("igtv", MIGRATE_IGTV, lambda: load_media_collection(IGTV_FILE, export_root)),
        ]:
            items = _run_feature(feature_name, enabled, loader)
            if items:
                all_posts.extend(items)

        if REPLAY_COMMENTS:
            _run_feature("comment_replay_mapping", True, lambda: attach_best_effort_comments(all_posts))

        _run_feature(
            "stories_dry_count",
            MIGRATE_STORIES,
            lambda: logging.info("Dry run: %s story item(s) available.", len(load_media_collection(STORIES_FILE, export_root))),
        )
        _run_feature(
            "profile_dry_check",
            MIGRATE_PROFILE,
            lambda: logging.info("Dry run: profile files found and will be applied in live mode."),
        )
        _run_feature(
            "following_dry_count",
            MIGRATE_FOLLOWING,
            lambda: logging.info(
                "Dry run: %s following username(s) available, max per live run=%s.",
                len(load_following_usernames()),
                FOLLOW_MAX_PER_RUN,
            ),
        )

        if not all_posts:
            logging.warning("Dry run: no scheduled feed/repost/archive/igtv posts enabled.")
        else:
            dry_run_report(all_posts)
    else:
        run_scheduler()