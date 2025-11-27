# Instagram UI & Functionality Plan for Meta Permissions

This document extends the previously captured workflow summary by focusing specifically on the UI surfaces and interactive flows we need to showcase when requesting additional Instagram Business permissions.

---

## 1. Current UI Inventory & Missing Modules

| Existing Surface | Location / Notes | Gap vs. Permission Goals |
| --- | --- | --- |
| DM Inbox & Thread View | `templates/ig_inbox.html`, `templates/ig_thread.html`, router logic in `app/routers/thread_handlers.py` | Covers `instagram_business_manage_messages` baseline (read/reply). Missing visual brand snapshot, assignment controls, and DM→Order actions that make commerce tie-in explicit. |
| AI Reply Worker UI Hooks | Inline AI suggestion badges rendered alongside messages | Demonstrates automation but not policy-compliant audit (need explicit opt-in toggles + usage log view). |
| Inventory / Order Panels | Non-IG pages that track fulfillment | Not currently linked from IG inbox; need cross-navigation to justify commerce workflows triggered from DMs. |
| Global Navigation | `templates/_nav.html` includes Instagram entry but no calendar/insights/moderation tabs | Need new nav items so reviewers can click into each scope-specific module. |

**Missing Modules to Build**
1. Brand Snapshot panel (basic profile data) embedded in inbox.
2. Messaging enhancements: agent assignment, canned responses, DM→Order wizard.
3. Content Calendar + Scheduler (posts/reels) with inline previews.
4. Insights dashboard with charts and exports.
5. Comment moderation board with reply/hide/escalate actions.
6. Usage log modal covering automation + publishing history for compliance.

---

## 2. Feature Modules per Permission

### `instagram_business_basic`
- **UI:** Brand Snapshot card in the thread view header showing profile photo, follower count, bio snippet, verification badge, last-sync timestamp, and CTA to “Refresh from Instagram”.
- **Data Sources:** Scheduled profile sync stored in `system_settings`; fallback to live fetch via `instagram_api`.
- **Reviewer Story:** Proves we only show business-owned profile metadata to authenticated staff.

### `instagram_business_manage_messages`
- **UI Enhancements:**  
  - Agent assignment pill + dropdown.  
  - Canned response sidebar with searchable templates.  
  - DM→Order wizard (modal) that pre-fills customer info and product suggestions before pushing into existing order creation flow.  
  - Message action tray (mark resolved, escalate, mute) including a one-click "Yöneticiye Eskale Et" button that posts to `/ig/inbox/{conversation_id}/escalate` to pause AI and alert human admins.
- **Evidence:** Shows deeper operational tooling around messages rather than raw API access.

### `instagram_business_content_publish`
- **UI:** Content Calendar page with monthly/weekly views, cards representing drafts, and a “Create Post/Reel” modal. Includes media picker (ingesting catalog assets), caption editor with hashtag suggestions, scheduling controls, and publish status timeline.
- **Supporting Elements:** Badge showing required scope and a log panel (left sidebar) listing recent publishes with success/error states.

### `instagram_business_manage_insights`
- **UI:** Insights dashboard with tabs for Overview, Content, Audience. Widgets: KPI tiles (reach, impressions, CTR), line chart for engagement vs. time, table of top-performing posts by SKU, and export/download button.
- **Filters:** Date range picker, media type, SKU filter integrated with inventory data.

### `instagram_business_manage_comments`
- **UI:** Comment moderation board listing recent comments per post with user avatars, text, sentiment tag, and quick actions (reply inline, hide/unhide, delete, convert to DM). Include bulk selection + assign to agents.
- **Context:** Side panel shows original media thumbnail and past interactions.

---

## 3. Detailed UI Flows & Components

1. **Brand Snapshot in Inbox**
   - Component: `BrandSnapshotCard` (Jinja partial).  
   - Flow: On load, fetch `/ig/profile/basic` → render stats, enable “Refresh” button hitting same endpoint with `force=true`.  
   - Visual cues: Last synced timestamp, spinner during refresh, error toast if fetch fails.

2. **Messaging Enhancements**
   - Agent assignment dropdown binds to `/ig/inbox/{conversation_id}/assign`.  
   - Canned responses drawer queries `/ig/templates?conversation_id=` and inserts selected text into reply box.  
   - DM→Order wizard: Stepper modal (Customer → Cart → Review). On submit, posts to `/orders/from-dm`.

3. **Content Calendar**
   - Calendar grid built with lightweight JS (e.g., FullCalendar or custom HTMX).  
   - “Create Post” button opens modal with form fields (media picker connecting to asset library, caption, hashtags, scheduled time).  
   - Drafts persist via `/ig/content/drafts`. Publish queue displayed in sidebar with statuses streamed from worker channel.

4. **Insights Dashboard**
   - Use charting lib (Chart.js or Plotly) embedded via CDN.  
   - Each widget fetches from `/ig/insights/...` endpoints with caching.  
   - Export button triggers CSV download for selected filters.

5. **Comment Moderation Board**
   - Table list with infinite scroll/hard pagination.  
   - Row actions call `/ig/comments/{comment_id}/{action}` (reply/hide/delete).  
   - “Convert to DM” triggers backend linking comment author to DM conversation and opens inbox tab.

6. **Usage Logs**
   - Modal accessible from each module showing last publishing actions, comment moderation entries, and automation toggles.  
   - Data served from `/ig/audit/logs`.

---

## 4. Backend Hooks & Data Strategy

| Module | API / Worker Needed | Notes |
| --- | --- | --- |
| Brand Snapshot | `GET /ig/profile/basic` queuing `instagram_api.fetch_user_username` + `/me` fields; background cron updates cache. | Stores in `system_settings`. |
| Messaging Enhancements | `POST /ig/inbox/{id}/assign`, `GET/POST /ig/templates`, `POST /orders/from-dm`. | Reuse `thread_handlers` router; ensure audit logging. |
| Admin Escalations | `POST /ig/inbox/{conversation_id}/escalate` creates `admin_messages`, halts AI automation, and surfaces alerts in inbox lists. | Shares persistence + notification helpers with AI worker to keep parity between manual and automatic escalations. |
| Content Calendar | `POST /ig/content/drafts`, `GET /ig/content/schedule`, worker `scripts/worker_publish.py` calling Graph publish endpoints. | Draft media stored in S3/local, scheduler updates statuses. |
| Insights Dashboard | `GET /ig/insights/overview`, `/content`, `/audience`; background job caches metrics (Redis/DB). | TTL 24h, manual refresh triggers Graph call. |
| Comment Moderation | `GET /ig/comments`, `POST /ig/comments/{id}/reply`, `/hide`, `/delete`, `/convert-to-dm`. | Requires new service functions + audit trail table. |
| Usage Logs | `GET /ig/audit/logs` summarizing actions from publish worker + comment actions + AI replies. | Enables compliance narrative. |

Mock data: Provide seeded rows for schedule, insights, and comments to ensure demo works even if real Graph calls limited.

---

## 5. Screencast / Reviewer Storyboard

1. **Login & Navigate** – Show Instagram nav with new tabs (Inbox, Calendar, Insights, Comments).
2. **Brand Snapshot + Messaging** – Open a DM thread, highlight Brand Snapshot card, assign agent, use canned response, launch DM→Order wizard, send reply (demonstrates basic + manage_messages).
3. **Content Publishing** – Click “Content Calendar”, create a scheduled reel referencing inventory, show it appearing in queue with upcoming publish time (content_publish).
4. **Insights Review** – Switch to “Insights” tab, adjust date range, point to KPI tiles and export button (manage_insights).
5. **Comment Moderation** – Open “Comments” board, hide a spam comment, reply to another, convert a third to DM (manage_comments).
6. **Audit Modal & Compliance** – Open usage log modal showing recorded actions, reiterating data retention + refresh controls.

This sequence ensures every requested permission has an observable UI action tied to it for Meta reviewers.

