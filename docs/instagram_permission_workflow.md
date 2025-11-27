# Instagram Permission Implementation Notes

This document captures the current Instagram integration status for the HimanIstanbul helper app, the user journeys that justify each requested Meta permission, and the backend/frontend work required to support those journeys and the compliance package.

---

## 1. Current Messaging Integration (Audit)

- **Webhook ingestion** – `app/routers/instagram.py` handles subscription verification, signature validation, raw payload storage, and queueing for ingestion. This already exercises `instagram_business_manage_messages` for real customer DMs.
- **Graph access layer** – `app/services/instagram_api.py` owns token resolution, conversation fetching, message hydration, and attachment streaming. All conversation sync jobs go through this module, ensuring we respect Graph platform selection and rate limits.
- **Inbox router/UI** – `app/routers/thread_handlers.py` renders `ig_thread.html`, merges Graph conversation IDs with internal conversation rows, exposes AI suggestions, and links customer context (orders, inventory, shipping). The inbox already displays Instagram conversations inside the app.
- **AI reply worker** – `scripts/worker_reply.py` pulls `ai_shadow_state`, drafts replies via `app/services/ai_reply.py`, and sends outbound DMs with `instagram_api.send_message`. It enforces debounce windows, product-level toggles, and audit logging.
- **Ingestion service** – `app/services/ingest.py` upserts conversations/messages coming from webhooks or manual sync, links attachments, and auto-detects product mentions. Functions like `_auto_link_instagram_post` tie DMs back to catalog items for fulfillment.
- **Manual AI disengage marker** – The same ingestion flow now watches outbound agent messages for a `...` marker (or ellipsis). When present, it creates an admin escalation, pauses `ai_shadow_state`, and ensures automation stays disabled until re-enabled inside the UI. This keeps Meta reviewers confident that humans can instantly override AI even when replying directly from the Instagram app.

These components prove we already have working infrastructure for `instagram_business_manage_messages`. However, additional scopes require new flows (content calendar, insights, comment moderation, basic profile surfacing) that are not yet implemented.

---

## 2. End-To-End User Journeys Mapped to Permissions

### Personas
1. **Sales Associate (DM agent):** Answers DMs, sends product links, schedules follow-ups.
2. **Content Manager:** Schedules reels/posts, reuses product assets, needs publishing confirmation.
3. **Operations Manager:** Reviews funnel metrics, product-level insights, and service-level KPIs.
4. **Community Manager:** Triages public comments, hides spam, escalates escalations into DMs.

### `instagram_business_basic`
- **Allowed usage reference:** Provides access to basic profile fields, connected IG Business account metadata, and catalog linkage so long as surfaced inside the experience.
- **How we will use it:**
  - Display verified IG username, profile picture, follower count, and bio inside the Inbox header so agents confirm they are responding on behalf of the correct business.
  - Surface attribution of catalog items to IG posts when linking orders, ensuring shipping staff see which IG campaign triggered the sale.
- **Key UI:** “Brand Snapshot” sidebar showing profile stats, last synced timestamp, and quick links.
- **Compliance:** Only business admins can view the data, sync runs hourly, and retention mirrors existing account metadata retention policies.

### `instagram_business_manage_messages`
- **Current usage:** Full inbox rendering, AI-assisted replies, webhook ingestion, and manual sync already rely on this scope.
- **Enhancements to justify re-review:**
  - Add agent assignment, templates, and canned responses stored per conversation.
  - Provide a “DM to Order” wizard that pre-fills order drafts with DM information to demonstrate a complete commerce workflow.
- **Data handling:** Store only messages necessary for operations, allow deletion upon request, and maintain audit logs inside `ai_shadow_state`.

### `instagram_business_content_publish`
- **Planned usage:**
  - Build a Content Calendar module where merchants create posts/reels referencing inventory SKUs. Assets upload through the existing ingestion pipeline and scheduled jobs call the Graph publish endpoint via a new `content_publish` worker.
  - Publishing flow: user drafts → selects media from catalog → optional captions/templates → selects publish time → worker executes at schedule and logs result.
- **Compliance:** Only authorized staff can publish, drafts keep local copies, and failed publishes notify via inbox system.

### `instagram_business_manage_insights`
- **Planned usage:**
  - Pull reach, impressions, engagement, and story interactions per media/post to show ROI and reorder priority.
  - Correlate insights with inventory and order data, highlighting top-performing SKUs inside the dashboard.
- **Compliance:** Cache metrics with TTL (24h), expose export/download, and allow manual refresh with rate limiting.

### `instagram_business_manage_comments`
- **Planned usage:**
  - Central comment moderation board listing comments for latest posts, with actions to reply, hide, delete, or convert into a DM thread.
  - Provide spam detection tags and assignment to sales associates for follow-up.
- **Compliance:** Respect user privacy by showing only comments on business-owned media, audit each moderation action, and reflect changes directly in Graph to avoid desync.

---

## 3. Backend Enhancements

| Area | Work Items | Files / Services |
| --- | --- | --- |
| Messaging audit trail | Expand message ingestion to tag assignment + template usage | [`app/services/ingest.py`](../app/services/ingest.py), [`app/routers/thread_handlers.py`](../app/routers/thread_handlers.py) |
| Comment APIs | New router `app/routers/ig_comments.py` exposing list/hide/reply endpoints. Service helpers in `app/services/instagram_api.py` for `/comments`, `/replies`, `/media`. Persist moderation logs. | `app/routers`, `app/services/instagram_api.py`, new table `comment_actions`. |
| Publishing | `app/services/content_publish.py` to build creative payloads, integrate with `scripts/worker_reply.py`’s queue system or a new `scripts/worker_publish.py`. Reuse asset ingestion for media uploads (IG container creation + publish). | `scripts/worker_publish.py`, `app/services/content_publish.py`, `app/routers/content_calendar.py`. |
| Insights | `app/services/ig_insights.py` using `/{media_id}/insights` and `/{ig_user}/insights`. Cache in Redis/DB, expose via `app/routers/ig_insights.py`. | `app/services/ig_insights.py`, `app/routers/ig_insights.py`, DB table `ig_insights_cache`. |
| Basic profile sync | Extend existing `_get_base_token_and_id` usage with scheduled fetch of `/me?fields=username,profile_picture_url,followers_count`. Persist in `system_settings` for UI display. | `app/services/instagram_api.py`, `scripts/worker_ingest.py`. |

---

## 4. Frontend / UX Updates

- **Inbox Enhancements:** Update `templates/ig_thread.html` to show Brand Snapshot (profile stats), agent assignment dropdown, canned responses, and DM-to-Order wizard. Inline comment referencing DM threads with linked orders.
- **Content Calendar UI:** New page (e.g., `templates/ig_calendar.html`) listing scheduled posts, drag-and-drop slotting, preview modal with caption + media. Form posts to `/ig/content/drafts`.
- **Insights Dashboard:** Charts for reach/impressions by SKU, story performance over time, and CTA conversions. Use existing frontend framework (Jinja + HTMX) to render aggregated data.
- **Comment Moderation Board:** Table view with filters (unread, spam, replied), quick actions (reply, hide, escalate to DM). Integrate with backend comment endpoints.
- **Screencast readiness:** Ensure each UI path is clickable from main nav so the Meta screencast can demonstrate login → profile snapshot → DM reply (messages scope) → schedule post (content_publish) → view insights → moderate comments.

---

## 5. Compliance & Screencast Package

- **Policy alignment notes:**
  - Store only business-owned data, purge upon deauthorization.
  - Show in-app toggles to disable automated sends (already supported globally/product-level in AI worker).
  - Limit insights caching to 24h, include “Refresh from Meta” CTA with timestamp.
  - Provide access logs for comment moderation and publishing actions.
- **Screencast outline:**
  1. Login as HimanIstanbul admin.
  2. Show Brand Snapshot (basic scope).
  3. Demonstrate DM reply with template + AI suggestion (manage_messages).
  4. Create scheduled post referencing a SKU and show pending queue (content_publish).
  5. Navigate to Insights dashboard, filter by SKU, export CSV (manage_insights).
  6. Moderate a comment, convert another into DM follow-up (manage_comments).
- **Custom Q&A responses:** Document per-scope answers describing the flows above, referencing exact UI screens and worker names for credibility.

---

## Next Steps Checklist
1. Implement comment router/service, moderation UI, and audit tables.
2. Build content calendar + publishing worker with asset reuse.
3. Add insights service + dashboard with caching and exports.
4. Surface profile basics and catalog linkage in Inbox.
5. Draft Meta review responses & record screencast following the outline.

This structured rollout ensures every requested permission is justified with a concrete, demonstrable workflow inside the HimanIstanbul helper app.

