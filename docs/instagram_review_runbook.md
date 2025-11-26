# Instagram Permission Review Runbook

This runbook documents the QA steps and screencast script for demonstrating every Meta permission the app requests. Use it before submissions to ensure each UI workflow behaves as expected.

---

## 1. Pre-flight Checklist
- [ ] Environment variables (`IG_PAGE_ACCESS_TOKEN`, `IG_PAGE_ID`, `IG_USER_ID`) populated.
- [ ] `worker_reply`, `worker_ingest`, and new `worker_publish` processes running.
- [ ] Sample media IDs available for comment moderation tests.
- [ ] Brand profile cache primed via `/ig/inbox/brand-profile`.

---

## 2. QA Scenarios

### 2.1 Inbox & Brand Snapshot (`instagram_business_basic`, `instagram_business_manage_messages`)
1. Navigate to `/ig/inbox/{conversation_id}`.
2. Verify **Brand Snapshot** card shows username, follower counts, and refresh button.
3. Change assignee via dropdown and confirm toast updates.
4. Insert canned response into reply box; ensure DM→Order wizard opens, pre-fills contact, and saves a draft.
5. Send a reply and confirm appearance in message list.

### 2.2 Content Calendar & Publishing (`instagram_business_content_publish`)
1. Visit `/ig/content/calendar`.
2. Create a draft with caption + media URL + scheduled time.
3. Confirm entry appears in queue table.
4. Click “Şimdi Yayınla” to trigger immediate publish and check `worker_publish` logs / audit table.

### 2.3 Insights Dashboard (`instagram_business_manage_insights`)
1. Open `/ig/insights/dashboard`.
2. Tap “Yenile” to fetch latest metrics; ensure KPI cards update and chart renders.
3. For manual verification, hit `/ig/insights/overview` directly and confirm JSON contains follower metrics.

### 2.4 Comment Moderation (`instagram_business_manage_comments`)
1. Open `/ig/comments/moderation`.
2. Enter a media_id and load comments.
3. Perform reply, hide/unhide, delete, and “DM’e Dönüştür” actions.
4. Open Audit log table and confirm entries were recorded in chronological order.

---

## 3. Screencast Script
1. **Intro & Brand Snapshot** – Show login, open DM thread, highlight profile card, assignment dropdown, canned responses, DM→Order wizard, and send a reply.
2. **Content Calendar** – Navigate to `/ig/content/calendar`, create a draft, point to schedule queue, force publish one item.
3. **Insights Dashboard** – Navigate to `/ig/insights/dashboard`, refresh metrics, describe chart and KPI tiles.
4. **Comment Moderation** – Load comments for a sample media ID, perform hide/reply/delete, and highlight audit log updates.
5. **Closing** – Mention worker logs + data retention controls.

Use clean browser tabs and keep narration aligned with Meta’s questionnaire questions.

---

## 4. Troubleshooting Tips
- **Profile cache stale:** call `/ig/inbox/brand-profile?force=1`.
- **Content publish errors:** check `ig_publishing_audit` table or `worker_publish` logs.
- **Insights fetch 403:** confirm requested metrics are available for the IG account and the access token has `instagram_business_manage_insights`.
- **Comment fetch empty:** ensure media_id belongs to the connected business account and has public comments.

---

Maintaining this document alongside the new UI ensures reviewers and teammates can consistently demonstrate compliant usage of every requested permission.

