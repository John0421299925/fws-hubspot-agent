"""
FWS Agent 2 — HubSpot Inbox Agent (consolidated single-file version)
Version: v1.12

Everything Agent 2 needs is in this one file, deliberately, so it's easy
to copy-paste into a single GitHub file rather than managing many small
files and folders. This is the file Vercel runs.

Version history:
  v1.0 - Initial deploy. Guessed subscriptionType 'conversation.creation'
         (wrong) and invoice properties 'client_name'/'vendor_name' (wrong).
  v1.1 - Fixed event routing: both Communication and Invoice events arrive
         as subscriptionType 'object.creation', distinguished by
         objectTypeId ('0-18' for Communication, '0-53' for Invoice) —
         confirmed against real HubSpot sample payloads.
  v1.2 - Fixed get_invoice_details(): client company now fetched via the
         real v4 association (not a guessed 'client_name' property);
         vendor name parsed from hs_title's real format
         "{Client} - {Supplier} - {Date}" as a best-effort fallback since
         vendor isn't associated to the invoice in clv-invoice-automation
         yet. Fixes the 400 Bad Request from searching companies with an
         empty name value.
  v1.3 - Removed hs_pipeline/hs_pipeline_stage from ticket creation
         (were sending display labels, not valid IDs) — turned out these
         fields are actually REQUIRED, so this just traded one 400 error
         for another. Superseded by v1.4.
  v1.4 - Fixed properly: fetches real pipeline/stage IDs at runtime via
         /crm/v3/pipelines/tickets, matching TICKET_PIPELINE/
         TICKET_STATUS_NEW by label (falls back to the account's first
         pipeline/stage if no label match). Cached for 1 hour, same
         pattern as owner-name resolution.
  v1.5 - No more errors, but client_company_id and vendor_name_hint came
         back empty on a real invoice. Two real fixes:
         (a) client company association: added retry with short backoff,
         since the invoice-created webhook fires before
         clv-invoice-automation finishes its line-items loop and THEN
         associates the client company — a genuine race condition, not a
         wrong query.
         (b) vendor name: disabled the title-parsing approach entirely.
         It relied on splitting "{Client} - {Supplier} - {Date}", but a
         real client name ("Campus Living Villages - Sydney University")
         itself contains " - ", which broke the split and could have
         silently attached the file to the wrong company. Left blank
         until there's a reliable source (ideally clv-invoice-automation
         associating the vendor company directly, like it does for
         client).
  v1.6 - v1.5's retry window (~12s) still wasn't long enough — the
         original webhook's real timing (line items + association) takes
         30-40+ seconds for larger invoices. Extended to 7 attempts x 5s
         (~30s max wait). Also added maxDuration: 60 to vercel.json, since
         Vercel would otherwise kill the function before the retries
         could finish.
  v1.7 - Found the real root cause of Communication events never firing:
         HubSpot's own docs confirm generic/expanded-object webhooks do
         NOT support conversation.* events — they require the legacy
         webhook format instead. Created a separate legacy "Conversation"
         subscription (subscriptionType: "conversation.creation", no
         objectTypeId, objectId is the conversation ID directly) and
         added a matching branch. The old Communication/expanded-object
         branch is left in place but will likely never fire real events
         — harmless to leave, real traffic should come through the new
         conversation.creation branch instead.
  v1.8 - First real email successfully classified end-to-end (category
         fws_info, correctly routed to forward-to-James). Failed on the
         next step: allocate_conversation got a 400 Bad Request with no
         visible reason. Added _raise_with_body() to log HubSpot's actual
         error response text before raising, instead of the generic
         'Bad Request' message that hides what HubSpot really
         complained about. No functional fix yet — need to see the real
         error body on the next test to know what to actually fix.
  v1.9 - Got the real error: "EMPTY_UPDATE_REQUEST_BODY" — confirmed via
         HubSpot's own docs and multiple community threads that the
         Conversations API's PATCH endpoint ONLY supports updating
         status (open/closed), NOT assigning an owner. This is a genuine
         platform limitation, not a bug on our end — there is no public
         API way to do this. Removed allocate_conversation() calls from
         service_request/fws_info; they now rely solely on the forwarded
         email (with its note) to alert Christine/James, who'll need to
         manually claim the conversation in HubSpot. A HubSpot Workflow
         ("Assign conversation owner" action, needs Service/Data Hub
         Pro+) could replace this from HubSpot's side if wanted later.
  v1.10 - Added a one-time discovery endpoint (/api/webhook/debug/channels)
         to fetch the real inbox/channel/channel-account IDs needed to
         implement forward_email() for real. Sending a message via the
         Conversations API needs a senderActorId, channelId, and
         channelAccountId specific to this HubSpot account — rather than
         guess these (and likely repeat the multi-round guessing cycle we
         went through with the ticket pipeline IDs), fetch them for real
         first, then implement the actual send in the next version.
  v1.11 - Implemented forward_email() for real using the discovered IDs:
         channelId "1002" (EMAIL), channelAccountId "2083221560"
         (sales@futurewaste.com.au), senderActorId "A-46606016" (this
         app's own ID, following HubSpot's documented app-actor pattern).
         Not yet confirmed against a real successful send — first real
         test will tell us if senderActorId format is right.
  v1.12 - Got the real error: "Sender actor A-46606016 not found".
         HubSpot's docs confirm senderActorId must be a real HubSpot
         USER's ID, not an app ID — "an agent actor ID that is
         associated to the HubSpot user in the account who is sending
         the message." Fixed to resolve Christine's owner ID at runtime
         (via the existing owner-name cache) and use that instead.
"""
import os
import time
import logging
import sys

import requests
from flask import Flask, request, jsonify
import anthropic

# ================================================================
# CONFIG — real known values as defaults; secrets from env vars
# ================================================================
HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")
HUBSPOT_API_BASE = "https://api.hubapi.com"
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

HUDOC_EMAIL = os.getenv("HUDOC_EMAIL", "futurewastesolutions.7b4e@app.hubdoc.com")
JAMES_EMAIL = os.getenv("JAMES_EMAIL", "james@futurewaste.com.au")
CHRISTINE_EMAIL = os.getenv("CHRISTINE_EMAIL", "christine.baylon@futurewaste.com.au")

JAMES_OWNER_NAME = os.getenv("JAMES_OWNER_NAME", "James H Whelan")
CHRISTINE_OWNER_NAME = os.getenv("CHRISTINE_OWNER_NAME", "Support Christine Jane Baylon")

TICKET_PIPELINE = os.getenv("TICKET_PIPELINE", "Support Pipeline")
TICKET_STATUS_NEW = os.getenv("TICKET_STATUS_NEW", "New Ticket")

NOTE_SERVICE_REQUEST = "Please Action"
NOTE_SPAM = "Classified as spam and closed automatically — review if this looks wrong."

# TODO: confirm these against HubSpot's actual subscription dropdown once
# you click "Create subscription" in the private app's Webhooks tab.
COMMUNICATION_OBJECT_TYPE_ID = os.getenv("HUBSPOT_COMMUNICATION_OBJECT_TYPE_ID", "0-18")
INVOICE_OBJECT_TYPE_ID = os.getenv("HUBSPOT_INVOICE_OBJECT_TYPE_ID", "0-53")  # TODO: confirm real value

HEADERS = {"Authorization": f"Bearer {HUBSPOT_API_KEY}", "Content-Type": "application/json"}

# ================================================================
# LOGGING
# ================================================================
logger = logging.getLogger("fws_agent2")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(handler)


def log_decision(item_id, category, actions_taken):
    logger.info(f"item={item_id} category={category} actions={actions_taken}")


# ================================================================
# HUBSPOT CLIENT
# ================================================================
_owner_cache = {"by_name": {}, "fetched_at": 0}
_OWNER_CACHE_TTL_SECONDS = 60 * 60


def _refresh_owner_cache():
    owners = {}
    after = None
    while True:
        url = f"{HUBSPOT_API_BASE}/crm/v3/owners"
        params = {"limit": 100}
        if after:
            params["after"] = after
        resp = requests.get(url, headers=HEADERS, params=params)
        resp.raise_for_status()
        data = resp.json()
        for owner in data.get("results", []):
            full_name = f"{owner.get('firstName', '')} {owner.get('lastName', '')}".strip()
            owners[full_name] = owner["id"]
        after = data.get("paging", {}).get("next", {}).get("after")
        if not after:
            break
    _owner_cache["by_name"] = owners
    _owner_cache["fetched_at"] = time.time()


def get_owner_id_by_name(display_name):
    if not _owner_cache["by_name"] or (time.time() - _owner_cache["fetched_at"]) > _OWNER_CACHE_TTL_SECONDS:
        _refresh_owner_cache()
    if display_name in _owner_cache["by_name"]:
        return _owner_cache["by_name"][display_name]
    matches = [n for n in _owner_cache["by_name"] if display_name.lower() in n.lower()]
    if len(matches) == 1:
        return _owner_cache["by_name"][matches[0]]
    raise KeyError(f"Could not resolve HubSpot owner for '{display_name}' (candidates: {matches})")


def _raise_with_body(resp):
    """Log HubSpot's actual error response body before raising, so we can
    see the real reason for a 400/403/etc instead of just the generic
    'Bad Request for url: ...' message that hides what HubSpot actually
    complained about."""
    if not resp.ok:
        logger.error(f"HubSpot API error {resp.status_code} for {resp.url}: {resp.text}")
    resp.raise_for_status()


def close_conversation(conversation_id):
    url = f"{HUBSPOT_API_BASE}/conversations/v3/conversations/threads/{conversation_id}"
    resp = requests.patch(url, headers=HEADERS, json={"status": "CLOSED"})
    _raise_with_body(resp)


def allocate_conversation(conversation_id, owner_display_name):
    """
    NOT CURRENTLY USED — kept for reference only. HubSpot's Conversations
    API does not support assigning an owner via this PATCH endpoint (their
    own docs confirm only status updates/restores are supported here);
    calling this will fail with EMPTY_UPDATE_REQUEST_BODY. See the note
    in handle_service_request/handle_fws_info for the actual approach.
    """
    owner_id = get_owner_id_by_name(owner_display_name)
    url = f"{HUBSPOT_API_BASE}/conversations/v3/conversations/threads/{conversation_id}"
    resp = requests.patch(url, headers=HEADERS, json={"assignedTo": f"HUBSPOT_OWNER-{owner_id}"})
    _raise_with_body(resp)


def get_conversation_email(conversation_id):
    url = f"{HUBSPOT_API_BASE}/conversations/v3/conversations/threads/{conversation_id}/messages"
    resp = requests.get(url, headers=HEADERS, params={"limit": 1, "sort": "-createdAt"})
    resp.raise_for_status()
    results = resp.json().get("results", [])
    if not results:
        raise ValueError(f"No messages found for conversation {conversation_id}")
    latest = results[0]
    return {
        "email_id": latest["id"],
        "subject": latest.get("subject", ""),
        "body": latest.get("text", latest.get("richText", "")),
    }


# Real values discovered via /api/webhook/debug/channels — specific to
# this HubSpot account's Sales@futurewaste.com.au inbox.
EMAIL_CHANNEL_ID = "1002"
SALES_CHANNEL_ACCOUNT_ID = "2083221560"


def forward_email(conversation_id, to_address, note=""):
    """
    Sends a message into the thread addressed to to_address, using
    HubSpot's Conversations API. senderActorId must be a real HubSpot
    USER's actor ID (format "A-{userId}") — confirmed via HubSpot's docs
    and community reports that this cannot be an app ID, it must be "an
    agent actor ID that is associated to the HubSpot user in the account
    who is sending the message." Using Christine's owner ID (she's the
    default inbox owner) via the existing owner-resolution cache.
    """
    sender_owner_id = get_owner_id_by_name(CHRISTINE_OWNER_NAME)
    text = f"{note}\n\n---\nForwarded by FWS Agent 2." if note else "Forwarded by FWS Agent 2."
    url = f"{HUBSPOT_API_BASE}/conversations/v3/conversations/threads/{conversation_id}/messages"
    payload = {
        "type": "MESSAGE",
        "text": text,
        "senderActorId": f"A-{sender_owner_id}",
        "channelId": EMAIL_CHANNEL_ID,
        "channelAccountId": SALES_CHANNEL_ACCOUNT_ID,
        "recipients": [
            {"deliveryIdentifier": {"type": "HS_EMAIL_ADDRESS", "value": to_address}}
        ],
    }
    resp = requests.post(url, headers=HEADERS, json=payload)
    _raise_with_body(resp)
    logger.info(f"Forwarded conversation {conversation_id} to {to_address}")


_pipeline_cache = {"pipeline_id": None, "stage_id": None, "fetched_at": 0}
_PIPELINE_CACHE_TTL_SECONDS = 60 * 60


def _get_default_ticket_pipeline_and_stage():
    """
    HubSpot's Tickets API requires internal numeric IDs for hs_pipeline
    and hs_pipeline_stage (display labels like "Support Pipeline" cause a
    400 Bad Request). Rather than guess these, fetch them for real at
    runtime and cache them — mirrors the owner-name caching pattern above.
    Picks the pipeline matching TICKET_PIPELINE by label if found,
    otherwise falls back to the account's first/default pipeline, and
    takes that pipeline's first stage.
    """
    if _pipeline_cache["pipeline_id"] and (time.time() - _pipeline_cache["fetched_at"]) < _PIPELINE_CACHE_TTL_SECONDS:
        return _pipeline_cache["pipeline_id"], _pipeline_cache["stage_id"]

    url = f"{HUBSPOT_API_BASE}/crm/v3/pipelines/tickets"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    pipelines = resp.json().get("results", [])
    if not pipelines:
        raise ValueError("No ticket pipelines found on this HubSpot account")

    match = next((p for p in pipelines if p.get("label") == TICKET_PIPELINE), None)
    chosen = match or pipelines[0]

    stages = chosen.get("stages", [])
    if not stages:
        raise ValueError(f"Pipeline '{chosen.get('label')}' has no stages")
    stage_match = next((s for s in stages if s.get("label") == TICKET_STATUS_NEW), None)
    chosen_stage = stage_match or stages[0]

    _pipeline_cache["pipeline_id"] = chosen["id"]
    _pipeline_cache["stage_id"] = chosen_stage["id"]
    _pipeline_cache["fetched_at"] = time.time()
    return chosen["id"], chosen_stage["id"]


def create_ticket(subject, description, owner_display_name):
    owner_id = get_owner_id_by_name(owner_display_name)
    pipeline_id, stage_id = _get_default_ticket_pipeline_and_stage()
    url = f"{HUBSPOT_API_BASE}/crm/v3/objects/tickets"
    payload = {
        "properties": {
            "subject": subject,
            "content": description,
            "hs_pipeline": pipeline_id,
            "hs_pipeline_stage": stage_id,
            "hubspot_owner_id": owner_id,
            "source_type": "EMAIL",
        }
    }
    resp = requests.post(url, headers=HEADERS, json=payload)
    resp.raise_for_status()
    return resp.json()["id"]


def find_company_by_type_and_name(company_type, name_hint):
    url = f"{HUBSPOT_API_BASE}/crm/v3/objects/companies/search"
    payload = {
        "filterGroups": [{"filters": [
            {"propertyName": "type", "operator": "CONTAINS_TOKEN", "value": company_type},
            {"propertyName": "name", "operator": "CONTAINS_TOKEN", "value": name_hint},
        ]}],
        "properties": ["name", "type"],
        "limit": 1,
    }
    resp = requests.post(url, headers=HEADERS, json=payload)
    resp.raise_for_status()
    results = resp.json().get("results", [])
    return results[0]["id"] if results else None


def attach_file_to_company(company_id, file_url_or_path, filename):
    upload_url = f"{HUBSPOT_API_BASE}/filemanager/api/v3/files/upload"
    with open(file_url_or_path, "rb") as f:
        files = {"file": (filename, f)}
        data = {"options": '{"access": "PRIVATE"}', "folderPath": "/invoices"}
        resp = requests.post(upload_url, headers={"Authorization": HEADERS["Authorization"]}, files=files, data=data)
    resp.raise_for_status()
    file_id = resp.json()["id"]

    note_url = f"{HUBSPOT_API_BASE}/crm/v3/objects/notes"
    note_payload = {"properties": {"hs_note_body": f"Auto-attached invoice: {filename}", "hs_attachment_ids": str(file_id)}}
    note_resp = requests.post(note_url, headers=HEADERS, json=note_payload)
    note_resp.raise_for_status()
    note_id = note_resp.json()["id"]

    assoc_url = f"{HUBSPOT_API_BASE}/crm/v4/objects/notes/{note_id}/associations/default/companies/{company_id}"
    assoc_resp = requests.put(assoc_url, headers=HEADERS)
    assoc_resp.raise_for_status()


def get_invoice_details(invoice_id):
    """
    Uses the REAL data model from clv-invoice-automation's createHubSpotInvoice():
    - hs_title is set as "{Client Company Name} - {Supplier Name} - {Invoice Date}"
    - Client company is linked via a v4 association (NOT a text property)
    - Vendor/supplier is NOT associated to the invoice at all in the current
      script, so we parse it from the title as a best-effort fallback.

    NOTE: the invoice-created webhook fires the instant the invoice object
    itself is created — but clv-invoice-automation associates the client
    company AFTER looping through all line items (which can be 40+ API
    calls for a big invoice). So the association may genuinely not exist
    yet when this first runs. We retry a few times with short waits before
    giving up, rather than failing immediately on an empty result.

    TODO (longer-term, optional): have clv-invoice-automation associate
    the client (and ideally vendor) company immediately after creating the
    invoice record, before the line-items loop — would remove this race
    condition at the source instead of working around it here.
    """
    client_company_id = None
    # The original invoice webhook's own timing (line items loop + company
    # association) regularly takes 30-40+ seconds for larger invoices, so a
    # short retry window isn't enough — 6 attempts with 5s waits gives ~30s
    # total, which should cover most cases while staying safely under
    # Vercel's function timeout.
    for attempt in range(7):
        assoc_url = f"{HUBSPOT_API_BASE}/crm/v4/objects/invoices/{invoice_id}/associations/companies"
        assoc_resp = requests.get(assoc_url, headers=HEADERS)
        assoc_resp.raise_for_status()
        assoc_results = assoc_resp.json().get("results", [])
        if assoc_results:
            client_company_id = assoc_results[0]["toObjectId"]
            break
        if attempt < 6:
            logger.info(f"No company association yet for invoice {invoice_id}, retrying (attempt {attempt + 1}/7)...")
            time.sleep(5)

    # NOTE: vendor name parsing from the title was tried and deliberately
    # disabled. Title format is "{Client} - {Supplier} - {Date}", but real
    # client names can themselves contain " - " (e.g. "Campus Living
    # Villages - Sydney University"), which breaks a naive split and could
    # silently attach the invoice file to the WRONG company. Safer to skip
    # vendor attachment entirely until there's a reliable source for it.
    # TODO: the real fix is having clv-invoice-automation associate the
    # vendor company to the invoice directly (same as it already does for
    # client), removing the need to parse anything from the title at all.
    vendor_name_hint = ""

    return {
        "client_company_id": client_company_id,
        "vendor_name_hint": vendor_name_hint,
        "conversation_id": None,  # TODO: not currently stored anywhere on the invoice —
                                   # see note in handle_invoice_created about this
        "invoice_file_id": None,  # TODO: confirm where/if the source file is stored
    }


# ================================================================
# CLASSIFIER
# ================================================================
CLASSIFY_PROMPT = """You are classifying an email that arrived in the Sales@futurewaste.com.au
shared inbox (viewed via HubSpot Conversations) for FutureWaste Services
(FWS). Read the email and choose exactly ONE category:

1. invoice — an SP (service provider) invoice or tax invoice. This is
   handled by a separate upstream agent (forwarding to Veryfi for OCR) —
   if you classify an email this way, NO ACTION should be taken here;
   this agent's invoice-related work happens later, on a different
   trigger.
2. service_request — the sender is requesting a service, action, or
   help from FWS (not simply providing information).
3. fws_info — credit notes, payment receipts, meeting requests, or any
   other information relevant to FWS that isn't an invoice or a service
   request.
4. spam — unsolicited, irrelevant, or clearly automated junk mail with
   no legitimate business relevance.

Respond with only the category name: invoice, service_request, fws_info,
or spam.

If you are not confident, prefer fws_info over spam."""

VALID_CATEGORIES = {"invoice", "service_request", "fws_info", "spam"}


def classify_email(subject, body):
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=20,
        system=CLASSIFY_PROMPT,
        messages=[{"role": "user", "content": f"Subject: {subject}\n\nBody:\n{body}"}],
    )
    category = response.content[0].text.strip().lower()
    return category if category in VALID_CATEGORIES else "fws_info"


# ================================================================
# ACTIONS
# ================================================================
def handle_service_request(email_id, conversation_id):
    forward_email(conversation_id, CHRISTINE_EMAIL, note=NOTE_SERVICE_REQUEST)
    # NOTE: allocate_conversation() removed — HubSpot's own docs confirm
    # the Conversations API only supports updating a thread's status via
    # PATCH, NOT assigning an owner. This isn't a bug, it's a genuine
    # platform limitation (confirmed via multiple HubSpot community
    # threads reporting the same "EMPTY_UPDATE_REQUEST_BODY" error).
    # Christine gets the forwarded email with the "Please Action" note;
    # she'll need to manually claim the conversation in HubSpot rather
    # than have it pre-assigned. A HubSpot Workflow ("Assign conversation
    # owner" action, needs Service/Data Hub Pro+) could do this from the
    # HubSpot side if that becomes worth setting up later.
    log_decision(email_id, "service_request", ["forwarded_christine"])


def handle_fws_info(email_id, conversation_id):
    forward_email(conversation_id, JAMES_EMAIL)
    log_decision(email_id, "fws_info", ["forwarded_james"])


def handle_spam(email_id, conversation_id):
    forward_email(conversation_id, CHRISTINE_EMAIL, note=NOTE_SPAM)
    close_conversation(conversation_id)
    log_decision(email_id, "spam", ["forwarded_christine_fyi", "closed_conversation"])


def handle_invoice_no_action(email_id, conversation_id):
    log_decision(email_id, "invoice", ["no_action_awaiting_invoice_created_event"])


ACTION_MAP = {
    "invoice": handle_invoice_no_action,
    "service_request": handle_service_request,
    "fws_info": handle_fws_info,
    "spam": handle_spam,
}


def handle_invoice_created(invoice_id, client_company_id, vendor_name_hint,
                            conversation_id, invoice_file_path, invoice_filename):
    actions_taken = []

    if client_company_id:
        if invoice_file_path:
            attach_file_to_company(client_company_id, invoice_file_path, invoice_filename)
            actions_taken.append(f"attached_file_client:{client_company_id}")
        else:
            actions_taken.append("client_found_but_no_file_to_attach")
    else:
        actions_taken.append("client_company_not_found")

    vendor_company_id = find_company_by_type_and_name("Vendor", vendor_name_hint) if vendor_name_hint else None
    if vendor_company_id:
        if invoice_file_path:
            attach_file_to_company(vendor_company_id, invoice_file_path, invoice_filename)
            actions_taken.append(f"attached_file_vendor:{vendor_company_id}")
    else:
        actions_taken.append("vendor_company_not_found_or_no_name_hint")

    if conversation_id:
        forward_email(conversation_id, HUDOC_EMAIL)
        actions_taken.append("forwarded_hudoc")
        close_conversation(conversation_id)
        actions_taken.append("closed_conversation")
    else:
        actions_taken.append("no_conversation_id_available_skipped_forward_and_close")

    ticket_id = create_ticket(
        subject=f"SP invoice received — {invoice_id}",
        description="Auto-created from Sales inbox. Awaiting client invoice in Xero.",
        owner_display_name=JAMES_OWNER_NAME,
    )
    actions_taken.append(f"created_ticket:{ticket_id}")

    log_decision(invoice_id, "invoice_created", actions_taken)


VERSION = "v1.12"

# ================================================================
# FLASK APP — Vercel's Python runtime looks for a WSGI app named `app`
# ================================================================
app = Flask(__name__)


@app.route("/api/webhook", methods=["GET"])
def health_check():
    return jsonify({"status": "ok", "agent": "fws_agent2_hubspot_inbox", "version": VERSION}), 200


@app.route("/api/webhook/debug/channels", methods=["GET"])
def debug_channels():
    """
    One-time discovery endpoint: fetches the real inbox/channel/channel-
    account IDs needed to actually send messages via the Conversations
    API (forward_email's real implementation needs these — sender actor
    ID, channel ID, channel account ID — rather than guessing them, which
    would likely just produce another opaque error like the ticket
    pipeline issue did).
    """
    inboxes_resp = requests.get(f"{HUBSPOT_API_BASE}/conversations/v3/conversations/inboxes", headers=HEADERS)
    channels_resp = requests.get(f"{HUBSPOT_API_BASE}/conversations/v3/conversations/channels", headers=HEADERS)
    channel_accounts_resp = requests.get(
        f"{HUBSPOT_API_BASE}/conversations/v3/conversations/channel-accounts", headers=HEADERS
    )
    return jsonify({
        "inboxes": inboxes_resp.json() if inboxes_resp.ok else {"error": inboxes_resp.text},
        "channels": channels_resp.json() if channels_resp.ok else {"error": channels_resp.text},
        "channel_accounts": channel_accounts_resp.json() if channel_accounts_resp.ok else {"error": channel_accounts_resp.text},
    }), 200


@app.route("/api/webhook", methods=["POST"])
def webhook():
    events = request.get_json(force=True)
    if not isinstance(events, list):
        events = [events]

    results = []
    for event in events:
        subscription_type = event.get("subscriptionType", "")
        object_type_id = event.get("objectTypeId", "")
        try:
            if subscription_type == "conversation.creation":
                # Legacy format — HubSpot's docs confirm conversation.*
                # events aren't supported under the new expanded/generic
                # object model, so this arrives differently to Invoice
                # (no objectTypeId at all, just a direct conversation ID).
                conversation_id = str(event.get("objectId"))
                email = get_conversation_email(conversation_id)
                category = classify_email(email["subject"], email["body"])
                ACTION_MAP[category](email["email_id"], conversation_id)
                results.append({"conversation_id": conversation_id, "category": category, "status": "processed"})

            elif subscription_type == "object.creation" and object_type_id == COMMUNICATION_OBJECT_TYPE_ID:
                conversation_id = str(event.get("objectId"))
                email = get_conversation_email(conversation_id)
                category = classify_email(email["subject"], email["body"])
                ACTION_MAP[category](email["email_id"], conversation_id)
                results.append({"conversation_id": conversation_id, "category": category, "status": "processed"})

            elif subscription_type == "object.creation" and object_type_id == INVOICE_OBJECT_TYPE_ID:
                invoice_id = str(event.get("objectId"))
                details = get_invoice_details(invoice_id)
                handle_invoice_created(
                    invoice_id, details["client_company_id"], details["vendor_name_hint"],
                    details["conversation_id"], details["invoice_file_id"], f"invoice_{invoice_id}.pdf",
                )
                results.append({"invoice_id": invoice_id, "status": "processed"})

            else:
                logger.info(f"Ignoring unrecognised event: {subscription_type}")
                results.append({"status": "ignored", "subscriptionType": subscription_type})

        except Exception as e:
            logger.error(f"Error processing event {event}: {e}")
            results.append({"status": "error", "error": str(e), "event": event})

    return jsonify({"results": results}), 200
