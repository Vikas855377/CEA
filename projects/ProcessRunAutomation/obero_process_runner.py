"""Open an authenticated CEA Support/Xactly session and persist its cookies."""

import os
import sys
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from dotenv import load_dotenv
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


PROJECT_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PROJECT_ROOT.parents[1]
load_dotenv(REPO_ROOT / ".env")

OBERO_BASE_URL = os.getenv("OBERO_BASE_URL", "https://ceasupport.obero.net").rstrip("/")
OBERO_LOGIN_URL = os.getenv(
    "OBERO_LOGIN_URL",
    f"{OBERO_BASE_URL}/?callbackUrl="
    f"{OBERO_BASE_URL}/m/Account/AzureLoginContinued?returnUrl=%2Fm",
)
OBERO_SUCCESS_URL = os.getenv("OBERO_SUCCESS_URL", f"{OBERO_BASE_URL}/m")
OBERO_HOME_URL = os.getenv("OBERO_HOME_URL", f"{OBERO_BASE_URL}/m")
PROCESS_APP_URL = os.getenv("CEA_PROCESS_APP_URL", f"{OBERO_BASE_URL}/ProcessApp")
PROCESS_LAUNCH_URL = os.getenv(
    "CEA_PROCESS_LAUNCH_URL",
    f"{OBERO_BASE_URL}/m/Tiles/StartModule/Process",
)
RUN_PROCESS_URL = os.getenv(
    "CEA_RUN_PROCESS_URL",
    f"{OBERO_BASE_URL}/ProcessApp/Home/RunProcess",
)
PROCESS_FS_ID = os.getenv("CEA_PROCESS_FS_ID", "133")
SESSION_FILE = Path(os.getenv("OBERO_SESSION_FILE", ".obero-session.json"))
XACTLY_USERNAME = os.getenv("XACTLY_USERNAME", "")
XACTLY_PASSWORD = os.getenv("XACTLY_PASSWORD", "")
HEADLESS = os.getenv("OBERO_HEADLESS", "true").lower() not in {"0", "false", "no"}
FORCE_LOGIN = os.getenv("OBERO_FORCE_LOGIN", "false").lower() in {"1", "true", "yes"}
SESSION_CHECK_TIMEOUT_MS = int(os.getenv("OBERO_SESSION_CHECK_TIMEOUT_MS", "12000"))
APP_READY_TIMEOUT_MS = int(os.getenv("OBERO_APP_READY_TIMEOUT_MS", "6000"))


def is_success_url(url: str) -> bool:
    expected = urlparse(OBERO_SUCCESS_URL)
    actual = urlparse(url)
    return (
        actual.hostname == expected.hostname
        and actual.path.rstrip("/") == expected.path.rstrip("/")
    )


def first_visible(page, selectors: list[str]):
    for selector in selectors:
        matches = page.locator(selector)
        for index in range(matches.count()):
            candidate = matches.nth(index)
            if candidate.is_visible():
                return candidate
    return None


def complete_xactly_login(page) -> bool:
    if not XACTLY_USERNAME or not XACTLY_PASSWORD:
        print("[ERROR] XACTLY_USERNAME and XACTLY_PASSWORD are required in .env.")
        return False

    try:
        password = page.locator("input[type='password']:visible").first
        password.wait_for(state="visible", timeout=30_000)
    except PlaywrightTimeoutError:
        print("[FAIL] Could not find the Xactly password field.")
        return False

    username = first_visible(
        page,
        [
            "input[type='email']",
            "input[name*='user' i]",
            "input[id*='user' i]",
            "input[type='text']",
        ],
    )
    if username is None:
        print("[FAIL] Could not find the Xactly username field.")
        return False

    username.fill(XACTLY_USERNAME)
    password.fill(XACTLY_PASSWORD)
    login_form = password.locator("xpath=ancestor::form[1]")
    login_button = login_form.locator(
        "button[type='submit'], input[type='submit'], button"
    ).last
    if login_button.count() == 0:
        print("[FAIL] Could not find the Xactly login button.")
        return False

    print("Submitting Xactly credentials...")
    login_button.click()
    try:
        password.wait_for(state="hidden", timeout=30_000)
    except PlaywrightTimeoutError:
        print("[FAIL] Xactly did not accept the credentials or remained on login.")
        return False

    # Authorization behavior differs by tenant: some show an Approve form,
    # while previously authorized tenants redirect straight back to Obero.
    approve = None
    approval_deadline_ms = int(os.getenv("XACTLY_APPROVAL_WAIT_MS", "20000"))
    waited_ms = 0
    while waited_ms < approval_deadline_ms:
        if is_success_url(page.url):
            print("Xactly access was already approved; continuing after direct redirect.")
            return True
        approve = first_visible(
            page,
            [
                "button[name*='approve' i]",
                "button[id*='approve' i]",
                "input[name*='approve' i]",
                "input[id*='approve' i]",
                "input[value*='approve' i]",
            ],
        )
        if approve is None:
            submits = page.locator(
                "form button[type='submit']:visible, form input[type='submit']:visible"
            )
            if submits.count():
                approve = submits.last
        if approve is not None:
            break
        page.wait_for_timeout(250)
        waited_ms += 250
    if approve is None:
        if is_success_url(page.url):
            return True
        print(
            f"[FAIL] Xactly neither redirected to Obero nor showed an Approve "
            f"button within {approval_deadline_ms / 1000:.0f} seconds. Current page: {page.url}"
        )
        return False

    print("Approving Xactly access...")
    try:
        approve.click()
    except PlaywrightTimeoutError:
        # Playwright may finish the click but time out while waiting for the
        # resulting cross-site redirect. The caller performs the authoritative
        # wait_for_url check, so allow it to determine whether authentication
        # actually succeeded.
        print(
            "[WARN] Approve click navigation did not settle within 30 seconds; "
            "continuing to verify the authenticated Obero URL."
        )
    return True


def run_cea_process(page, context) -> bool:
    print(f"Loading CEA Process App for process {PROCESS_FS_ID}...")
    observed_tokens = []

    def capture_request_token(request):
        token = request.headers.get("requestverificationtoken", "")
        if token:
            observed_tokens.append((request.url, token, request.headers))

    def capture_response_token(response):
        token = response.headers.get("requestverificationtoken", "")
        if token:
            observed_tokens.append((response.url, token, {}))

    page.on("request", capture_request_token)
    page.on("response", capture_response_token)
    print("Starting Process App through the CEA module launcher...")
    response = page.goto(PROCESS_LAUNCH_URL, wait_until="domcontentloaded")

    try:
        page.wait_for_load_state("networkidle", timeout=APP_READY_TIMEOUT_MS)
    except PlaywrightTimeoutError:
        # Some application pages poll continuously; a stable DOM is sufficient.
        page.wait_for_load_state("domcontentloaded")
        page.wait_for_timeout(1_000)

    if urlparse(page.url).path.lower().rstrip("/").endswith("/login"):
        page.wait_for_timeout(2_000)
        controls = page.evaluate(
            """
            () => [...document.querySelectorAll('a, button, input[type="submit"]')]
              .filter(e => e.offsetParent !== null)
              .map(e => ({
                text: (e.innerText || e.value || e.getAttribute('aria-label') || '').trim(),
                href: e.href || ''
              }))
            """
        )
        safe_controls = [
            {"text": item["text"][:80], "href": item["href"]}
            for item in controls
        ]
        print(f"[FAIL] Process App redirected to its login page: {page.url}")
        print(f"Visible login controls: {safe_controls}")
        return False

    if response is not None and not response.ok:
        print(f"[FAIL] Could not launch CEA Process App (status: {response.status}).")
        return False

    token_script = """
    () => {
      const selectors = [
        'input[name="__RequestVerificationToken"]',
        'input[name="RequestVerificationToken"]',
        'meta[name="request-verification-token"]',
        'meta[name="csrf-token"]',
        'meta[name="xsrf-token"]'
      ];
      for (const selector of selectors) {
        const element = document.querySelector(selector);
        if (element) return element.value || element.content || '';
      }
      for (const element of document.querySelectorAll('input, meta, [data-token], [data-csrf]')) {
        const values = [
          element.value,
          element.content,
          element.getAttribute('data-token'),
          element.getAttribute('data-csrf')
        ];
        for (const value of values) {
          if (value && value.length > 80) return value;
        }
      }
      return '';
    }
    """
    form_token = ""
    for attempt in range(3):
        try:
            form_token = page.evaluate(token_script)
            break
        except PlaywrightError as exc:
            if "Execution context was destroyed" not in str(exc) or attempt == 2:
                print(f"[FAIL] Could not inspect the Process App page: {exc}")
                return False
            page.wait_for_load_state("domcontentloaded")
            page.wait_for_timeout(1_000)

    if not form_token:
        for frame in page.frames:
            if frame == page.main_frame:
                continue
            try:
                form_token = frame.evaluate(token_script)
            except PlaywrightError:
                continue
            if form_token:
                break

    cookies = context.cookies(PROCESS_APP_URL)
    token_cookies = [
        cookie
        for cookie in cookies
        if any(
            marker in cookie["name"].lower()
            for marker in ("requestverificationtoken", "csrf", "xsrf", "antiforgery")
        )
    ]
    token_cookie = next(
        (
            cookie["value"]
            for cookie in token_cookies
            if cookie["value"]
        ),
        "",
    )

    process_tokens = [
        (url, token, headers)
        for url, token, headers in observed_tokens
        if urlparse(url).path.lower().startswith("/processapp")
    ]
    selected_process_token = next(
        (item for item in reversed(process_tokens) if item[2]),
        process_tokens[-1] if process_tokens else None,
    )
    verification_token = selected_process_token[1] if selected_process_token else form_token
    if form_token and token_cookie and ":" not in form_token:
        verification_token = f"{token_cookie}:{form_token}"
    elif not form_token and ":" in token_cookie:
        verification_token = token_cookie

    if not verification_token:
        print("[FAIL] Could not find the CEA request verification token.")
        print(f"Final Process App URL: {page.url}")
        print(f"Page title: {page.title()}")
        diagnostics = page.evaluate(
            """
            () => ({
              inputs: [...document.querySelectorAll('input')]
                .map(e => e.name || e.id || e.type).filter(Boolean),
              metas: [...document.querySelectorAll('meta[name]')]
                .map(e => e.name).filter(Boolean)
            })
            """
        )
        print(f"Input names: {diagnostics['inputs']}")
        print(f"Meta names: {diagnostics['metas']}")
        print(f"Cookie names: {[cookie['name'] for cookie in cookies]}")
        print(f"Frame URLs: {[frame.url for frame in page.frames]}")
        print(f"Observed token request URLs: {[url for url, _, _ in observed_tokens]}")
        return False


    inherited_headers = {}
    if selected_process_token:
        print(f"Using ProcessApp verification context from: {selected_process_token[0]}")
        source_headers = selected_process_token[2]
        allowed_headers = {
            "accept-language",
            "cache-control",
            "pragma",
            "request-id",
        }
        inherited_headers = {
            name: value
            for name, value in source_headers.items()
            if name.lower() in allowed_headers
        }
        print(f"Reusing ProcessApp header names: {sorted(inherited_headers)}")

    if ":" in verification_token:
        cookie_token = verification_token.split(":", 1)[0]
        context.add_cookies(
            [
                {
                    "name": "__RequestVerificationToken",
                    "value": cookie_token,
                    "domain": urlparse(OBERO_BASE_URL).hostname,
                    "path": "/",
                    "secure": True,
                    "httpOnly": True,
                    "sameSite": "Lax",
                }
            ]
        )
        print("Prepared the required ProcessApp anti-forgery cookie.")

    result = page.evaluate(
        """
        async ({url, token, fsId, inheritedHeaders}) => {
          const response = await fetch(url, {
            method: 'POST',
            credentials: 'include',
            headers: {
              ...inheritedHeaders,
              'Accept': 'application/json, text/plain, */*',
              'Content-Type': 'application/json;charset=UTF-8',
              'RequestVerificationToken': token,
              'X-Requested-With': 'XMLHttpRequest'
            },
            body: JSON.stringify({fsId})
          });
          return {
            ok: response.ok,
            status: response.status,
            url: response.url,
            redirected: response.redirected,
            contentType: response.headers.get('content-type') || '',
            text: await response.text()
          };
        }
        """,
        {
            "url": RUN_PROCESS_URL,
            "token": verification_token,
            "fsId": PROCESS_FS_ID,
            "inheritedHeaders": inherited_headers,
        },
    )

    if (
        not result["ok"]
        and "anti-forgery form field" in result["text"].lower()
        and ":" in verification_token
    ):
        form_token_value = verification_token.split(":", 1)[1]
        print("Retrying RunProcess with ASP.NET anti-forgery form data...")
        result = page.evaluate(
            """
            async ({url, token, formToken, fsId, inheritedHeaders}) => {
              const headers = {...inheritedHeaders};
              delete headers['content-type'];
              headers['Accept'] = 'application/json, text/plain, */*';
              headers['RequestVerificationToken'] = token;
              headers['X-Requested-With'] = 'XMLHttpRequest';
              headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8';
              const body = new URLSearchParams({
                fsId,
                __RequestVerificationToken: formToken
              });
              const response = await fetch(url, {
                method: 'POST',
                credentials: 'include',
                headers,
                body: body.toString()
              });
              return {
                ok: response.ok,
                status: response.status,
                url: response.url,
                redirected: response.redirected,
                contentType: response.headers.get('content-type') || '',
                text: await response.text()
              };
            }
            """,
            {
                "url": RUN_PROCESS_URL,
                "token": verification_token,
                "formToken": form_token_value,
                "fsId": PROCESS_FS_ID,
                "inheritedHeaders": inherited_headers,
            },
        )

    body = result["text"][:1000]
    if not result["ok"]:
        plain_text = page.evaluate(
            """
            html => {
              const document = new DOMParser().parseFromString(html, 'text/html');
              return document.body?.innerText || document.documentElement?.innerText || '';
            }
            """,
            result["text"],
        )
        plain_text = "\n".join(
            line.strip() for line in plain_text.splitlines() if line.strip()
        )
        print(
            f"[FAIL] RunProcess returned HTTP {result['status']} "
            f"from {result['url']} (redirected={result['redirected']}, "
            f"content-type={result['contentType']})."
        )
        print(f"Response text: {plain_text[:4000] or body}")
        return False

    print(f"[SUCCESS] Process {PROCESS_FS_ID} submitted (HTTP {result['status']}).")
    if body:
        print(f"RunProcess response: {body}")
    return True


def main() -> int:
    expected_host = urlparse(OBERO_SUCCESS_URL).hostname
    if not expected_host:
        print("[ERROR] OBERO_SUCCESS_URL is invalid.")
        return 2

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(
            headless=HEADLESS,
            args=[
                "--disable-background-networking",
                "--disable-component-update",
                "--disable-default-apps",
                "--disable-dev-shm-usage",
                "--disable-extensions",
                "--disable-gpu",
                "--disable-notifications",
                "--disable-popup-blocking",
                "--disable-sync",
                "--no-first-run",
            ],
        )
        context_options = {}
        if SESSION_FILE.exists() and not FORCE_LOGIN:
            context_options["storage_state"] = str(SESSION_FILE)
        context = browser.new_context(**context_options)
        page = context.new_page()
        page.route(
            "**/*",
            lambda route: route.abort()
            if route.request.resource_type in {"image", "media", "font"}
            else route.continue_(),
        )
        session_check_failed = False

        if SESSION_FILE.exists() and not FORCE_LOGIN:
            print(f"Checking saved Obero session: {OBERO_BASE_URL}")
            try:
                page.goto(
                    OBERO_SUCCESS_URL,
                    wait_until="domcontentloaded",
                    timeout=SESSION_CHECK_TIMEOUT_MS,
                )
            except PlaywrightTimeoutError:
                if is_success_url(page.url):
                    print(
                        f"[WARN] Saved-session page load timed out for {OBERO_BASE_URL}, "
                        "but the authenticated URL was reached; continuing."
                    )
                else:
                    session_check_failed = True
                    print(
                        f"[WARN] Saved-session check timed out for {OBERO_BASE_URL}; "
                        "forcing a fresh login."
                    )
                    try:
                        page.goto("about:blank", wait_until="commit", timeout=5_000)
                    except PlaywrightError:
                        pass

        if (
            FORCE_LOGIN
            or session_check_failed
            or not SESSION_FILE.exists()
            or not is_success_url(page.url)
        ):
            print(f"Opening Obero login page: {OBERO_BASE_URL}")
            page.goto(OBERO_LOGIN_URL, wait_until="domcontentloaded")
            print(f"Obero login page loaded: {page.url}")

        if not is_success_url(page.url):
            try:
                xactly_link = page.get_by_text("Login with Xactly", exact=True).first
                xactly_link.wait_for(state="visible", timeout=30_000)
                print("Clicking Login with Xactly...")
                xactly_link.click()
            except PlaywrightTimeoutError:
                print("[FAIL] Could not find the 'Login with Xactly' link.")
                browser.close()
                return 1

            if not complete_xactly_login(page):
                browser.close()
                return 1

        print("Waiting for Obero authentication...")

        try:
            page.wait_for_url(
                is_success_url,
                timeout=300_000,
            )
            try:
                page.wait_for_load_state("domcontentloaded", timeout=30_000)
            except PlaywrightTimeoutError:
                if not is_success_url(page.url):
                    raise
                print(
                    f"[WARN] {OBERO_BASE_URL}/m reached the authenticated URL "
                    "before its page load completed; continuing."
                )
        except PlaywrightTimeoutError:
            current = urlparse(page.url)
            query = parse_qs(current.query)
            if "unsuccessfullogin" in current.path.lower() or query.get("Exception"):
                message = unquote(query.get("Exception", ["Obero rejected the login."])[0])
                print(f"[FAIL] {message.strip()}")
            else:
                print(
                    f"[FAIL] Login did not reach {OBERO_SUCCESS_URL} within 5 minutes. "
                    f"Current page: {page.url}"
                )
            browser.close()
            return 1

        final_url = urlparse(page.url)
        query = parse_qs(final_url.query)
        if "unsuccessfullogin" in final_url.path.lower() or query.get("Exception"):
            message = unquote(query.get("Exception", ["Obero rejected the login."])[0])
            print(f"[FAIL] {message.strip()}")
            print(
                "Ask the Obero/Xactly administrator to authorize this user for "
                "the training application."
            )
            browser.close()
            return 1

        context.storage_state(path=str(SESSION_FILE))
        SESSION_FILE.chmod(0o600)
        print(f"[SUCCESS] Authenticated on {page.url}")
        print(f"Session saved to {SESSION_FILE}")

        # The saved-session check/login already lands on the authenticated home.
        # Avoid downloading the same page a second time before ProcessApp launch.
        print("Authenticated home is ready; launching ProcessApp directly.")

        if not run_cea_process(page, context):
            browser.close()
            return 1

        browser.close()
        return 0


if __name__ == "__main__":
    sys.exit(main())
