import asyncio
import random
import os
import yaml
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
from utils.logger import already_applied, log_applied
from utils.browser import get_browser_options

with open("config.yaml") as f:
    cfg = yaml.safe_load(f)

PLATFORM = "Naukri"
APPLICANT = cfg["applicant"]
LIMITS = cfg["limits"]
RESUME = os.path.abspath(cfg["resume_path"])


async def delay(base=None):
    base = base or LIMITS["delay_between_jobs"]
    await asyncio.sleep(random.uniform(base * 0.8, base * 1.4))


async def login(page, email, password):
    print("🔐 Logging into Naukri...")
    await page.goto("https://www.naukri.com/nlogin/login", wait_until="domcontentloaded")
    await page.wait_for_timeout(2000)

    await page.fill("#usernameField", email)
    await page.fill("#passwordField", password)
    await page.click("button[type='submit']")
    await page.wait_for_timeout(4000)
    print("✅ Naukri logged in")


async def apply_to_job(page, url, title, company, location, skills=None):
    if already_applied(url):
        print(f"  ⏭️  Skip (already applied): {title}")
        return False

    from utils.matcher import evaluate_job_with_ai
    eval_res = evaluate_job_with_ai(title, company=company, skills=skills, url=url)
    if not eval_res["matches"]:
        print(f"  ⏭️  Skip (non-matching stack): {title} ({eval_res['reason']})")
        return False

    ai_score = eval_res.get("score", 75)
    ai_reason = eval_res.get("reason", "")

    from utils.confirmation import ask_confirmation
    if not ask_confirmation(title, company, PLATFORM, location=location, url=url, skills=skills, ai_score=ai_score, ai_reason=ai_reason):
        return False

    try:
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)

        # Click Apply button
        apply_btn = await page.query_selector("button#apply-button, a#apply-button, button.apply-button")
        if not apply_btn:
            # Try text-based match
            apply_btn = await page.query_selector("text=Apply")
        if not apply_btn:
            print(f"  ❌ No apply button: {title}")
            return False

        await apply_btn.click()
        await page.wait_for_timeout(3000)

        # Handle any modal / chatbot questionnaire using Gemini AI
        from utils.ai_engine import answer_screening_question

        # 1. Text & numeric inputs & textareas
        inputs = await page.query_selector_all("input[type='text'], input[type='number'], input[type='tel'], textarea")
        for inp in inputs:
            try:
                val = await inp.input_value()
                if val:
                    continue

                placeholder = (await inp.get_attribute("placeholder") or "").strip()
                name_attr = (await inp.get_attribute("name") or "").strip()
                aria_label = (await inp.get_attribute("aria-label") or "").strip()
                
                # Check for adjacent label
                inp_id = await inp.get_attribute("id")
                label_text = ""
                if inp_id:
                    lbl = await page.query_selector(f"label[for='{inp_id}']")
                    if lbl:
                        label_text = (await lbl.inner_text()).strip()

                q_prompt = label_text or aria_label or placeholder or name_attr or "General question"
                answered_val = answer_screening_question(q_prompt, question_type="text", job_title=title, company=company)
                if answered_val:
                    await inp.fill(str(answered_val))
            except Exception:
                pass

        # 2. Radio buttons / options
        radios = await page.query_selector_all("input[type='radio']")
        for rad in radios:
            try:
                is_checked = await rad.is_checked()
                if not is_checked:
                    rad_id = await rad.get_attribute("id")
                    rad_label = ""
                    if rad_id:
                        lbl = await page.query_selector(f"label[for='{rad_id}']")
                        if lbl:
                            rad_label = (await lbl.inner_text()).lower()
                    if rad_label in ["yes", "immediate", "15 days", "15", "full time"]:
                        await rad.check()
            except Exception:
                pass

        # 3. Dropdowns
        selects = await page.query_selector_all("select")
        for sel in selects:
            try:
                options = await sel.query_selector_all("option")
                texts = [await o.inner_text() for o in options]
                ans = answer_screening_question("selection dropdown", question_type="select", options=texts, job_title=title, company=company)
                if ans:
                    await sel.select_option(label=ans)
            except Exception:
                pass

        # Submit / confirm
        submit = (
            await page.query_selector("button:has-text('Submit')") or
            await page.query_selector("button:has-text('Apply')") or
            await page.query_selector("button:has-text('Confirm')")
        )
        if submit:
            await submit.click()
            await page.wait_for_timeout(2000)

        log_applied(url, title, company, location, PLATFORM)
        print(f"  ✅ Applied (AI Score {ai_score}%): {title} @ {company}")
        await delay()
        return True

    except PWTimeout:
        print(f"  ⏱️ Timeout: {title}")
        return False
    except Exception as e:
        print(f"  ⚠️ Error: {title} — {e}")
        return False


async def run(email, password):
    count = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(**get_browser_options())
        context = await browser.new_context(viewport={"width": 1280, "height": 800})
        page = await context.new_page()

        await login(page, email, password)

        for keyword in cfg["keywords"]:
            if count >= LIMITS["max_per_platform"]:
                break

            for location in cfg["filters"]["locations"]:
                if count >= LIMITS["max_per_platform"]:
                    break

                kw = keyword.replace(" ", "-").lower()
                loc = location.lower()
                # Naukri URL: /react-developer-jobs-in-pune  or  /react-developer-jobs
                if loc == "remote":
                    search_url = f"https://www.naukri.com/{kw}-jobs?jobAge=7&experience=0"
                else:
                    search_url = f"https://www.naukri.com/{kw}-jobs-in-{loc}?jobAge=7&experience=0"

                print(f"\n🔍 Naukri: '{keyword}' in {location}")
                try:
                    await page.goto(search_url, wait_until="domcontentloaded")
                    await page.wait_for_timeout(3000)

                    # Scroll to load listings
                    for _ in range(2):
                        await page.keyboard.press("End")
                        await page.wait_for_timeout(1000)

                    job_cards = await page.query_selector_all("article.jobTuple, div.srp-jobtuple-wrapper")
                    print(f"   Found {len(job_cards)} listings")

                    for card in job_cards:
                        if count >= LIMITS["max_per_platform"]:
                            break
                        try:
                            title_el = await card.query_selector("a.title, a.jobTitle")
                            if not title_el:
                                continue
                            job_title = (await title_el.inner_text()).strip()
                            job_url = await title_el.get_attribute("href")

                            company_el = await card.query_selector("a.subTitle, a.companyInfo")
                            company = (await company_el.inner_text()).strip() if company_el else ""

                            loc_el = await card.query_selector("span.locWdth, li.location")
                            job_loc = (await loc_el.inner_text()).strip() if loc_el else location

                            skills = []
                            try:
                                tag_els = await card.query_selector_all("ul.tags-gt li, .tag-li, .tag, [class*='chip'], ul.tagsAndSkills li")
                                for t in tag_els[:6]:
                                    txt = (await t.inner_text()).strip()
                                    if txt and txt not in skills:
                                        skills.append(txt)
                            except Exception:
                                pass

                            result = await apply_to_job(page, job_url, job_title, company, job_loc, skills=skills)
                            if result:
                                count += 1

                        except Exception as e:
                            continue

                    await asyncio.sleep(LIMITS["delay_between_searches"])

                except Exception as e:
                    print(f"  ⚠️ Search error: {e}")
                    continue

        await browser.close()
    print(f"\n✅ Naukri done — {count} applications")
    return count
