import asyncio
import random
import os
import yaml
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
from utils.logger import already_applied, log_applied
from utils.browser import get_browser_options

with open("config.yaml") as f:
    cfg = yaml.safe_load(f)

PLATFORM = "Indeed"
APPLICANT = cfg["applicant"]
COVER = cfg["cover_note"].strip()
LIMITS = cfg["limits"]
RESUME = os.path.abspath(cfg["resume_path"])


async def delay(base=None):
    base = base or LIMITS["delay_between_jobs"]
    await asyncio.sleep(random.uniform(base * 0.8, base * 1.5))


async def handle_indeed_form(page, job_title="", company="", skills=None):
    """Fill Indeed's Easy Apply multi-step form with Gemini AI"""
    try:
        from utils.ai_engine import answer_screening_question, tailor_cover_note

        # Resume upload
        file_input = await page.query_selector("input[type='file']")
        if file_input:
            await file_input.set_input_files(RESUME)
            await page.wait_for_timeout(1500)

        # Text fields
        inputs = await page.query_selector_all("input[type='text'], input[type='number'], input[type='tel']")
        for inp in inputs:
            try:
                val = await inp.input_value()
                if val:
                    continue

                label_text = ""
                label_id = await inp.get_attribute("id")
                if label_id:
                    label_el = await page.query_selector(f"label[for='{label_id}']")
                    if label_el:
                        label_text = (await label_el.inner_text()).lower()
                if not label_text:
                    label_text = (await inp.get_attribute("aria-label") or await inp.get_attribute("placeholder") or "").lower()

                if "phone" in label_text or "mobile" in label_text:
                    await inp.fill(APPLICANT["phone"])
                elif "city" in label_text or "location" in label_text:
                    await inp.fill("Bengaluru, Karnataka")
                elif "current" in label_text and ("salary" in label_text or "ctc" in label_text):
                    await inp.fill(APPLICANT.get("current_ctc", "0"))
                elif "salary" in label_text or "ctc" in label_text:
                    await inp.fill(APPLICANT.get("expected_ctc", "400000"))
                elif "notice" in label_text:
                    await inp.fill(APPLICANT.get("notice_period", "15"))
                elif "experience" in label_text or "year" in label_text:
                    await inp.fill(APPLICANT.get("experience_years", "0"))
                elif "name" in label_text:
                    await inp.fill(APPLICANT["name"])
                else:
                    ans = answer_screening_question(label_text, question_type="text", job_title=job_title, company=company)
                    await inp.fill(str(ans))
            except Exception:
                pass

        # Textareas (cover letter / custom questions)
        textareas = await page.query_selector_all("textarea")
        for ta in textareas:
            val = await ta.input_value()
            if not val:
                ta_id = await ta.get_attribute("id")
                q_text = ""
                if ta_id:
                    lbl = await page.query_selector(f"label[for='{ta_id}']")
                    if lbl:
                        q_text = (await lbl.inner_text()).strip()
                if q_text and len(q_text) > 5 and not "cover" in q_text.lower():
                    ans = answer_screening_question(q_text, question_type="textarea", job_title=job_title, company=company)
                    await ta.fill(ans)
                else:
                    tailored = tailor_cover_note(job_title=job_title, company=company, skills=skills)
                    await ta.fill(tailored)

        # Dropdowns
        selects = await page.query_selector_all("select")
        for sel in selects:
            try:
                options = await sel.query_selector_all("option")
                texts = [await o.inner_text() for o in options]
                ans = answer_screening_question("dropdown option", question_type="select", options=texts, job_title=job_title, company=company)
                if ans:
                    await sel.select_option(label=ans)
            except Exception:
                pass

    except Exception as e:
        print(f"  ⚠️ Form error: {e}")


async def apply_to_job(page, job_id, title, company, location, skills=None):
    job_url = f"https://in.indeed.com/viewjob?jk={job_id}"

    if already_applied(job_url):
        print(f"  ⏭️  Skip: {title}")
        return False

    from utils.matcher import evaluate_job_with_ai
    eval_res = evaluate_job_with_ai(title, company=company, skills=skills, url=job_url)
    if not eval_res["matches"]:
        print(f"  ⏭️  Skip (non-matching stack): {title} ({eval_res['reason']})")
        return False

    ai_score = eval_res.get("score", 75)
    ai_reason = eval_res.get("reason", "")

    from utils.confirmation import ask_confirmation
    if not ask_confirmation(title, company, PLATFORM, location=location, url=job_url, skills=skills, ai_score=ai_score, ai_reason=ai_reason):
        return False

    try:
        await page.goto(job_url, wait_until="domcontentloaded")
        await page.wait_for_timeout(2500)

        # Find Indeed Easy Apply button
        apply_btn = (
            await page.query_selector("button.indeedApplyButton") or
            await page.query_selector("button[id*='indeedApplyButton']") or
            await page.query_selector("span.indeed-apply-button-label")
        )
        if not apply_btn:
            print(f"  ❌ No Easy Apply: {title}")
            return False

        await apply_btn.click()
        await page.wait_for_timeout(3000)

        # Indeed opens apply in a new tab or modal — handle both
        # Try to find the apply iframe
        frame = None
        for f in page.frames:
            if "indeed" in f.url and "apply" in f.url:
                frame = f
                break

        target = frame or page

        # Walk through multi-step form
        max_steps = 6
        for step in range(max_steps):
            await handle_indeed_form(target, job_title=title, company=company, skills=skills)

            # Check for final submit
            submit = await target.query_selector("button[type='submit']:has-text('Submit')")
            if submit:
                await submit.click()
                await page.wait_for_timeout(2000)
                log_applied(job_url, title, company, location, PLATFORM)
                print(f"  ✅ Applied: {title} @ {company}")
                await delay()
                return True

            # Next step
            next_btn = (
                await target.query_selector("button:has-text('Continue')") or
                await target.query_selector("button:has-text('Next')") or
                await target.query_selector("button[type='submit']")
            )
            if next_btn:
                await next_btn.click()
                await page.wait_for_timeout(2000)
            else:
                break

        return False

    except PWTimeout:
        print(f"  ⏱️ Timeout: {title}")
        return False
    except Exception as e:
        print(f"  ⚠️ Error: {title} — {e}")
        return False


async def run(email=None):
    count = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(**get_browser_options())
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()

        # Indeed India — no login required for Easy Apply on many jobs
        # But pre-fill email if available
        await page.goto("https://in.indeed.com", wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)

        for keyword in cfg["keywords"]:
            if count >= LIMITS["max_per_platform"]:
                break

            for location in cfg["filters"]["locations"]:
                if count >= LIMITS["max_per_platform"]:
                    break

                loc_param = "remote" if location.lower() == "remote" else location
                kw_encoded = keyword.replace(" ", "+")

                # Indeed India URL with Easy Apply filter (&iafilter=1) and date posted last 7 days (&fromage=7)
                search_url = (
                    f"https://in.indeed.com/jobs?q={kw_encoded}&l={loc_param}"
                    f"&iafilter=1&fromage=7&explvl=entry_level"
                )

                print(f"\n🔍 Indeed: '{keyword}' in {location}")

                try:
                    await page.goto(search_url, wait_until="domcontentloaded")
                    await page.wait_for_timeout(3000)

                    # Extract job IDs from the page
                    job_cards = await page.query_selector_all("div.job_seen_beacon, div.resultContent")
                    print(f"   Found {len(job_cards)} listings")

                    for card in job_cards:
                        if count >= LIMITS["max_per_platform"]:
                            break
                        try:
                            # Get job ID from data attribute
                            job_id = await card.get_attribute("data-jk")
                            if not job_id:
                                link_el = await card.query_selector("a[data-jk]")
                                if link_el:
                                    job_id = await link_el.get_attribute("data-jk")

                            if not job_id:
                                continue

                            title_el = await card.query_selector("h2.jobTitle span, span[title]")
                            job_title = (await title_el.inner_text()).strip() if title_el else keyword

                            company_el = await card.query_selector("span.companyName, [data-testid='company-name']")
                            company = (await company_el.inner_text()).strip() if company_el else ""

                            loc_el = await card.query_selector("div.companyLocation, [data-testid='text-location']")
                            job_loc = (await loc_el.inner_text()).strip() if loc_el else location

                            skills = []
                            try:
                                tag_els = await card.query_selector_all("div.metadataContainer div, div.salary-snippet-container, div.jobMetaDataGroup")
                                for t in tag_els[:4]:
                                    txt = (await t.inner_text()).strip()
                                    if txt and txt not in skills:
                                        skills.append(txt)
                            except Exception:
                                pass

                            result = await apply_to_job(page, job_id, job_title, company, job_loc, skills=skills)
                            if result:
                                count += 1

                        except Exception:
                            continue

                    await asyncio.sleep(LIMITS["delay_between_searches"])

                except Exception as e:
                    print(f"  ⚠️ Search error: {e}")
                    continue

        await browser.close()
    print(f"\n✅ Indeed done — {count} applications")
    return count
