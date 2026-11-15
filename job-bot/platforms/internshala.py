import asyncio
import random
import os
import yaml
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
from utils.logger import already_applied, log_applied
from utils.browser import get_browser_options

with open("config.yaml") as f:
    cfg = yaml.safe_load(f)

PLATFORM = "Internshala"
APPLICANT = cfg["applicant"]
COVER = cfg["cover_note"].strip()
LIMITS = cfg["limits"]
RESUME = os.path.abspath(cfg["resume_path"])


async def delay(base=None):
    base = base or LIMITS["delay_between_jobs"]
    await asyncio.sleep(random.uniform(base * 0.8, base * 1.5))


async def login(page, email, password):
    print("🔐 Logging into Internshala...")
    await page.goto("https://internshala.com/login/user", wait_until="domcontentloaded")
    await page.wait_for_timeout(2000)

    await page.fill("#email", email)
    await page.fill("#password", password)
    await page.click("#login_submit")
    await page.wait_for_timeout(3000)
    print("✅ Internshala logged in")


async def fill_application_form(page, job_title="", company="", skills=None):
    """Handle Internshala's cover letter / application form with Gemini AI"""
    try:
        from utils.ai_engine import answer_screening_question, tailor_cover_note

        # 1. Cover letter textarea (dynamically tailored using Gemini)
        cover_textarea = await page.query_selector("textarea#cover_letter_text, textarea[name='cover_letter']")
        if cover_textarea:
            val = await cover_textarea.input_value()
            if not val:
                tailored = tailor_cover_note(job_title=job_title, company=company, skills=skills)
                await cover_textarea.fill(tailored)

        # 2. Availability question (when can you start)
        avail = await page.query_selector("input#availability, select#availability")
        if avail:
            tag = await avail.evaluate("el => el.tagName")
            if tag == "SELECT":
                await avail.select_option(index=1)
            else:
                await avail.fill("Immediately")

        # 3. Handle employer assignment & custom screening question textareas
        textareas = await page.query_selector_all("textarea:not(#cover_letter_text):not([name='cover_letter'])")
        for ta in textareas:
            try:
                val = await ta.input_value()
                if val:
                    continue
                # Extract question label or placeholder
                ta_id = await ta.get_attribute("id")
                q_text = ""
                if ta_id:
                    lbl = await page.query_selector(f"label[for='{ta_id}']")
                    if lbl:
                        q_text = await lbl.inner_text()
                if not q_text:
                    q_text = await ta.get_attribute("placeholder") or "Why should you be hired for this role?"

                ai_ans = answer_screening_question(q_text, question_type="textarea", job_title=job_title, company=company)
                await ta.fill(ai_ans)
            except Exception:
                pass

        # 4. Any other text inputs (phone, city, experience, etc.)
        inputs = await page.query_selector_all("input[type='text']:not([readonly])")
        for inp in inputs:
            try:
                name = (await inp.get_attribute("name") or "").lower()
                placeholder = (await inp.get_attribute("placeholder") or "").lower()
                val = await inp.input_value()
                if val:
                    continue
                if "phone" in name or "phone" in placeholder:
                    await inp.fill(APPLICANT["phone"])
                elif "city" in name or "location" in placeholder:
                    await inp.fill("Bengaluru")
                elif "github" in name or "github" in placeholder:
                    await inp.fill("https://github.com/Avinashb722")
                elif "portfolio" in name or "website" in placeholder:
                    await inp.fill("https://github.com/Avinashb722")
                else:
                    ans = answer_screening_question(placeholder or name, question_type="text", job_title=job_title, company=company)
                    await inp.fill(str(ans))
            except Exception:
                pass

        # 5. Radio options (e.g. Yes/No for availability)
        radios = await page.query_selector_all("input[type='radio']")
        for rad in radios:
            try:
                if not (await rad.is_checked()):
                    val_attr = (await rad.get_attribute("value") or "").lower()
                    if val_attr in ["yes", "y", "immediate", "1"]:
                        await rad.check()
            except Exception:
                pass
    except Exception as e:
        print(f"  ⚠️ Form fill error: {e}")


async def apply_to_job(page, url, title, company, location, skills=None):
    if already_applied(url):
        print(f"  ⏭️  Skip: {title}")
        return False

    from utils.matcher import evaluate_job_with_ai
    eval_res = evaluate_job_with_ai(title, company=company, skills=skills, url=url)
    if not eval_res["matches"]:
        print(f"  ⏭️  Skip (non-matching): {title} ({eval_res['reason']})")
        return False

    ai_score = eval_res.get("score", 75)
    ai_reason = eval_res.get("reason", "")

    from utils.confirmation import ask_confirmation
    if not ask_confirmation(title, company, PLATFORM, location=location, url=url, skills=skills, ai_score=ai_score, ai_reason=ai_reason):
        return False

    try:
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)

        # Find Apply Now button
        apply_btn = (
            await page.query_selector("a#apply_now_button") or
            await page.query_selector("button:has-text('Apply now')") or
            await page.query_selector("a:has-text('Apply now')")
        )
        if not apply_btn:
            print(f"  ❌ No apply button: {title}")
            return False

        await apply_btn.click()
        await page.wait_for_timeout(2500)

        # Fill the form with AI assistance
        await fill_application_form(page, job_title=title, company=company, skills=skills)

        # Submit
        submit = (
            await page.query_selector("input#submit, button#submit") or
            await page.query_selector("button:has-text('Submit application')") or
            await page.query_selector("button:has-text('Submit')")
        )
        if submit:
            await submit.click()
            await page.wait_for_timeout(2000)
            log_applied(url, title, company, location, PLATFORM)
            print(f"  ✅ Applied (AI Score {ai_score}%): {title} @ {company}")
            await delay()
            return True

        return False

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

            kw = keyword.replace(" ", "%20")

            # Internshala has /jobs and /internships — hit both
            for section in ["jobs", "internships"]:
                if count >= LIMITS["max_per_platform"]:
                    break

                search_url = f"https://internshala.com/{section}/{keyword.lower().replace(' ', '-')}-{section}"
                print(f"\n🔍 Internshala ({section}): '{keyword}'")

                try:
                    await page.goto(search_url, wait_until="domcontentloaded")
                    await page.wait_for_timeout(3000)

                    # Scroll to load all cards
                    for _ in range(3):
                        await page.keyboard.press("End")
                        await page.wait_for_timeout(800)

                    job_cards = await page.query_selector_all(".individual_internship, .job-internship-card")
                    print(f"   Found {len(job_cards)} listings")

                    for card in job_cards:
                        if count >= LIMITS["max_per_platform"]:
                            break
                        try:
                            # Get the detail page link
                            link_el = await card.query_selector("a.job-title-href, a.view_detail_button, h3 a")
                            if not link_el:
                                continue

                            href = await link_el.get_attribute("href")
                            if not href:
                                continue
                            job_url = f"https://internshala.com{href}" if href.startswith("/") else href

                            # Extract authentic job title from DOM or URL slug
                            title_text = ""
                            for sel in ["a.job-title-href", "h3.heading_4_5 a", "div.heading_4_5 a", ".job-internship-name", "h3", ".job-title", ".profile"]:
                                t_el = await card.query_selector(sel)
                                if t_el:
                                    txt = (await t_el.inner_text()).strip()
                                    if txt and txt.lower() not in ["view details", "apply", "apply now"]:
                                        title_text = txt
                                        break

                            if not title_text:
                                from utils.matcher import slug_to_title
                                title_text = slug_to_title(job_url)

                            job_title = title_text or keyword

                            company_el = await card.query_selector(".company_name, .company-name")
                            company = (await company_el.inner_text()).strip() if company_el else ""

                            location_el = await card.query_selector(".location_link, .location")
                            location = (await location_el.inner_text()).strip() if location_el else "India"

                            skills = []
                            try:
                                tag_els = await card.query_selector_all(".round_tabs, .status-container, .skills-list span, .item_body")
                                for t in tag_els[:6]:
                                    txt = (await t.inner_text()).strip()
                                    if txt and txt not in skills:
                                        skills.append(txt)
                            except Exception:
                                pass

                            result = await apply_to_job(page, job_url, job_title, company, location, skills=skills)
                            if result:
                                count += 1

                        except Exception:
                            continue

                    await asyncio.sleep(LIMITS["delay_between_searches"])

                except Exception as e:
                    print(f"  ⚠️ Search error: {e}")
                    continue

        await browser.close()
    print(f"\n✅ Internshala done — {count} applications")
    return count
