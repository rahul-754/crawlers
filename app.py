import sys
import asyncio
import re
import os
from bs4 import BeautifulSoup
from pymongo import MongoClient
import pandas as pd
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy
from urllib.parse import urlparse
from playwright.async_api import async_playwright

import asyncio
import random
import math

# --------------------------------------------------
# UTF-8 Console Setup (for special characters)
# --------------------------------------------------
if sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

# --------------------------------------------------
# MongoDB Setup
# --------------------------------------------------

# --------------------------------------------------
# Read Input URLs
# --------------------------------------------------
def read_urls(file_path):
    df = pd.read_excel(file_path) if file_path.endswith(".xlsx") else pd.read_csv(file_path)
    return df["url"].dropna().tolist()

# --------------------------------------------------
# ✳️ PRACTO Extraction Function
# --------------------------------------------------
def extract_structured_data_from_practo(html, url):
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def safe_select(*selectors):
        """Try multiple selectors, return first match text or 'NA'."""
        for selector in selectors:
            el = soup.select_one(selector)
            if el and el.get_text(strip=True):
                return el.get_text(strip=True)
        return "NA"

    def safe_select_all(*selectors):
        """Join all matching text from multiple selectors."""
        results = []
        for selector in selectors:
            for el in soup.select(selector):
                txt = el.get_text(strip=True)
                if txt:
                    results.append(txt)
        return ", ".join(results) if results else "NA"

    # Core fields with fallbacks
    data["name"] = safe_select("h1", "#container h1")
    data["clinic_name"] = safe_select("div.c-profile__clinic__name h2 a", ".c-profile--clinic__name")
    data["education"] = safe_select("#education p", "div.info-section p")
    data["experience"] = safe_select("#experience h2", "div.info-section h2")

    data["speciality"] = safe_select(
        ".u-d-inline-flex",
        "#container div span > h2",
        "#container div span",
        ".c-profile--doctor__speciality"
    )

    data["address"] = safe_select(
        "div.c-profile--clinic__address",
        "div p.address",
        ".c-profile--clinic__address"
    )

    data["mci"] = safe_select("#registrations .pure-u-1", "#registrations div")
    data["passing_year"] = safe_select("#education span span")
    data["memberships"] = safe_select_all("#memberships .p-entity--list")

    data["fees"] = safe_select(
        "[data-qa-id='consultation_fee']",
        "#container div:nth-of-type(3) > div",
        ".c-profile--clinic__fee"
    )

    data["timing"] = safe_select(
        "[data-qa-id='timings_list']",
        "div.u-cushion--left"
    )

    data["awards"] = safe_select_all("#awards\\ and\\ recognitions .pure-u-1")
    data["specializations"] = safe_select_all("#specializations .pure-u-1")
    data["full_education"] = safe_select_all("#education .pure-u-1")
    data["full_experience"] = safe_select_all("#experience .pure-u-1")
    data["registrations"] = safe_select_all("#registrations .pure-u-1")
    data["services"] = safe_select_all("#services .pure-u-1-3")

    # ✅ Multiple clinics like Selenium version
    clinics = soup.select(".c-profile--clinic--item")
    seen_clinics = set()
    index = 1
    for clinic in clinics:
        cname_el = clinic.select_one(".c-profile--clinic__name")
        cname_text = cname_el.get_text(strip=True) if cname_el else "NA"

        if cname_text in seen_clinics:
            continue
        seen_clinics.add(cname_text)

        data[f"clinic__name{index}"] = cname_text
        data[f"address{index}"] = clinic.select_one(".c-profile--clinic__address").get_text(strip=True) if clinic.select_one(".c-profile--clinic__address") else "NA"
        data[f"timing{index}"] = clinic.select_one(".u-cushion--left").get_text(strip=True) if clinic.select_one(".u-cushion--left") else "NA"
        data[f"fee{index}"] = clinic.select_one("[data-qa-id='consultation_fee']").get_text(strip=True) if clinic.select_one("[data-qa-id='consultation_fee']") else "NA"

        index += 1

    return data

def extract_structured_data_from_quickerala(html, url):
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def safe_xpath(soup, selector):
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else "NA"

    try:
        data['name'] = soup.select_one("div.c-left h2").get_text(strip=True)
    except:
        data['name'] = "NA"

    try:
        data['education'] = soup.select_one("div.c-left div > span").get_text(strip=True)
    except:
        data['education'] = "NA"

    try:
        data['speciality'] = soup.select_one("div.c-left span").get_text(strip=True)
    except:
        data['speciality'] = "NA"

    try:
        data['clinic_name'] = soup.select_one("div.c-right h4").get_text(strip=True)
    except:
        data['clinic_name'] = "NA"

    try:
        data['address'] = soup.select_one("div.c-right p").get_text(strip=True)
    except:
        data['address'] = "NA"

    return data


def extract_structured_data_from_patakare(html, url):
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def safe(selector):
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else "NA"

    data['name'] = safe("div.container > div.row > div > div h1")
    data['phone'] = safe("div.row > div > div > div > div > p:nth-of-type(2)")
    data['speciality'] = safe("div.container > div.row > div > div p:nth-of-type(2)")
    data['email'] = safe("div.row > div > div > div > div > p:nth-of-type(2) a")
    data['address'] = safe("div.container > div.row > div > div p:nth-of-type(5)")

    return data


def extract_structured_data_from_drlogy(html, url):
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def get_text(sel, default="NA"):
        el = soup.select_one(sel)
        return el.get_text(strip=True) if el else default

    data["name"] = get_text(".hph1")

    hph2 = soup.select(".hph2")
    data["education"] = hph2[0].get_text(strip=True) if len(hph2) > 0 else "NA"
    data["speciality"] = hph2[1].get_text(strip=True) if len(hph2) > 1 else "NA"

    data["experience"] = get_text(".hpd-v")
    data["Services Provided"] = get_text(".hpd-v1")

    sections = soup.select(".dtls-pra h4")
    data_blocks = soup.select(".dtls-pra ul")
    for i, section in enumerate(sections):
        label = section.get_text(strip=True).lower()
        value = data_blocks[i].get_text(strip=True) if i < len(data_blocks) else "NA"
        if "registration" in label:
            data["mci"] = value
        elif "education" in label:
            data["full_education"] = value
        elif "language" in label:
            data["Languages spoken"] = value
        elif "services" in label:
            data["services"] = value
        elif "specialization" in label:
            data["specializations"] = value

    fallback_sections = soup.select(".hph-2.view-all-par")
    for sec in fallback_sections:
        try:
            label = sec.select_one("h2").get_text(strip=True).lower()
            values = [li.get_text(strip=True) for li in sec.select("ul li")]
            value = ", ".join(values) if values else "NA"
            if "registration" in label:
                data["mci"] = value
            elif "education" in label:
                data["full_education"] = value
            elif "language" in label:
                data["Languages spoken"] = value
            elif "services" in label:
                data["services"] = value
            elif "specialization" in label:
                data["specializations"] = value
        except:
            continue

    data["clinic_name"] = get_text(".dr-hp .hp-h-2")
    data["address"] = get_text(".dr-hp .pc-docs-adress")

    try:
        fee_section = soup.select_one(".dr-hp .dr-fee")
        p_tags = fee_section.select("p") if fee_section else []
        data["timing"] = ", ".join(p.get_text(strip=True) for p in p_tags)
    except:
        data["timing"] = "NA"

    try:
        fee_section = soup.select_one(".dr-hp .dr-tim")
        p_tags = fee_section.select("p") if fee_section else []
        data["fees"] = ", ".join(p.get_text(strip=True) for p in p_tags)
    except:
        data["fees"] = "NA"

    return data

def extract_structured_data_from_drdata(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def safe(row_idx):
        try:
            return soup.select_one(
                f"div:nth-of-type(3) > div > div:nth-of-type(1) > table > tbody > tr:nth-of-type({row_idx}) > td:nth-of-type(2)"
            ).get_text(strip=True)
        except:
            return "NA"

    data["name"] = safe(1)
    data["speciality"] = safe(2)
    data["education"] = safe(3)
    data["address"] = safe(6)

    return data

def extract_structured_data_from_healthfrog(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def safe(selector):
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else "NA"

    data["name"] = safe("div.col-sm-9 > div:nth-of-type(1)")
    data["address"] = safe("div.col-sm-9 > div:nth-of-type(2) div p:nth-of-type(1)")
    data["phone"] = safe("div.col-sm-9 > div:nth-of-type(2) div p:nth-of-type(3)")

    return data


def extract_structured_data_from_ask4healthcare(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def safe(selector):
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else "NA"

    data["name"] = safe("div.docdetailhead h1")
    data["speciality"] = safe("div#ContentPlaceHolder1_divSpec span")
    data["address"] = safe("div#ContentPlaceHolder1_divHosAdd span")
    data["phone"] = safe("#ContentPlaceHolder1_loginHyperlink")

    try:
        clinic_raw = soup.select_one("#ContentPlaceHolder1_pnlVistingDetail p")
        data["clinic_name"] = clinic_raw.get_text(strip=True).split("Hospital Name")[-1].strip() if clinic_raw else "NA"
    except:
        data["clinic_name"] = "NA"

    return data

def extract_structured_data_from_apollo247(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def safe_text(cls):
        el = soup.select_one(f".{cls}")
        return el.get_text(strip=True) if el else "NA"

    def safe_list_text(cls, tag="p"):
        el = soup.select(f".{cls} {tag}")
        return ", ".join(e.get_text(strip=True) for e in el) if el else "NA"

    data["name"] = safe_text("DoctorProfileCard_doctorName__MIyRL")
    data["speciality"] = safe_text("DoctorProfileCard_specialty__NqwMO")
    data["experience"] = safe_text("DoctorProfileCard_experience__Sc9lA")
    data["Languages spoken"] = safe_text("DoctorProfileCard_languages__quMKs")
    data["clinic_name"] = safe_text("DoctorProfileCard_value__Dl2aa")
    data["address"] = safe_text("DoctorProfileCard_address__9LhAg")

    data["mci"] = safe_list_text("Sections_registration__efQuF")
    data["full_education"] = safe_list_text("Sections_education__F_ZfH")
    data["services"] = safe_list_text("Sections_conditions__WlGKt", tag="li")
    data["fee"] = safe_list_text("slots_heading__1iC9I")
    
    try:
        timing = soup.select_one(".slots_availabilityText__qX8fg")
        data["timing"] = timing.get_text(strip=True) if timing else "NA"
    except:
        data["timing"] = "NA"

    return data

def extract_structured_data_from_curofy(html, url):
    data = {"source_url": url}
    try:
        text = html

        def extract(key, offset):
            try:
                pos = text.index(key) + offset
                val = ""
                while pos < len(text) and text[pos] != '"':
                    val += text[pos]
                    pos += 1
                return val
            except:
                return "NA"

        data["mci"] = extract("mci_reg_no", 13)
        data["phone"] = extract("mob_no", 9)
        data["name"] = extract("display_name", 15)
        data["speciality"] = extract("specialty_name", 17)
        data["alternate_email"] = extract("alternate_username", 21) + "@gmail.com"
        data["email"] = extract('email":', 8)
        data["address"] = extract('clinic_address":', 17)
        data["locality"] = extract('locality":', 11)
        data["education"] = extract('degrees":', 10)

    except Exception as e:
        print("Error extracting curofy:", e)
    return data



def extract_structured_data_from_deldure(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def by_id(id_):
        el = soup.select_one(f"#{id_}")
        return el.get_text(strip=True) if el else "NA"

    data["name"] = by_id("vendorDisplayName")
    data["address"] = by_id("vendorAddress")
    data["city"] = by_id("vendorCity")
    data["pincode"] = by_id("vendorZip")
    data["state"] = by_id("vendorState")

    return data


def extract_structured_data_from_credihealth(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def by_xpath(xpath):
        try:
            return soup.select_one(xpath).get_text(strip=True)
        except:
            return "NA"

    def safe(css):
        el = soup.select_one(css)
        return el.get_text(strip=True) if el else "NA"

    data["name"] = safe("h1 a")
    data["clinic_name"] = safe("div.right-box h3")
    data["education"] = safe("div.left-box p:nth-of-type(1)")
    data["experience"] = safe("div.left-box p:nth-of-type(3) span:nth-of-type(1)")
    data["speciality"] = safe("div.left-box p:nth-of-type(2)")
    data["mci"] = safe("div[app-doctor-profile] div:nth-of-type(4) div:nth-of-type(2)")
    data["passing_year"] = safe("div[app-doctor-profile] div:nth-of-type(2) ul li")
    data["address"] = safe("div.right-box p")

    return data

def extract_structured_data_from_doctor360(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def safe_xpath(css):
        el = soup.select_one(css)
        return el.get_text(strip=True) if el else "NA"

    data["name"] = safe_xpath("div.doctor-content h1")
    data["clinic_name"] = safe_xpath("div.doctor-details span:nth-of-type(1)")
    data["speciality"] = safe_xpath("div.doctor-content h3")
    data["address"] = safe_xpath("div.doctor-details span:nth-of-type(2)")

    return data

def extract_structured_data_from_bajajfinservhealth(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def sel(attr_val):
        return soup.select_one(f"[data-testid='{attr_val}']")

    data["name"] = sel("wmTYMydGLESpVZTRZYriSG").text.strip() if sel("wmTYMydGLESpVZTRZYriSG") else "NA"
    data["speciality"] = sel("b8oG35eArfUfRUtEe25efT").text.strip() if sel("b8oG35eArfUfRUtEe25efT") else "NA"
    data["experience"] = sel("pPQm89BqCeScC2c7Y7Bonn").text.strip() if sel("pPQm89BqCeScC2c7Y7Bonn") else "NA"
    data["education"] = sel("rcx1zDjU2z543uHnF6xmih").text.strip() if sel("rcx1zDjU2z543uHnF6xmih") else "NA"
    data["clinic_name"] = sel("doctor-clinic-name").text.strip() if sel("doctor-clinic-name") else "NA"
    data["address"] = sel("doctor-clinic-address").text.strip() if sel("doctor-clinic-address") else "NA"

    # Languages, MCI, Education (from data block)
    try:
        extra_items = soup.select("span.e-css-16h2hts-body-1[data-testid='aV5x7gMm1FoLYexyuAX3Qt']")
        if len(extra_items) > 0:
            data["Languages spoken"] = extra_items[0].text.strip()
        if len(extra_items) > 1:
            data["mci"] = extra_items[1].text.strip()
        if len(extra_items) > 2:
            data["full_education"] = extra_items[2].text.strip()
    except:
        pass

    return data

def extract_structured_data_from_doctoriduniya(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def safe(css):
        el = soup.select_one(css)
        return el.get_text(strip=True) if el else "NA"

    data["name"] = safe("div.d-name h1")
    data["clinic_name"] = safe("div.hos-det h2 a")
    data["education"] = safe("div.d-name span:nth-of-type(1)")
    data["speciality"] = safe("div.d-name span:nth-of-type(2)")
    addr_block = soup.select_one("div.hos-det")
    data["address"] = addr_block.get_text(separator=", ", strip=True) if addr_block else "NA"

    return data

def extract_structured_data_from_askadoctor24x7(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def get_text(css):  # safe extractor
        el = soup.select_one(css)
        return el.get_text(strip=True) if el else "NA"

    data["name"] = get_text("#wholeBodyDiv h1 span:nth-of-type(1)")
    data["speciality"] = get_text("#wholeBodyDiv h1 span:nth-of-type(3)")
    data["experience"] = get_text(".DoctorProfileCard_experience__Sc9lA")
    data["address"] = get_text("#wholeBodyDiv h1 div")

    fields = {
        "Specialty": "specialty",
        "State Licence Number/NPI": "mci",
        "State/Board registered with": "mci state",
        "Specific Skills and Interests": "skills",
        "Years of experience": "experience"
    }

    for row in soup.select("#professionalDetailsDiv .row"):
        label = row.select_one(".label")
        value = row.select_one(".value")
        if label and value and label.text.strip() in fields:
            data[fields[label.text.strip()]] = value.text.strip()

    for key in fields.values():
        data.setdefault(key, "NA")

    contact_fields = {
        "City": "city",
        "State": "state",
        "Country": "country"
    }

    for row in soup.select("#contactDetailsDiv .row"):
        label = row.select_one(".label")
        value = row.select_one(".value")
        if label and value and label.text.strip() in contact_fields:
            data[contact_fields[label.text.strip()]] = value.text.strip()

    for key in contact_fields.values():
        data.setdefault(key, "NA")

    loc_fields = {
        "Location": "address",
        "Timing": "timing"
    }

    for row in soup.select("#officeDetailDiv .row"):
        label = row.select_one(".label")
        value = row.select_one(".value")
        if label and value and label.text.strip() in loc_fields:
            data[loc_fields[label.text.strip()]] = value.text.strip()

    for key in loc_fields.values():
        data.setdefault(key, "NA")

    prof_fields = {
        "Honors/Awards": "awards",
        "Affiliations": "memberships",
        "Other professional achievements": "achievements"
    }

    for row in soup.select("#otherDetailsDiv .row"):
        label = row.select_one(".label")
        value = row.select_one(".value")
        if label and value and label.text.strip() in prof_fields:
            data[prof_fields[label.text.strip()]] = value.text.strip()

    for key in prof_fields.values():
        data.setdefault(key, "NA")

    try:
        edu_rows = soup.select("#graduationDetailsDiv .row")
        education = []
        for row in edu_rows:
            label = row.select_one(".label")
            value = row.select_one(".value")
            if label and value:
                education.append(f"{label.text.strip()}: {value.text.strip().replace('\xa0', '')}")
        data["full_education"] = ", ".join(education) if education else "NA"
    except:
        data["full_education"] = "NA"

    return data

def extract_structured_data_from_prescripson(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def get(css):
        el = soup.select_one(css)
        return el.get_text(strip=True).replace(".", "") if el else "NA"

    data["name"] = get("div[data-testid='doctor-profile'] h1")
    data["education"] = get("div[data-testid='doctor-profile'] span:nth-of-type(2)")
    data["experience"] = get("div[data-testid='doctor-profile'] div:nth-of-type(3) p b")
    data["speciality"] = get("div[data-testid='doctor-profile'] span:nth-of-type(1)")
    data["mci"] = get("div[data-testid='doctor-profile'] div:nth-of-type(10) p")
    data["passing_year"] = get("div[data-testid='doctor-profile'] div:nth-of-type(11) ul li div:nth-of-type(2) p:nth-of-type(2)")

    return data
def extract_structured_data_from_justdialdds(html, url):
    from bs4 import BeautifulSoup
    from datetime import datetime
    import re

    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def safe(css):
        el = soup.select_one(css)
        return el.get_text(strip=True) if el else "NA"

    data["name"] = safe("div h1 span span")
    data["address"] = safe("ul li span span span span")
    data["phone"] = safe("#comp-contact span:nth-of-type(2)")

    # Try to extract year of start from string (e.g. "2008")
    try:
        year_text = soup.select_one("div ul li").get_text(strip=True)
        year_match = re.search(r"\b(19|20)\d{2}\b", year_text)
        if year_match:
            data["experience"] = datetime.now().year - int(year_match.group())
        else:
            data["experience"] = "NA"
    except:
        data["experience"] = "NA"

    return data


def extract_structured_data_from_ihindustan(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    try:
        name_edu = soup.select_one("#app-layout h3 a span").get_text(strip=True)
        parts = [p.strip() for p in name_edu.split(",")]
        data['name'] = parts[0] if len(parts) > 0 else "NA"
        data['education'] = parts[1] if len(parts) > 1 else "NA"
    except:
        data['name'] = data['education'] = "NA"

    def get_text(css):
        el = soup.select_one(css)
        return el.get_text(strip=True) if el else "NA"

    data['address'] = get_text("#app-layout > div:nth-of-type(3) > div:nth-of-type(3) > div:nth-of-type(1) > div > div:nth-of-type(2)")
    data['phone'] = get_text("#app-layout > div:nth-of-type(3) > div:nth-of-type(3) > div:nth-of-type(1) > div > p > a > span")

    return data


def extract_structured_data_from_lybrate(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def get_text(css):
        el = soup.select_one(css)
        return el.get_text(strip=True) if el else "NA"

    def get_list_items(xpath_heading_text):
        try:
            heading = soup.find("h3", string=xpath_heading_text)
            if heading:
                next_div = heading.find_next_sibling("div")
                if next_div:
                    return ", ".join(li.get_text(strip=True) for li in next_div.select("li")) or "NA"
        except:
            pass
        return "NA"

    data['name'] = get_text(".doctorCard_doctorName__profile__WWGCS")
    data['clinic_name'] = get_text("h3.css-13z06yo")
    data['address'] = get_text("div.css-0 > div:nth-of-type(1)")
    data['clinic_Locationn'] = get_text(".cliniclocation_locAddText__vN6U8")
    data['experience'] = get_text(".doctorCard_experience__AyNl1")
    data['doctor_location'] = get_text(".doctorCard_locality__profile__4Kjc7")
    data['education'] = get_text(".doctorCard_docDegree__eZgab.doctorCard_docDegree__profile__h6OPg")

    data['memberships'] = get_list_items("Professional Memberships")
    data['full_experience'] = get_list_items("Past Experience")
    data['full_education'] = get_list_items("Education")
    data['speciality'] = get_list_items("Speciality")
    data['specializations'] = get_list_items("Other treatment areas")
    data['Languages spoken'] = get_list_items("Languages spoken")
    data['fees'] = get_text(".doctorCard_cosmeticLogoWrapper__5t6em")

    # Multiple clinics
    clinics = soup.select(".clinicCard_cardContainer__2Sekg")
    for index, clinic in enumerate(clinics, start=1):
        name = clinic.select_one(".clinicCard_heading__A8cCn.clinicCard_heading__normal__11Zgs")
        address = clinic.select_one(".clinicCard_clinicAdd____rlg")
        fee = clinic.select_one(".clinicCard_text__HS137")
        timing = clinic.select_one(".clinicCard_timeContainer__LECv8")

        data[f'clinic__name{index}'] = name.get_text(strip=True) if name else "NA"
        data[f'address{index}'] = address.get_text(strip=True) if address else "NA"
        data[f'fee{index}'] = fee.get_text(strip=True) if fee else "NA"
        data[f'timing{index}'] = timing.get_text(strip=True) if timing else "NA"

    data['timing'] = data.get('timing1', "NA")

    return data

def extract_structured_data_from_sehat(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def safe(css):
        el = soup.select_one(css)
        return el.get_text(strip=True) if el else "NA"

    data["name"] = safe("#page-content-wrapper h1")
    data["clinic_name"] = safe("#practiceinfo h2 a").split(",")[0] if soup.select_one("#practiceinfo h2 a") else "NA"
    data["address"] = safe("#practiceinfo p span")
    data["education"] = safe("#page-content-wrapper p:nth-of-type(2)")
    data["experience"] = safe("#page-content-wrapper ul li span")
    data["speciality"] = safe("#overview ul li p")

    try:
        raw = soup.select_one("#overview ul")
        if raw:
            lines = raw.get_text(separator="\n", strip=True).split("\n")
            data["passing_year"] = ", ".join(
                f"{line.split(',')[0].strip()} - {line.split(',')[1].strip()}" for line in lines if "," in line
            )
        else:
            data["passing_year"] = "NA"
    except:
        data["passing_year"] = "NA"

    return data
def extract_structured_data_from_lazoi(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    try:
        full_name = soup.select_one("#str_docName").text.strip()
        if "(" in full_name:
            name, spec = full_name.split("(", 1)
            data["name"] = name.strip()
            data["speciality"] = spec.replace(")", "").strip()
        else:
            data["name"] = full_name
            data["speciality"] = "NA"
    except:
        data["name"] = "NA"
        data["speciality"] = "NA"

    def safe(css):
        el = soup.select_one(css)
        return el.get_text(strip=True) if el else "NA"

    data["clinic_name"] = safe("#div_locationWrapper h4")
    
    try:
        addr_element = soup.select_one("#div_locationWrapper div div div a[data-address]")
        data["address"] = addr_element['data-address'].strip() if addr_element else "NA"
    except:
        data["address"] = "NA"

    data["education"] = safe("ul li:nth-of-type(5) span")
    data["fees"] = safe("#div_locationWrapper li:nth-of-type(1)")
    data["timing"] = safe("#div_locationWrapper li:nth-of-type(2)")

    return data
def extract_structured_data_from_skedoc(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def safe(cls):
        el = soup.select_one(f".{cls}")
        return el.get_text(strip=True) if el else "NA"

    data["name"] = safe("drName")
    data["speciality"] = safe("specializ")
    data["address"] = safe("hospital_info")
    data["education"] = safe("qualif")
    data["timing"] = safe("timingsTable")
    data["fees"] = safe("col_orange")

    try:
        profile_tab = soup.select_one("#profileTab")
        def extract_list(header):
            try:
                section = profile_tab.find(text=header).find_parent("div").find_next_sibling("ul")
                items = [li.get_text(strip=True) for li in section.find_all("li")]
                return ", ".join(items) if items else "NA"
            except:
                return "NA"

        data["specializations"] = extract_list("Specializations")
        data["full_education"] = extract_list("Qualifications")
        data["full_experience"] = extract_list("Experiences")
        data["Expertises"] = extract_list("Expertises")
        data["mci"] = extract_list("Registrations")
        data["clinic_name"] = profile_tab.select_one("#headingOne0 button div h3").get_text(strip=True) if profile_tab.select_one("#headingOne0 button div h3") else "NA"
    except:
        data["specializations"] = data["full_education"] = data["full_experience"] = data["Expertises"] = data["mci"] = data["clinic_name"] = "NA"

    return data

def extract_structured_data_from_mymedisage(html, url):
    from bs4 import BeautifulSoup
    data = {"source_url": url}
    soup = BeautifulSoup(html, "html.parser")

    def safe(css):
        el = soup.select_one(css)
        return el.get_text(strip=True) if el else "NA"

    data["name"] = safe("div > div > div > div:nth-of-type(3) p")
    data["speciality"] = safe("div > div > div > div:nth-of-type(3) a p")
    data["adress"] = safe("div > div > div > div:nth-of-type(3) span")

    # Education
    try:
        section = soup.find("p", string="Academics")
        li_elements = section.find_next("div").find_all("li") if section else []
        data["education"] = ", ".join(li.get_text(strip=True) for li in li_elements) if li_elements else "NA"
        data["full_education"] = data["education"]
    except:
        data["education"] = data["full_education"] = "NA"

    # Experience
    try:
        section = soup.find("p", string="Experience")
        li_elements = section.find_next("div").find_all("li") if section else []
        data["full_experience"] = ", ".join(li.get_text(strip=True) for li in li_elements) if li_elements else "NA"
    except:
        data["full_experience"] = "NA"

    # Publications
    try:
        section = soup.find("p", string="Research & Publications")
        lis = section.find_next("ul").find_all("li")
        publications = []
        for li in lis:
            title = li.find("p", class_="font-medium")
            desc = li.find("p", class_="text-gray-500 break-words")
            date_tag = li.find("span", string=lambda s: s and "Published on" in s)
            publications.append({
                "title": title.text.strip() if title else "",
                "description": desc.text.strip() if desc else "",
                "published_date": date_tag.parent.text.replace("Published on", "").strip() if date_tag else ""
            })
        data["Publications"] = publications if publications else "NA"
    except:
        data["Publications"] = "NA"

    return data
def extract_structured_data_from_medibuddy(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def safe(css):
        el = soup.select_one(css)
        return el.get_text(strip=True) if el else "NA"

    data["name"] = safe("h1.text-2xl.font-bold")
    data["speciality"] = soup.select_one('[name="DOCTOR_PROFILE_PAGE_SPECIALITY_BUTTON"]')
    data["speciality"] = data["speciality"].get_text(strip=True) if data["speciality"] else "NA"
    data["address"] = safe("p.text-base.leading-5")
    data["education"] = safe("h2.text-base.font-bold")
    data["fees"] = safe("div.self-center.flex.grow h3.text-lg.font-bold")
    data["experience"] = safe("h3.text-lg.font-bold.leading-6")
    data["Languages spoken"] = safe("h3.text-sm.font-bold")
    data["clinic_name"] = safe("div h3")  # Simplified selector assuming uniqueness

    return data
def extract_structured_data_from_myupchar(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    try:
        docbox = soup.select_one(".doc_min_info")
        spans = docbox.find_all("span")

        data["name"] = docbox.find("h1").text.strip() if docbox.find("h1") else "NA"
        data["speciality"] = spans[1].text.strip() if len(spans) > 1 else "NA"
        data["education"] = spans[2].text.strip() if len(spans) > 2 else "NA"
        data["experience"] = spans[3].text.strip() if len(spans) > 3 else "NA"
    except:
        data["name"] = data["speciality"] = data["education"] = data["experience"] = "NA"

    def get_list_text_by_id(id_):
        try:
            el = soup.find(id=id_)
            return ", ".join(item.text.strip() for item in el.find_all("li")) if el else "NA"
        except:
            return "NA"

    data["fees"] = soup.select_one(".clinic-fee")
    data["fees"] = data["fees"].text.split(":")[1].strip() if data["fees"] else "NA"

    try:
        clinic = soup.select_one(".clinic-address")
        data["clinic_name"] = clinic.find("strong").text.strip() if clinic else "NA"
        data["address"] = clinic.text.strip() if clinic else "NA"
    except:
        data["clinic_name"] = data["address"] = "NA"

    try:
        li_tags = soup.select("#doctor-clinic li")
        data["timing"] = li_tags[1].text.strip() if len(li_tags) > 1 else "NA"
    except:
        data["timing"] = "NA"

    data["services"] = get_list_text_by_id("doctor-services")
    data["memberships"] = get_list_text_by_id("doctor-memberships")
    data["full_experience"] = get_list_text_by_id("doctor-experience")
    data["full_education"] = get_list_text_by_id("doctor-qualifications")

    try:
        specs = soup.select("#doctor-specialties a")
        data["specializations"] = ", ".join(a.text.strip() for a in specs) if specs else "NA"
    except:
        data["specializations"] = "NA"

    return data

def extract_structured_data_from_healthworldhospitals(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def safe_text(selector):
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else "NA"

    data["name"] = safe_text("section:nth-of-type(1) h1")

    # Degrees and Specialities
    try:
        section = soup.select_one("section:nth-of-type(2) div.col-md-8")
        paragraphs = section.find_all("p")
        for p in paragraphs:
            text = p.text.strip()
            if "Degrees:" in text:
                data["education"] = text.replace("Degrees:", "").strip()
            if "Specialities:" in text:
                data["speciality"] = text.replace("Specialities:", "").strip()
    except:
        data["education"] = data["speciality"] = "NA"

    # Memberships
    try:
        lis = soup.select("section:nth-of-type(2) div.col-md-8 ul li")
        data["memberships"] = ", ".join(li.get_text(strip=True) for li in lis)
    except:
        data["memberships"] = "NA"

    return data
def extract_structured_data_from_docindia(html, url):
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def by_id(id_):
        tag = soup.find(id=id_)
        return tag.get_text(strip=True) if tag else "NA"

    data["name"] = by_id("docName")
    data["speciality"] = by_id("docSpeciality")
    data["education"] = by_id("docTitle")

    # Clinic info
    clinic_name = soup.select_one(".clinic-name")
    data["clinic_name"] = clinic_name.get_text(strip=True) if clinic_name else "NA"

    address = soup.select_one(".clinic-direction")
    data["address"] = address.get_text(strip=True) if address else "NA"

    # Services
    try:
        services = soup.select("#ServicesOffered ul li")
        data["services"] = ", ".join(li.get_text(strip=True) for li in services) or "NA"
    except:
        data["services"] = "NA"

    def get_list(id_):
        try:
            lis = soup.select(f"#{id_} li")
            return ", ".join(li.get_text(strip=True) for li in lis) or "NA"
        except:
            return "NA"

    data["full_education"] = get_list("Education_list")
    data["specializations"] = get_list("Specializations_list")
    data["awards"] = get_list("Award_list")

    # Additional clinics
    clinics = soup.select(".location-list")
    for i, clinic in enumerate(clinics, start=1):
        try:
            name = clinic.select_one(".clinic-name")
            address = clinic.select_one(".clinic-direction")
            data[f"clinic__name{i}"] = name.get_text(strip=True) if name else "NA"
            data[f"address{i}"] = address.get_text(strip=True) if address else "NA"
        except:
            data[f"clinic__name{i}"] = "NA"
            data[f"address{i}"] = "NA"

    return data

def extract_structured_data_from_meddco(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    try:
        overview = soup.select_one(".dr_details")
        if overview:
            h2 = overview.find("h2")
            data["name"] = h2.get_text(strip=True) if h2 else "NA"

            qualifications = overview.select("h4[style='color: #1d1d1d;']")
            data["education"] = qualifications[0].get_text(strip=True) if len(qualifications) > 0 else "NA"
            data["speciality"] = qualifications[1].get_text(strip=True) if len(qualifications) > 1 else "NA"
        else:
            data["name"] = data["education"] = data["speciality"] = "NA"
    except:
        data["name"] = data["education"] = data["speciality"] = "NA"

    try:
        data["clinic_name"] = soup.select_one("#dr_list h2").get_text(strip=True)
    except:
        data["clinic_name"] = "NA"

    return data

def extract_structured_data_from_medindia(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    try:
        h1 = soup.select_one("#menu-top h1")
        if h1:
            header = h1.get_text(strip=True)
            name, specialty = header.split(",", 1)
            data["name"] = name.strip()
            data["speciality"] = specialty.strip()
        else:
            data["name"] = data["speciality"] = "NA"
    except:
        data["name"] = data["speciality"] = "NA"

    try:
        address = soup.select_one(".contactDetails p").get_text()
        data["address"] = address.split("Address:")[1].strip() if "Address:" in address else address.strip()
    except:
        data["address"] = "NA"

    try:
        phone = soup.select_one(".popup-contact-details").get_text(strip=True)
        data["phone"] = phone
    except:
        data["phone"] = "NA"

    return data


def extract_structured_data_from_healthgrades(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    try:
        header = soup.select_one("#summary-section h1")
        if header:
            name, education = header.get_text(strip=True).split(",", 1)
            data["name"] = name.strip()
            data["education"] = education.strip()
        else:
            data["name"] = data["education"] = "NA"
    except:
        data["name"] = data["education"] = "NA"

    try:
        data["speciality"] = soup.select_one(".summary-header-row-specialty").get_text(strip=True)
    except:
        data["speciality"] = "NA"

    try:
        data["address"] = soup.select_one(".location-row-address").get_text(strip=True)
    except:
        data["address"] = "NA"

    try:
        data["gender"] = soup.select_one("p span span:nth-of-type(2)").get_text(strip=True)
    except:
        data["gender"] = "NA"

    try:
        age_text = soup.select_one("p span span:nth-of-type(4)").get_text(strip=True)
        data["age"] = age_text.split("Age")[1].strip() if "Age" in age_text else age_text
    except:
        data["age"] = "NA"

    try:
        data["phone"] = soup.select_one(".summary-standard-phone-link").get_text(strip=True)
    except:
        data["phone"] = "NA"

    return data
def extract_structured_data_from_clinicspots(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def get_text(selector, default="NA"):
        try:
            return soup.select_one(selector).get_text(strip=True)
        except:
            return default

    data["name"] = get_text("h1.text-primary span.mr-2")
    data["speciality"] = get_text("h2.font-semibold")
    data["experience"] = get_text("h2:contains('years of experience')", "")

    try:
        clinic = soup.select_one("li.p-3.border-b")
        data["clinic_name"] = clinic.select_one("a").get_text(strip=True)
        area = clinic.select("div")[0].get_text(strip=True)
        address = clinic.select("div")[1].get_text(strip=True)
        city = clinic.select("div")[2].get_text(strip=True)
        data["address"] = address + ", " + area + ", " + city
    except:
        data["clinic_name"] = "NA"
        data["address"] = "NA"

    def get_list_text(selector):
        try:
            return ", ".join([li.get_text(strip=True) for li in soup.select(f"{selector} li")])
        except:
            return "NA"

    data["specializations"] = get_list_text("#specializations")
    data["full_education"] = get_list_text("#educations")
    data["full_experience"] = get_list_text("section[aria-label='Experience'] ul")
    data["mci"] = get_list_text("section[aria-label='MCI'] ul")
    data["memberships"] = get_list_text("#memberships")
    data["services"] = get_list_text("#services")

    return data


def extract_structured_data_from_hexahealth(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def get_text(selector, default="NA"):
        try:
            return soup.select_one(selector).get_text(strip=True)
        except:
            return default

    data["name"] = get_text(".doctorOverview h1")
    data["speciality"] = get_text(".doctorOverview .specialization")
    data["education"] = get_text(".doctorOverview .education")
    data["timing"] = get_text(".available-timing-card")
    data["experience"] = get_text(".experienceSection")

    # Clinic name and address
    try:
        clinic = soup.select_one(".DoctorCard")
        data["clinic_name"] = clinic.select_one("p").get_text(strip=True)
        spans = clinic.select("span")
        if len(spans) > 3:
            address = spans[3].get_text(strip=True)
            data["address"] = f"{data['clinic_name']}, {address}"
        else:
            data["address"] = "NA"
    except:
        data["clinic_name"] = "NA"
        data["address"] = "NA"

    # Helper for list items from <ul><li>
    def extract_list_from_ul(section_id):
        try:
            section = soup.select_one(f"#{section_id}")
            return ", ".join([li.get_text(strip=True) for li in section.select("li")])
        except:
            return "NA"

    data["specializations"] = extract_list_from_ul("specializations")
    data["mci"] = extract_list_from_ul("registration-details")
    data["memberships"] = extract_list_from_ul("memberships")
    data["full_experience"] = extract_list_from_ul("work-experience")
    data["full_education"] = extract_list_from_ul("education-&-achievements")
    data["Treatments"] = extract_list_from_ul("treatments")

    # Multiple clinics
    try:
        clinics = soup.select(".DoctorCard")
        seen = set()
        for idx, clinic in enumerate(clinics, start=1):
            try:
                clinic_name = clinic.select_one("p").get_text(strip=True)
            except:
                clinic_name = "NA"

            if clinic_name in seen:
                continue
            seen.add(clinic_name)
            data[f"clinic__name{idx}"] = clinic_name

            try:
                spans = clinic.select("span")
                if len(spans) > 3:
                    addr = spans[3].get_text(strip=True)
                    data[f"address{idx}"] = f"{clinic_name}, {addr}"
                else:
                    data[f"address{idx}"] = "NA"
            except:
                data[f"address{idx}"] = "NA"
    except:
        pass

    return data


def extract_structured_data_from_mymedisage(html, url):
    soup = BeautifulSoup(html, 'html.parser')
    data = {'source_url': url}

    def safe_text(element):
        return element.get_text(strip=True) if element else 'NA'

    # Name
    name_el = soup.select_one("p.font-semibold.text-black.text-base.line-clamp-2")
    data['name'] = safe_text(name_el)

    # Speciality
    speciality_el = soup.select_one("a.text-sm.py-1.text-black p.flex.items-start")
    data['speciality'] = safe_text(speciality_el)

    # Address
    address_el = soup.select_one("span.text-sm.pb-1.my-1.text-black.flex.items-start")
    data['address'] = safe_text(address_el)

    return data




def extract_structured_data_from_kivihealth(html, url):
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def safe_select(selector):
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else "NA"

    def safe_select_all(selector):
        return ", ".join(el.get_text(strip=True) for el in soup.select(selector)) or "NA"

    # Mapping your snippet to your master structure
    data["name"] = safe_select("h1.doctor-name")
    data["clinic_name"] = safe_select("p.clinics-title")
    data["education"] = "NA"  # Not available in given HTML snippet
    data["experience"] = safe_select("p.text-muted.mb-2")  # Contains speciality + years
    data["speciality"] = safe_select("p.text-muted.mb-2")  # Same as above
    data["address"] = safe_select("p.clinics-title + p.m-0.text-sm")
    data["mci"] = "NA"
    data["passing_year"] = "NA"
    data["memberships"] = "NA"
    data["fees"] = "NA"  # Not in snippet, can be updated if available
    data["timing"] = safe_select_all("div.flex-start.mt-2.text-sm div p.m-0")
    data["awards"] = "NA"
    data["specializations"] = "NA"
    data["full_education"] = "NA"
    data["full_experience"] = "NA"
    data["registrations"] = "NA"
    data["services"] = "NA"

    # Multiple clinic support (Kivihealth profile usually has 1 clinic, but we'll follow same pattern)
    clinics = soup.select("p.clinics-title")
    seen_clinics = set()
    index = 1
    for clinic in clinics:
        try:
            cname_text = clinic.get_text(strip=True) if clinic else "NA"
            if cname_text in seen_clinics:
                continue
            seen_clinics.add(cname_text)

            data[f"clinic__name{index}"] = cname_text
            # Address immediately after clinic name
            address_el = clinic.find_next("p", class_="m-0 text-sm")
            data[f"address{index}"] = address_el.get_text(strip=True) if address_el else "NA"

            # Timings (all p.m-0 under timings div)
            timing_div = soup.select("div.flex-start.mt-2.text-sm div p.m-0")
            data[f"timing{index}"] = ", ".join(t.get_text(strip=True) for t in timing_div) if timing_div else "NA"

            # Phone
            phone_el = soup.select_one("a[href^='tel:']")
            data[f"fee{index}"] = phone_el.get_text(strip=True) if phone_el else "NA"  # Keeping key name as in master code
            index += 1
        except:
            continue

    return data


async def extract_structured_data_from_babymhospital(page, url):
    # Click all accordion sections before scraping
    sections_to_click = ["Qualification", "Work Experience", "Research", "Publications", "Awards"]
    for section in sections_to_click:
        try:
            await page.locator(f"text={section}").click()
            await page.wait_for_timeout(1000)
        except Exception as e:
            print(f"Could not click {section}: {e}")

    # Get updated HTML after clicks
    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    root = soup.select_one("div.container > div.tab_container")

    def safe_select(selector):
        if root:
            el = root.select_one(selector)
        else:
            el = soup.select_one(selector)
        return el.get_text(strip=True) if el else "NA"

    def safe_select_all(selector):
        if root:
            elements = root.select(selector)
        else:
            elements = soup.select(selector)
        return ", ".join(el.get_text(strip=True) for el in elements) if elements else "NA"

    # Matching Practo schema column names
    data["name"] = safe_select("#tab1 > div.tat-det > h5")
    data["clinic_name"] = "Baby Memorial Hospital"
    data["education"] = safe_select("div.item:has(a:contains('Qualification')) div.inner")
    data["experience"] = safe_select("div.item:has(a:contains('Work Experience')) div.inner")
    data["speciality"] = safe_select("#tab1 > div.tat-det > p.dr-postion:nth-of-type(1)")
    data["address"] = safe_select("div.address-block p")
    data["mci"] = "NA"
    data["passing_year"] = "NA"
    data["memberships"] = "NA"
    data["fees"] = "NA"
    data["timing"] = safe_select("div.doc-img > div.opening-times > ul")
    data["awards"] = safe_select_all("div.item:has(a:contains('Awards')) div.inner")
    data["specializations"] = data["speciality"]
    data["full_education"] = data["education"]
    data["full_experience"] = data["experience"]
    data["registrations"] = "NA"
    data["services"] = "NA"

    # Multiple clinic support
    data["clinic__name1"] = data["clinic_name"]
    data["address1"] = data["address"]
    data["timing1"] = data["timing"]
    data["fee1"] = data["fees"]

    return data

async def extract_structured_data_from_arogyamithra(html, url):
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    root = soup.select_one("div.main-wrapper > div.content")

    def safe_select(selector):
        if root:
            el = root.select_one(selector)
        else:
            el = soup.select_one(selector)
        return el.get_text(strip=True) if el else "NA"

    def safe_select_all(selector):
        if root:
            elements = root.select(selector)
        else:
            elements = soup.select(selector)
        return ", ".join(el.get_text(strip=True) for el in elements) if elements else "NA"

    # Match Practo schema column names
    data["name"] = safe_select("div.doc-info-left > div.doc-info-cont > h4.doc-name")
    data["clinic_name"] = safe_select("a.name")
    data["education"] = safe_select("div.widget.education-widget > div.experience-box > ul.experience-list")
    data["experience"] = "NA"  # Not explicitly given on this site
    data["speciality"] = safe_select("div.doc-info-left > div.doc-info-cont > p.doc-department")
    data["address"] = safe_select("div.doc-info-cont > div.clinic-details > p.doc-location")
    data["mci"] = "NA"
    data["passing_year"] = "NA"
    data["memberships"] = "NA"
    data["fees"] = safe_select("div.clini-infos > ul > li:nth-of-type(4)")
    data["timing"] = "NA"  # Not explicitly given
    data["awards"] = "NA"
    data["specializations"] = data["speciality"]
    data["full_education"] = data["education"]
    data["full_experience"] = "NA"
    data["registrations"] = "NA"
    data["services"] = safe_select_all("ul.clearfix")

    # Multiple clinic support (only one here)
    data["clinic__name1"] = data["clinic_name"]
    data["address1"] = data["address"]
    data["timing1"] = data["timing"]
    data["fee1"] = data["fees"]

    return data

async def extract_structured_data_from_medicover(html, url):
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    root = soup.select_one("div.container > div")

    def safe_select(selector):
        if root:
            el = root.select_one(selector)
        else:
            el = soup.select_one(selector)
        return el.get_text(strip=True) if el else "NA"

    def safe_select_all(selector):
        if root:
            elements = root.select(selector)
        else:
            elements = soup.select(selector)
        return ", ".join(el.get_text(strip=True) for el in elements) if elements else "NA"

    # Mapping JSON schema → Practo structure
    data["name"] = safe_select("div.col-xl-6.col-md-8 > div.doc-info-cont > h1")
    data["clinic_name"] = "Medicover Hospitals"
    data["education"] = safe_select("div.col-xl-6.col-md-8 > div.doc-info-cont > span")  # degree
    data["experience"] = safe_select("div.col-xl-6.col-md-8 > div.doc-info-cont > p.doc-experience:nth-of-type(3)")
    data["speciality"] = safe_select("div.col-xl-6.col-md-8 > div.doc-info-cont > p.doc-department:nth-of-type(2)")
    data["address"] = safe_select("p")
    data["mci"] = "NA"
    data["passing_year"] = "NA"
    data["memberships"] = "NA"
    data["fees"] = "NA"
    data["timing"] = safe_select("div.col-xl-6.col-md-8 > div.doc-info-cont > p.doc-department:nth-of-type(4)")
    data["awards"] = safe_select_all("#collapseThree > div.accordion-body.doc-p")
    data["specializations"] = safe_select("div.row > div.col-md-12.col-sm-12 > h2")  # expertise
    data["full_education"] = data["education"]
    data["full_experience"] = safe_select("div.accordion-body.doc-p > div.row > div.col-lg-12.col-md-12:nth-of-type(3)")
    data["registrations"] = "NA"
    data["services"] = "NA"

    # Multiple clinic structure (only one for Medicover profile)
    data["clinic__name1"] = data["clinic_name"]
    data["address1"] = data["address"]
    data["timing1"] = data["timing"]
    data["fee1"] = data["fees"]

    return data

async def extract_structured_data_from_maxhealthcare(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    root = soup.select_one("#content")

    def safe_select(selector):
        if root:
            el = root.select_one(selector)
        else:
            el = soup.select_one(selector)
        return el.get_text(strip=True) if el else "NA"

    def safe_select_all(selector):
        if root:
            elements = root.select(selector)
        else:
            elements = soup.select(selector)
        return ", ".join(el.get_text(strip=True) for el in elements) if elements else "NA"

    def safe_awards(selector):
        card = soup.select_one(selector)
        if card:
            award_items = card.select("li") + card.select("p")
            if award_items:
                return ", ".join([item.get_text(strip=True) for item in award_items if item.get_text(strip=True)])
            else:
                return card.get_text(strip=True)
        return "NA"

    # Practo schema mapping
    data["name"] = safe_select("div.d-lg-flex.pl-lg-70 > div.d-lg-flex.flex-column > h1.f-lg-36.f-17")
    data["clinic_name"] = "Max Healthcare"
    data["education"] = "NA"  # Not explicitly available
    data["experience"] = safe_select("div.d-lg-flex.pl-lg-70 > div.d-lg-flex.flex-column > p.color-white.l-h-12:nth-of-type(2)")
    data["speciality"] = safe_select("div.d-lg-flex.pl-lg-70 > div.d-lg-flex.flex-column > p.color-white.l-h-12:nth-of-type(1)")
    data["address"] = "NA"  # Could be from branch info if available
    data["mci"] = "NA"
    data["passing_year"] = "NA"
    data["memberships"] = "NA"
    data["fees"] = "NA"
    data["timing"] = safe_select("div.site-content.f-15 > ul > li:nth-of-type(1)")
    data["awards"] = safe_awards("#doctor-detail-accordion > div.bg-transparent.card:nth-of-type(2)")
    data["specializations"] = data["speciality"]
    data["full_education"] = data["education"]
    data["full_experience"] = data["experience"]
    data["registrations"] = "NA"
    data["services"] = "NA"

    # Multiple clinic format
    data["clinic__name1"] = data["clinic_name"]
    data["address1"] = data["address"]
    data["timing1"] = data["timing"]
    data["fee1"] = data["fees"]

    return data

async def extract_structured_data_from_babymhospital(page, url):
    # Click all accordion sections first
    sections_to_click = ["Qualification", "Work Experience", "Research", "Publications", "Awards"]
    for section in sections_to_click:
        try:
            await page.locator(f"text={section}").click()
            await page.wait_for_timeout(1000)
        except Exception as e:
            print(f"Could not click {section}: {e}")

    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    root = soup.select_one("div.container > div.tab_container")

    def safe(selector):
        if root:
            el = root.select_one(selector)
        else:
            el = soup.select_one(selector)
        return el.get_text(strip=True) if el else "NA"

    # Extract fields according to your schema
    data["name"] = safe("#tab1 > div.tat-det > h5")
    data["speciality"] = safe("#tab1 > div.tat-det > p.dr-postion:nth-of-type(1)")
    data["awards"] = safe("div.item:has(a:contains('Awards')) div.inner")
    data["publications"] = safe("div.item:has(a:contains('Publications')) div.inner")
    data["experience"] = safe("div.item:has(a:contains('Work Experience')) div.inner")
    data[""] = safe("div.item:has(a:contains('Qualification')) div.inner")
    data["research"] = safe("div.item:has(a:contains('Research')) div.inner")
    data["timings"] = safe("div.doc-img > div.opening-times > ul")

    return data



def extract_structured_data_from_manipalhospitals(html, url):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def safe_select(selector):
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else "NA"

    def safe_select_all(selector):
        return ", ".join(el.get_text(strip=True) for el in soup.select(selector)) or "NA"

    # Mapped fields for Manipal Hospitals
    data["name"] = safe_select("#doctor_name_fetch1755")
    data["speciality"] = safe_select("#desgn1755")
    data["degree"] = safe_select("tr > td > p.qualification-text")
    data["location"] = safe_select("div.section-heading > div.select-section.sec-flex > p.pic_box_text.dis-pro-box")

    # Optional fields for schema consistency with Practo
    data["clinic_name"] = "NA"
    data["education"] = "NA"
    data["experience"] = "NA"
    data["address"] = data["location"]
    data["mci"] = "NA"
    data["passing_year"] = "NA"
    data["memberships"] = "NA"
    data["fees"] = "NA"
    data["timing"] = "NA"
    data["awards"] = "NA"
    data["specializations"] = "NA"
    data["full_education"] = data["degree"]
    data["full_experience"] = "NA"
    data["registrations"] = "NA"
    data["services"] = "NA"

    # Multiple clinic support placeholder (structure match)
    clinics = soup.select(".c-profile--clinic--item")
    seen_clinics = set()
    index = 1
    for clinic in clinics:
        try:
            cname = clinic.select_one(".c-profile--clinic__name")
            cname_text = cname.get_text(strip=True) if cname else "NA"
            if cname_text in seen_clinics:
                continue
            seen_clinics.add(cname_text)

            data[f"clinic__name{index}"] = cname_text
            data[f"address{index}"] = clinic.select_one(".c-profile--clinic__address").get_text(strip=True) if clinic.select_one(".c-profile--clinic__address") else "NA"
            data[f"timing{index}"] = clinic.select_one(".u-cushion--left").get_text(strip=True) if clinic.select_one(".u-cushion--left") else "NA"
            data[f"fee{index}"] = clinic.select_one("[data-qa-id='consultation_fee']").get_text(strip=True) if clinic.select_one("[data-qa-id='consultation_fee']") else "NA"
            index += 1
        except:
            continue

    return data

from bs4 import BeautifulSoup

def extract_structured_data_from_mappls(html, url):
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def safe_select(selector):
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else "NA"

    def safe_select_all(selector):
        return ", ".join(el.get_text(strip=True) for el in soup.select(selector)) or "NA"

    # Main fields (mirroring Practo structure)
    data["clinic_name"] = safe_select("div.col-md-7.col-xs-7 > div.p-d-i-item > h2")
    data["address"] = safe_select("#mCSB_18_container > div.place-detail-sec p")
    data["email_id"] = safe_select("#mCSB_18_container > div.place-detail-sec p")
    data["phone_number"] = safe_select("#mCSB_18_container > div.place-detail-sec p")
    data["timings"] = safe_select("#mCSB_18_container > div.place-detail-sec p")
    data["specialisation"] = safe_select("#mCSB_18_container > div.place-detail-sec p")

    # These extra keys are added to keep structure similarity
    data["services"] = safe_select_all("#services .pure-u-1-3")
    data["awards"] = safe_select_all("#awards\\ and\\ recognitions .pure-u-1")
    data["memberships"] = safe_select_all("#memberships .p-entity--list")
    data["registrations"] = safe_select_all("#registrations .pure-u-1")

    # Multiple clinic support (same logic style as Practo)
    clinics = soup.select("div.col-md-7.col-xs-7 > div.p-d-i-item > h2")
    seen_clinics = set()
    index = 1
    for clinic in clinics:
        try:
            cname_text = clinic.get_text(strip=True) if clinic else "NA"
            if cname_text in seen_clinics:
                continue
            seen_clinics.add(cname_text)

            data[f"clinic_name{index}"] = cname_text
            address_el = clinic.find_parent().find_next("p")
            data[f"address{index}"] = address_el.get_text(strip=True) if address_el else "NA"
            index += 1
        except:
            continue

    return data

from bs4 import BeautifulSoup

def extract_structured_data_from_eka(html, url):
    soup = BeautifulSoup(html, "html.parser")
    data = {"source_url": url}

    def safe_select(selector):
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else "NA"

    def safe_select_all(selector):
        return ", ".join(el.get_text(strip=True) for el in soup.select(selector)) or "NA"

    # ===== Match Practo's Structure =====
    data["name"] = safe_select("div.flex.flex-col > h1.TitlesTitle3.text-text-primary")
    data["specialisation"] = safe_select("div.flex.items-center > span")  # Adjust if specialization selector differs
    data["experience"] = safe_select("div.flex.items-center > div.space-y-7.flex-1 > div:nth-of-type(1)")
    data["clinic_name"] = safe_select("a > div.hidden.items-center > div.tracking-1px")
    data["address"] = safe_select("div.space-y-8.flex > div.space-y-10.w-full > div.BodyCallout.text-text-secondary")
    data["location"] = safe_select("div.flex.items-center > div.space-y-7.flex-1 > div:nth-of-type(2)")

    # Phone numbers if available
    data["phone"] = safe_select_all("a[href^='tel:']")

    # Multiple Clinics Handling
    clinics = soup.select("a > div.hidden.items-center > div.tracking-1px")
    seen_clinics = set()
    index = 1
    for clinic in clinics:
        cname_text = clinic.get_text(strip=True)
        if cname_text in seen_clinics:
            continue
        seen_clinics.add(cname_text)

        data[f"clinic_name_{index}"] = cname_text
        address_el = clinic.find_parent().find_next("div", class_="BodyCallout text-text-secondary")
        data[f"address_{index}"] = address_el.get_text(strip=True) if address_el else "NA"
        index += 1

    # Timings (similar to Practo's "timings" field)
    data["timings"] = safe_select_all("div.flex.flex-col > div.text-text-secondary")

    # Fees (match Practo's "fees" field)
    data["fees"] = safe_select("div.flex.items-center.text-text-primary > span")

    return data






# --------------------------------------------------
# 🟩 ADD MORE FUNCTIONS BELOW LIKE THIS
# def extract_structured_data_from_quickerala(html, url):
#     # Your logic...
#     return {"source_url": url, "name": "Sample"}  # etc.
# --------------------------------------------------


# --------------------------------------------------
# 🔁 Map domain keywords to the right functions
# --------------------------------------------------
DOMAIN_EXTRACTORS = {
    "practo.com": extract_structured_data_from_practo,
    "quickerala.com": extract_structured_data_from_quickerala,
    "patakare.com": extract_structured_data_from_patakare,
    "drlogy.com": extract_structured_data_from_drlogy,
     "drdata.in": extract_structured_data_from_drdata,
    "healthfrog.in": extract_structured_data_from_healthfrog,
     "ask4healthcare.com": extract_structured_data_from_ask4healthcare,
    "apollo247.com": extract_structured_data_from_apollo247,
    "curofy.com": extract_structured_data_from_curofy,
    "converse.rgcross.com": extract_structured_data_from_curofy,
    "deldure.com": extract_structured_data_from_deldure,
    "credihealth.com": extract_structured_data_from_credihealth,
      "doctor360.in": extract_structured_data_from_doctor360,
    "bajajfinservhealth.in": extract_structured_data_from_bajajfinservhealth,
    "doctoriduniya.com": extract_structured_data_from_doctoriduniya,
     "askadoctor24x7.com": extract_structured_data_from_askadoctor24x7,
    "prescripson.com": extract_structured_data_from_prescripson,
    "justdialdds.com": extract_structured_data_from_justdialdds,
     "ihindustan.com": extract_structured_data_from_ihindustan,
    "lybrate.com": extract_structured_data_from_lybrate,
     "sehat.com": extract_structured_data_from_sehat,
    "lazoi.com": extract_structured_data_from_lazoi,
    "skedoc.com": extract_structured_data_from_skedoc,
     "mymedisage.com": extract_structured_data_from_mymedisage,
    "medibuddy.in": extract_structured_data_from_medibuddy,
    "myupchar.com": extract_structured_data_from_myupchar,
     "healthworldhospitals.com": extract_structured_data_from_healthworldhospitals,
    "docindia.org": extract_structured_data_from_docindia,
    "meddco.com": extract_structured_data_from_meddco,
     "medindia.net": extract_structured_data_from_medindia,
    "healthgrades.com": extract_structured_data_from_healthgrades,
    "clinicspots.com": extract_structured_data_from_clinicspots,
    "hexahealth.com": extract_structured_data_from_hexahealth,
    "mymedisage.com" : extract_structured_data_from_mymedisage,
    "kivihealth.com": extract_structured_data_from_kivihealth,
    "babymhospital.org": extract_structured_data_from_babymhospital,
    "arogyamitra.com": extract_structured_data_from_arogyamithra,
    "medicoverhospitals.com": extract_structured_data_from_medicover,
    "maxhealthcare.in": extract_structured_data_from_maxhealthcare,
    "manipalhospitals.com": extract_structured_data_from_manipalhospitals,
    "mappls.com": extract_structured_data_from_mappls,
    "eka.care": extract_structured_data_from_eka,

    # Add more mappings here:
    # "quickerala.com": extract_structured_data_from_quickerala,
    # "patakare.com": extract_structured_data_from_patakare,
    # "drlogy.com": extract_structured_data_from_drlogy,
    # "drdata": extract_structured_data_from_drdata,
}


# --------------------------------------------------
# Handle one URL using appropriate function
# --------------------------------------------------
# async def process_url(url, crawler, extractor_func):
#     config = CrawlerRunConfig(
#         cache_mode=CacheMode.BYPASS,
#         extraction_strategy=JsonCssExtractionStrategy({"name": "skip", "baseSelector": "body", "fields": []})
#     )
#     result = await crawler.arun(url=url, config=config)
#     if result.success and result.html:
#         return extractor_func(result.html, url)
#     return None

PLAYWRIGHT_DOMAINS = {"practo.com","www.docindia.org","babymhospital.org", "arogyamitra.com", "mappls.com","maxhealthcare.in"}


CHROME_PATH = r"C:\Users\Desk0012\AppData\Local\ms-playwright\chromium-1117\chrome-win\chrome.exe"


# async def fetch_with_playwright(url, wait_selectors=None, click_selectors=None, scroll=True, headless=False):
#     """
#     Universal Playwright HTML fetcher with dynamic waits, clicks, and scrolling.

#     Args:
#         url (str): The page URL.
#         wait_selectors (list[str]): CSS selectors to wait for before scraping.
#         click_selectors (list[str]): CSS selectors to click (like "View More").
#         scroll (bool): Whether to perform human-like scrolling.
#         headless (bool): Run in headless mode.
#     """
#     wait_selectors = wait_selectors or []
#     click_selectors = click_selectors or []

#     async with async_playwright() as p:
#         browser = await p.chromium.launch(headless=headless, args=["--no-sandbox"])
#         context = await browser.new_context(
#             user_agent=(
#                 "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#                 "AppleWebKit/537.36 (KHTML, like Gecko) "
#                 "Chrome/115.0.0.0 Safari/537.36"
#             )
#         )
#         page = await context.new_page()

#         try:
#             await page.goto(url, wait_until="domcontentloaded", timeout=60000)

#             # ✅ Wait for required elements
#             for selector in wait_selectors:
#                 try:
#                     await page.wait_for_selector(selector, timeout=10000)
#                 except:
#                     print(f"⚠️ Warning: Selector {selector} not found on {url}")

#             # ✅ Click elements if required
#             for selector in click_selectors:
#                 try:
#                     if await page.locator(selector).is_visible():
#                         await page.click(selector)
#                         await asyncio.sleep(2)
#                 except:
#                     print(f"⚠️ Warning: Could not click selector {selector} on {url}")

#             # ✅ Human-like scrolling
#             # if scroll:
#             #     for y in range(0, 5000, 500):
#             #         await page.mouse.wheel(0, 500)
#             #         await asyncio.sleep(0.3)

#             html = await page.content()
#             return html

#         except Exception as e:
#             print(f"❌ Playwright fetch failed for {url}: {e}")
#             return ""

#         finally:
#             await context.close()
#             await browser.close()

async def fetch_with_playwright(url, wait_selectors=None, click_selectors=None, scroll=True, headless=False, return_page=False):
    wait_selectors = wait_selectors or []
    click_selectors = click_selectors or []

    p = await async_playwright().start()
    browser = await p.chromium.launch(headless=headless, args=["--no-sandbox"])
    context = await browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/115.0.0.0 Safari/537.36"
        )
    )
    page = await context.new_page()

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)

        # Wait for selectors
        for selector in wait_selectors:
            try:
                await page.wait_for_selector(selector, timeout=10000)
            except:
                print(f"⚠️ Warning: Selector {selector} not found on {url}")

        # Click selectors
        for selector in click_selectors:
            try:
                if await page.locator(selector).is_visible():
                    await page.click(selector)
                    await asyncio.sleep(1.5)
            except:
                print(f"⚠️ Could not click {selector} on {url}")

        # Optional scrolling (human-like)
        if scroll:
            for y in range(0, 5000, 500):
                await page.mouse.wheel(0, 500)
                await asyncio.sleep(0.3)

        if return_page:
            return page, browser, context
        else:
            html = await page.content()
            await context.close()
            await browser.close()
            await p.stop()
            return html

    except Exception as e:
        print(f"❌ Playwright fetch failed for {url}: {e}")
        await context.close()
        await browser.close()
        await p.stop()
        return None



# async def process_url(url, crawler, extractor_func, sem, record_id=None):
#     async with sem:
#         try:
#             domain = urlparse(url).netloc.replace("www.", "")

#             # ✅ Check if domain has an extractor
#             if domain not in DOMAIN_EXTRACTORS:
#                 print(f"⏩ Skipping {url} (no extractor for domain: {domain})")
#                 return None

#             if domain in PLAYWRIGHT_DOMAINS:
#                 # Use Playwright
#                 html = await fetch_with_playwright(url)
#                 extracted_data = extractor_func(html, url)
#             else:
#                 # Use Crawl4AI
#                 config = CrawlerRunConfig(
#                     cache_mode=CacheMode.BYPASS,
#                     extraction_strategy=JsonCssExtractionStrategy({
#                         "name": "skip",
#                         "baseSelector": "body",
#                         "fields": []
#                     })
#                 )
#                 result = await crawler.arun(url=url, config=config)
#                 if not result.success or not result.html:
#                     return None
#                 extracted_data = extractor_func(result.html, url)

#             if isinstance(extracted_data, dict):
#                 extracted_data["Record_id"] = record_id
#                 extracted_data["url"] = url
#                 return extracted_data
#             else:
#                 print(f"⚠️ Extractor did not return a dict for {url}")

#         except Exception as e:
#             print(f"⚠️ Exception in process_url for {url}: {e}")

#     return None


def get_domain(url):
    domain = urlparse(url).netloc.lower()
    # remove 'www.' and take last two parts for main domain
    parts = domain.split('.')
    if len(parts) > 2:
        domain = '.'.join(parts[-2:])
    return domain
async def process_url(url, crawler, extractor_func, sem, record_id=None):
    async with sem:
        try:
            domain = get_domain(url)

            if domain not in DOMAIN_EXTRACTORS:
                print(f"⏩ Skipping {url} (no extractor for domain: {domain})")
                return None

            if domain in PLAYWRIGHT_DOMAINS:
                if asyncio.iscoroutinefunction(extractor_func):
                    # Open page and pass to async extractor
                    p = await async_playwright().start()
                    browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
                    context = await browser.new_context()
                    page = await context.new_page()
                    await page.goto(url, timeout=60000)

                    try:
                        extracted_data = await extractor_func(page, url)
                    finally:
                        await context.close()
                        await browser.close()
                        await p.stop()
                else:
                    # Sync extractor works on HTML string
                    async with async_playwright() as p:
                        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
                        context = await browser.new_context()
                        page = await context.new_page()
                        await page.goto(url, timeout=60000)
                        html = await page.content()
                        extracted_data = extractor_func(html, url)
                        await context.close()
                        await browser.close()

            else:
                # Crawl4AI domains
                config = CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,
                    extraction_strategy=JsonCssExtractionStrategy({
                        "name": "skip",
                        "baseSelector": "body",
                        "fields": []
                    })
                )
                result = await crawler.arun(url=url, config=config)
                if not result.success or not result.html:
                    return None
                extracted_data = extractor_func(result.html, url)

            if isinstance(extracted_data, dict):
                extracted_data["Record_id"] = record_id
                extracted_data["url"] = url
                return extracted_data
            else:
                print(f"⚠️ Extractor did not return a dict for {url}")

        except Exception as e:
            print(f"⚠️ Exception in process_url for {url}: {e}")

    return None

# --------------------------------------------------
# 🧠 Main Logic
# --------------------------------------------------
# async def main():
#     try:
#         input_file = r"C:\Users\Desk0012\Downloads\filtered_urls.xlsx"
#         df = pd.read_excel(input_file)

#         urls = df['url'].dropna().tolist()
#         browser_config = BrowserConfig(headless=True)
#         batch, BATCH_SIZE = [], 5

#         async with AsyncWebCrawler(config=browser_config) as crawler:
#             for url in tqdm(urls, desc="Scraping", ncols=100):
#                 try:
#                     extractor_func = None

#                     # Match only one extractor
#                     for domain, func in DOMAIN_EXTRACTORS.items():
#                         if domain in url:
#                             extractor_func = func
#                             break

#                     if not extractor_func:
#                         print(f"❌ No extractor found for: {url}")
#                         continue

#                     result = await process_url(url, crawler, extractor_func)

#                     if result:
#                         batch.append(result)
#                         if len(batch) == BATCH_SIZE:
#                             try:
#                                 collection.insert_many(batch)
#                                 print(f"✅ Inserted {len(batch)} records.")
#                             except Exception as insert_err:
#                                 print(f"⚠️ Error inserting batch: {insert_err}")
#                             batch.clear()

#                 except Exception as e:
#                     print(f"⚠️ Error processing URL {url}: {e}")

#             # Insert remaining results
#             if batch:
#                 try:
#                     collection.insert_many(batch)
#                     print(f"✅ Inserted remaining {len(batch)} records.")
#                 except Exception as final_insert_err:
#                     print(f"⚠️ Error inserting final batch: {final_insert_err}")

#     except Exception as main_err:
#         print(f"🚨 Unexpected error in main(): {main_err}")

client = MongoClient("mongodb://localhost:27017/")


# Database & collection setup
input_db = client["Gastroenterology"]
input_collection = input_db["allowed_domains"]

output_db = client["Gastroenterology"]
target_collection = output_db["first_scrape"]

master_db = client["medical_data"]
master_collection = master_db["master_with_parsed_education"]


# Env-based batching
batch_index = int(os.getenv("BATCH_INDEX", "0"))
batch_size = int(os.getenv("BATCH_SIZE", "10000"))
skip = batch_index * batch_size

# === Main Async Function ===
async def main():
    try:
        total_count = input_collection.count_documents({})
        batch_size = 10000  # can adjust based on memory
        total_batches = math.ceil(total_count / batch_size)
        print(f"🔹 Total URLs: {total_count}, batches: {total_batches}")

        for batch_index in range(total_batches):
            skip = batch_index * batch_size
            cursor = input_collection.find().skip(skip).limit(batch_size)
            urls_with_ids = list(cursor)

            if not urls_with_ids:
                continue

            # Step 1: Map URLs
            url_to_record = {item.get("link"): item for item in urls_with_ids if item.get("link")}
            all_urls = list(url_to_record.keys())

            # Step 2: Check reused URLs in master & target
            reused_master = list(master_collection.find({"souce_url": {"$in": all_urls}}))
            reused_target = list(target_collection.find({"source_url": {"$in": all_urls}}))
            reused_urls_set = {doc.get("source_url") for doc in reused_master + reused_target}
            print(f"Batch {batch_index}: {len(reused_urls_set)} reused URLs skipped.")

            # Step 3: Filter URLs to scrape
            to_scrape = [item for item in urls_with_ids if item.get("link") not in reused_urls_set]
            if not to_scrape:
                print(f"Batch {batch_index}: All URLs already processed.")
                continue

            # Step 4: Async scraper setup
            browser_config = BrowserConfig(headless=True)
            concurrency = 5
            insert_batch_size = 50
            batch_results = []
            sem = asyncio.Semaphore(concurrency)

            async with AsyncWebCrawler(config=browser_config) as crawler:

                async def process_and_store(item):
                    nonlocal batch_results
                    url = item.get('link')
                    record_id = item.get('Record_id')
                    client_name = item.get('Client_Name')
                    client_city = item.get('City')
                    client_speciality = item.get('Specialty')
                    extractor_func = next((func for domain, func in DOMAIN_EXTRACTORS.items() if domain in url), None)
                    if not extractor_func:
                        return

                    result = await process_url(url, crawler, extractor_func, sem, record_id=record_id)
                    if result:
                        result.update({
                            'client_name': client_name,
                            'client_city': client_city,
                            'client_speciality': client_speciality,
                            'url': url,
                            'record_id': record_id,
                            'original_record_id': None,
                            'new_record_id': record_id
                        })
                        try:
                            master_collection.insert_one(result)
                            batch_results.append(result)
                        except:
                            pass

                        if len(batch_results) >= insert_batch_size:
                            try:
                                target_collection.insert_many(batch_results)
                            except:
                                pass
                            batch_results.clear()

                # Run async tasks in batch
                tasks = [process_and_store(item) for item in to_scrape]
                for coro in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc=f"Scraping Batch {batch_index}", ncols=100):
                    await coro

                # Final insert
                if batch_results:
                    try:
                        target_collection.insert_many(batch_results)
                    except:
                        pass
                    batch_results.clear()

    except Exception:
        import traceback
        traceback.print_exc()

# === Run ===
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
