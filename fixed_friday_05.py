# pip install flask crawl4ai google-genai
from flask import Flask, request, jsonify
import asyncio
import os
import json
from typing import List, Optional, Dict, Any
from google import genai
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import PruningContentFilter

app = Flask(__name__)

# ---- Helper Functions ----
def clean_query_string(query: Optional[str]) -> str:
    """Remove --gemini from query string if present"""
    if not query:
        return ""
    return query.replace("--gemini", "").strip()

def safe_json_parse(text: str) -> dict:
    """Safely parse JSON response handling various formats"""
    try:
        if not text or not text.strip():
            return {}
        
        data = json.loads(text.strip())
        
        if isinstance(data, list):
            return {"doctor_info": data[0] if data else {}}
        
        if not isinstance(data, dict):
            return {"doctor_info": str(data)}
        
        return data
        
    except json.JSONDecodeError as e:
        print(f"JSON parsing error: {e}")
        return {}
    except Exception as e:
        print(f"Unexpected parsing error: {e}")
        return {}

# def build_clean_extraction_prompt(text_content: str) -> str:
#     return f"""
# Extract doctor information from this text and return a clean JSON structure.

# TEXT:
# {text_content}

# Return ONLY a JSON object with this exact structure:
# {
#   "personal_info": {
#     "name": "",
#     "experience": "",
#     "age": ""
#   },
#   "professional_info": {
#     "speciality": "",
#     "focus_area": "",
#     "languages": []
#   },
#   "education": {
#     "degrees": [
#       {
#         "Degree_1": "Extract the foundational primary medical degree. Only return 'MBBS' if it is explicitly mentioned. Do not return other degrees (e.g., BDS, MD, MS, DM, PhD, etc.). If MBBS is not found, return 'N/A'."
#       },
#       {
#         "Degree_2": "Extract the secondary medical degree if available. Look only for postgraduate qualifications that follow MBBS: MD with specialization (e.g., 'MD - General Medicine'), MS with specialization (e.g., 'MS - General Surgery'), or DNB with specialization (e.g., 'DNB - Anaesthesiology'). Extract the full degree in the exact format as written: 'Degree - Specialization'. If multiple are present, prioritize MD or MS over DNB. Do not include MBBS or non-medical degrees. If no valid secondary degree is found, return 'Not specified'."
#       },
#       {
#         "Degree_3": "Extract the tertiary degree if available. Look for super-specialty degrees that typically follow MD/MS: DM with specialization (e.g., 'DM - Cardiology'), MCh with specialization (e.g., 'MCh - Urology'), or subspecialty DNB degrees (e.g., 'DNB - Gastroenterology'). Return in the exact format 'Degree - Specialization'. If no tertiary degree is found, return 'Not specified'."
#       },
#       {
#         "Degree_4": "Extract the quaternary degree if available. Look for additional qualifications beyond super-specialty degrees: medical diplomas (e.g., 'Diploma in Medical Radiotherapy'), additional DNB degrees, DrNB degrees, or PhD degrees. Return the exact degree name as it appears. If no quaternary degree is found, return 'Not specified'."
#       },
#       {
#         "Degree_5": "Extract the fifth degree if available. Look for fellowships (e.g., 'Fellowship in Intensive Care Medicine'), certificates (e.g., 'Certificate Course in Anaesthesia Critical Care'), memberships (e.g., 'Member Of National Academy Of Medical Sciences'), or specialized training programs. Return the exact name as it appears. If no fifth degree is found, return 'Not specified'."
#       }
#     ],
#     "certifications": ""
#   },
#   "licenses": [],
#   "practice_locations": [
#     {
#       "address": "Full practice address, even if provided as standalone without any hospital/clinic name",
#       "city": "",
#       "state": "",
#       "country": "",
#       "pincode": "",
#       "latitude": "",
#       "longitude": "",
#       "plus_code": "",
#       "hco_name": "If linked with a hospital/clinic, capture name, otherwise 'NA'",
#       "hco_speciality": "",
#       "hco_type": "",
#       "phone": "",
#       "consultation_fee": "",
#       "timing": "",
#       "website": ""
#     }
#   ],
#   "contact_info": {
#     "phone": "",
#     "email": ""
#   },
#   "verification_info": {
#     "source": "",
#     "verified": true
#   }
# }

# STRICT RULES:
# - Do NOT include contact info (phone/email) from websites such as Practo, HexaHealth, hospital portals, or official directories.
# - Only include phone/email if explicitly stated as the doctor’s personal/professional contact in the text.
# - If not available, set as "NA".
# - Always capture full practice address (street, city, state, country, pincode) if available — even if it is provided as a standalone address without any HCO name.
# - Always extract practice consultation fee and timings if available in the text.
# - Always apply the 5 strict rules for degrees when populating Degree_1 through Degree_5.
# - Always return valid JSON only (no extra text, comments, or explanations).
# """


def build_clean_extraction_prompt(text_content: str) -> str:
    return f"""You are an information extraction system. Extract ONLY what is explicitly present in the text and return a strict JSON matching the schema below. Do not infer, compute, or guess any values. Return NA when not explicitly present.

TEXT:
{text_content}

Field guidance (short):
- personal_info.name: Copy the full name exactly as written (keep honorifics like "Dr." if present). No abbreviations or expansions. [exact copy]
- personal_info.experience: Only copy explicit phrases like "X years of experience" or "practicing since 20XX." Do not compute or infer.
- personal_info.age: Only if an explicit age is stated (e.g., "Age 45"). Do not derive from graduation year or DOB.
- professional_info.specialities: Copy all specialties exactly; split on commas and "and"; trim spaces; de-duplicate while preserving order.
- professional_info.focus_area: Copy explicit focus areas/subspecialties only; if not stated, NA.
- professional_info.languages: List languages only if explicitly stated (e.g., "Languages: English, Hindi"). No inference from name or region.
- education.degrees: Fill Degree_1..Degree_5 using the degree rules below. Do not place instructional text in values.
- education.certifications: Copy named certifications/board credentials if explicitly stated (e.g., "ATLS", "ACLS", "MRCP"). Otherwise NA.
- licenses: List medical council licenses exactly as written (e.g., "Maharashtra Medical Council – Reg. No. 12345 (2010)"). If multiple, include multiple array items. it will be mentioned as mci or Registrations or try to find don't keep any guess
- practice_locations: Output an array with 0–3 objects. For each, copy full address; parse city and pincode if explicitly present; hco_name is hospital/clinic name if linked, else "NA"; copy consultation_fee and timing exactly as stated.
- contact_info: Include phone/email only if explicitly the doctor’s personal/professional contact in the text. Exclude aggregator/directory numbers/emails.
- verification_info.source: Copy source name/identifier if present in the text or metadata; else NA.
- verification_info.verified: true only if at least name AND one core medical attribute (a specialty OR a degree) are explicitly present; else false.

Degree rules (strict precedence and values):
- Degree_1 (foundational): Only "MBBS" if explicitly present; else "N/A". Do not substitute BDS/AYUSH/paramedical degrees.
- Degree_2 (secondary PG): Only MD/MS/DNB with specialization, exact format "Degree - Specialization". If multiple, prefer MD/MS over DNB. If none, "Not specified".
- Degree_3 (tertiary super-specialty): Only DM/MCh/DNB (additional subspecialty beyond Degree_2), exact format. If none, "Not specified".
- Degree_4 (quaternary): Additional medical diplomas/certifications that are formal qualifications (e.g., "Diploma in Medical Radiotherapy")—exact name. If none, "Not specified".
- Degree_5 (fellowships/PhD/certificates): Fellowships, PhD, certificate courses, specialized training—exact name. If none, "Not specified".
- Degree rule precedence overrides general NA rule: use "N/A" or "Not specified" as above for degrees; use "NA" for all other missing fields.

Strict constraints:
- Copy-paste only: Values must be substrings from the provided text; no paraphrasing, expansion, normalization, or unit conversion.
- No guessing: If a field is not explicitly present, set to "NA" (or degree-specific fallback).
- No external contact info: Do not include phone/email from Practo/HexaHealth/hospital portals/directories unless explicitly stated as the doctor’s own contact in the text.
- Specialties: Do not add, prioritize, reorder, or infer specialties not explicitly present.
- Output: Return ONLY one JSON object that exactly matches the schema. No comments, no extra keys, no trailing text.

Return ONLY a JSON object with this exact structure (empty strings/arrays as placeholders are allowed, but must be replaced by extracted values or NA):
{{
  "personal_info": {{
    "name": "",
    "experience": "",
    "age": ""
  }},
  "professional_info": {{
    "specialities": [],
    "focus_area": "",
    "languages": []
  }},
  "education": {{
    "degrees": [
      {{ "Degree_1": "" }},
      {{ "Degree_2": "" }},
      {{ "Degree_3": "" }},
      {{ "Degree_4": "" }},
      {{ "Degree_5": "" }}
    ],
    "certifications": ""
  }},
  "licenses": [],
  "practice_locations": [
    {{
      "address": "",
      "city": "",
      "pincode": "",
      "hco_name": "",
      "phone": "",
      "consultation_fee": "",
      "timing": ""
    }}
  ],
  "contact_info": {{
    "phone": "",
    "email": ""
  }},
  "verification_info": {{
    "source": "",
    "verified": false
  }}
}}
"""




async def crawl_single_url(url: str) -> str:
    """Crawl a single URL and return cleaned text"""
    try:
        config = CrawlerRunConfig(
            markdown_generator=DefaultMarkdownGenerator(
                content_filter=PruningContentFilter()
            )
        )
        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(url=url, config=config)
            if getattr(result, "success", False):
                markdown = getattr(result, "markdown", "")
                return getattr(markdown, "raw_markdown", str(markdown) if markdown else "")
        return ""
    except Exception as e:
        print(f"Error crawling {url}: {str(e)}")
        return ""

def extract_with_gemini_clean(text_content: str, temperature: float = 0.1) -> dict:
    """Extract data with clean structure"""
    try:
        client = genai.Client(api_key="AIzaSyAMU7ObVxqz92nYCTJljHjWumwxfUupnsI")
        prompt = build_clean_extraction_prompt(text_content)
        #print(text_content)
        
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt,
            config={
                "response_mime_type": "application/json",
                "temperature": 1.5,
            },
        )
        
        response_text = getattr(response, 'text', str(response))
        parsed_data = safe_json_parse(response_text)
        
        return parsed_data if parsed_data else {}
        
    except Exception as e:
        print(f"Error with Gemini extraction: {str(e)}")
        return {}

def llm_strict_speciality_merge(all_profiles: List[dict], query_string: str, temperature: float = 0.1) -> dict:
    """STRICT speciality and comprehensive Indian name matching - only exact matches accepted"""
    
    profiles_text = "DOCTOR PROFILES TO ANALYZE:\n\n"
    
    for i, result in enumerate(all_profiles, 1):
        source_url = result.get('verification_info', {}).get('source', f'Source_{i}')
        profiles_text += f"PROFILE {i} - SOURCE: {source_url}\n"
        profiles_text += json.dumps(result, indent=2)
        profiles_text += "\n\n"

    # Use string concatenation instead of f-string to avoid brace escaping issues
    merge_prompt = """
You are an expert AI medical data analyst specializing in Indian doctor profiles with deep understanding of Indian naming conventions.

SEARCH QUERY: """ + query_string + """

""" + profiles_text + """

COMPREHENSIVE INDIAN NAME MATCHING RULES:

🇮🇳 **INDIAN NAMING CONVENTIONS AWARENESS:**

**Regional Patterns:**
- **North Indian**: First Name + Middle Name + Surname (e.g., "Rahul Kumar Sharma")
- **South Indian**: Village/Initial + Father's Name + Given Name + Caste (e.g., "M. Suresh Kumar Iyer")
- **Tamil**: Often "R. Ramesh" = "Ramesh Raman" (patronymic system)
- **Sikh**: Always includes Singh (male) or Kaur (female) (e.g., "Harpreet Singh Bedi")
- **Marathi**: Surname often comes first in documents (e.g., "Sharma, Rajesh Kumar")

**Name Normalization Steps (MANDATORY):**
1. Remove ALL honorifics: Dr., Prof., Mr., Mrs., Ms., Shri, Smt., etc.
2. Handle punctuation: Remove dots, normalize apostrophes, handle hyphens
3. Normalize spacing: Remove extra spaces, trim whitespace
4. Convert to lowercase for comparison
5. Handle comma-separated formats: "Sharma, Rajesh" → "Rajesh Sharma"
6. Recognize initials: "R.K. Sharma" vs "Rajesh Kumar Sharma"

**Token-Based Matching Rules:**

✅ **ACCEPT These Name Variations:**

1. **Exact Token Match (Different Orders)**
   - "Amar Singh" = "Singh Amar" ✅ (common in Maharashtra documents)
   - "Rajesh Kumar Patel" = "Patel Rajesh Kumar" ✅
   - "Ramesh Gupta" = "Gupta Ramesh" ✅

2. **Initial Expansion/Contraction**
   - "R.K. Sharma" = "Rajesh Kumar Sharma" ✅ (initials match)
   - "A. Singh" = "Amar Singh" ✅ (single initial expansion)
   - "S. Ramesh" = "Subramanian Ramesh" ✅ (South Indian patronymic)

3. **Standard Abbreviations**
   - "Ram Kumar" = "Ramkumar" ✅ (spacing variations)
   - "Raj Kumar" = "Rajkumar" ✅
   - "D'Souza" = "DSouza" ✅ (punctuation normalization)

4. **Sikh Name Variations**
   - "Harpreet Singh Bedi" = "Harpreet Kaur Bedi" ❌ (different gender markers)
   - "Harpreet Singh" = "Singh Harpreet" ✅ (order variation)

5. **Patronymic Variations (South Indian)**
   - "R. Ramesh" where R = Father's initial ✅
   - "Ramesh Raman" = "R. Ramesh" ✅ (if R matches Raman)

6. **Middle Name Flexibility**
   - "Rajesh Sharma" = "Rajesh Kumar Sharma" ✅ (missing middle name)
   - "A.K. Patel" = "Anil Patel" ✅ (one initial missing)

❌ **STRICT REJECTIONS:**

1. **Different Surnames**
   - "Rajesh Sharma" ≠ "Rajesh Verma" ❌ (different surnames)
   - "Singh" ≠ "Sing" ❌ (spelling difference)
  

2. **Different Given Names**
   - "Amar Singh" ≠ "Amaresh Singh" ❌ (different first names)
   - "Raj" ≠ "Rajesh" ❌ (not proven abbreviation)
   - "Krishna" ≠ "Krishnan" ❌ (different names)

3. **Incompatible Token Sets**
   - "Ramesh Kumar" ≠ "Kumar Suresh" ❌ (different given names)
   - "A.B. Patel" ≠ "Anil Sharma" ❌ (surname mismatch)

4. **Gender Marker Conflicts (Sikh)**
   - "Singh" (male) ≠ "Kaur" (female) ❌
   - Must match gender implications

5. **Incomplete Matches**
   - "Rajesh" ≠ "Rajesh Kumar Sharma" ❌ (too incomplete)
   - "R. Kumar" ≠ "Rajesh Patel" ❌ (surname missing)
   - "aren sunil kumar" ≠ "sunil kumar verma" ❌ (title missing)


**Advanced Matching Logic:**

🔍 **Name Comparison Algorithm:**
1. Normalize both names using steps above
2. Tokenize into word lists
3. Check if token sets are compatible:
   - All non-initial tokens must match exactly
   - Initials must be expandable to matching tokens
   - Handle surname-first formats automatically
4. Verify no conflicting tokens exist
5. Accept only if confidence is HIGH

**Cultural Context Awareness:**
- Women's names may include husband's name after marriage
- Caste names (Sharma, Patel, Iyer) are typically surnames
- Regional suffixes (Kumar, Singh, Kaur) have specific meanings
- Transliteration variations are common but must be handled carefully

**CRITICAL TEST CASES:**
- "Dr. Amaresh Singh" vs "Singh Amar" = ❌ REJECT (different first names)
- "Dr. Amaresh Singh" vs "Singh Amaresh" = ✅ ACCEPT (same tokens, reordered)
- "R.K. Sharma" vs "Rajesh Kumar Sharma" = ✅ ACCEPT (initials match)
- "Dr. Raj Patel" vs "Rajesh Patel" = ❌ REJECT (Raj ≠ Rajesh without proof)

🔒 **SPECIALITY MATCHING - ULTRA STRICT:**
- Endocrinology = Endocrinologist = Hormone Specialist = Hormonal Disorders Specialist = Diabetes Specialist = Diabetologist = Thyroid Specialist = Thyroid Disorders Specialist = Metabolism Specialist = Metabolic Disorders Specialist = Pituitary Specialist = Adrenal Specialist = Reproductive Endocrinologist ONLY
- Paediatrics = Pediatrics = Child Specialist = Paediatrician ONLY  
- Cardiology = Cardiologist = Heart Specialist = Cardiac Medicine ONLY
- Gastroenterology = Gastroenterologist = GI Specialist ONLY
- NO cross-speciality matching allowed
- NO flexible interpretation
- A doctor can have multiple specialities but if he has the speciality we are looking for it will acceptable(for example dr adithi internal medicine and Endocrinology is acceptable)
- A doctor can have multiple specialties, but only the ones listed above are acceptable.
- If multiple acceptable specialties are found, the specialty that matches the **query** must be listed first in the "specialities" array, followed by the others in the order they appear in the text.




🔒 DEGREE EXTRACTION – STRICT RULES

Degree_1 (Primary Medical Degree):
- Extract the foundational primary medical degree.
- Allowed: MBBS (only if explicitly mentioned).
- Not allowed: BDS, MD, MS, DM, PhD, etc.
- If MBBS is not found → return "N/A".

Degree_2 (Secondary Degree – Postgraduate):
- Extract postgraduate qualifications that follow MBBS.
- Allowed formats:
• MD – Specialization (e.g., MD – General Medicine)
• MS – Specialization (e.g., MS – General Surgery)
• DNB – Specialization (e.g., DNB – Anaesthesiology)
- Priority: MD/MS preferred over DNB.
- Not allowed: MBBS, non-medical degrees.
- If none found → return "Not specified".

Degree_3 (Tertiary Degree – Super-specialty):
- Extract super-specialty degrees that typically follow MD/MS.
- Allowed formats:
• DM – Specialization (e.g., DM – Cardiology)
• MCh – Specialization (e.g., MCh – Urology)
• DrNB degrees
• Subspecialty DNB – Specialization (e.g., DNB – Gastroenterology)
- If none found → return "Not specified".
- it can have multiple values

Degree_4 (Quaternary Degree – Advanced/Additional):
- Extract additional qualifications beyond super-specialty.
- Allowed formats:
• Medical Diplomas (e.g., Diploma in Medical Radiotherapy)
• Additional DNB degrees
- If none found → return "Not specified".
- It should not include anything else strictly
- it can have multiple values


Degree_5 (Fellowship / Certification / Other):
- Extract fellowships, certificates, or specialized training.
- Allowed formats:
• Fellowship (e.g., Fellowship in Intensive Care Medicine)
• Certificate (e.g., Certificate Course in Anaesthesia Critical Care)
• PhD (only medical/clinical PhDs)
• Specialized training programs
- If none found → return "Not specified".
- do not include any diploma or anything mentioned in the degree1 to degree4 strictly
- it can have multiple values



🏥 **LOCATION MATCHING:**
- Handle city aliases: Bangalore = Bengaluru, Mumbai = Bombay, Chennai = Madras, Mysore = Mysuru, Mangalore = Mangaluru, Gurgaon = Gurugram, Aurangabad = Chhatrapati Sambhajinagar
- State consistency required
- Geographic region should be logical

📋 **OUTPUT REQUIREMENTS:**
- Every profile MUST be categorized as accepted OR rejected
- Include detailed "reason_for_rejection" for ALL rejected profiles
- Accepted profiles must pass BOTH name AND speciality checks
- Total profiles = accepted + rejected count

**CRITICAL EXAMPLES FOR YOUR REFERENCE:**
1. Query: "Dr. Amaresh Singh General Physician" 
   - "Singh Amar, General Physician" = ❌ REJECT ("Amar" ≠ "Amaresh")
   - "Singh Amaresh, General Practice" = ✅ ACCEPT (same tokens + valid speciality)
   
2. Query: "Dr. R.K. Patel Cardiology"
   - "Rajesh Kumar Patel, Cardiologist" = ✅ ACCEPT (initials match + valid speciality)
   - "Raj Patel, Cardiology" = ❌ REJECT ("R" doesn't clearly = "Raj")

** for email and phone number that should be of proper hcp or hco email or phone number but it should not give any urls email like practo, hexahealth,maxhealthcare
   

Return EXACTLY this JSON structure:
{
    "accepted_profiles": [
        // ONLY profiles passing ALL strict criteria
    ],
    "rejected_profiles": [
        {
            "profile": {...},
            "reason_for_rejection": "detailed_reason_here"
        }
    ],
    "merged_profile": {
    "Cleaned_Query": \"""" + query_string + """\",
    
    "First_Name": "",
    "Last_Name": "",
    "Full_Name": "",
    
    "Primary_Email": "",
    "Primary_Phone": "",
    
    "Degree_1": "",
    "Degree_2": [],
    "Degree_3": [],
    "Degree_4": [],
    "Degree_5": [],
    
    "License_1_Issue_Year": "",
    "License_1_Number": "",
    "License_1_Body": "",
    "License_2_Issue_Year": "",
    "License_2_Number": "",
    "License_2_Body": "",
    
    "Experience_Years": "",
    "Primary_Speciality": "",
    "Secondary_Speciality": "",
    "Focus_Area": "",
    "Languages_Spoken_1": "",
    
    "Practice_HCO_Name": "",
    "Practice_Address": "",
    "Practice_City": "",
    "Practice_Consultation_Fee": "",
    "Practice_Timing": "",

    
    "Data_Sources": []
}

    "accepted_count": 0,
    "rejected_count": 0,
    "total_profiles": 0
}

FINAL INSTRUCTIONS:
- The most important rule in the merged data merge only accepted profiles don't merge rejected profiles
- Don't keep any false value which is not available in the json or not relevant to the qeury
- In secondary speciality u can keep the the speciality from focus area if you don't get
- For experience it should take the maximum one from the accepted data list 
- For fees it should take the highest one from the accepted data list 
- Data_Sources should be array of objects for each field here field data and proof link from where it has been taken should be mentioned for each data field don't miss for any column 
- Apply MAXIMUM strictness for name matching using Indian conventions
- Apply ULTRA strictness for speciality matching  
- Provide detailed rejection reasons
- Better to reject uncertain matches than accept false positives
- Focus on precision over recall
- My end goal is to get the right data increase the fill rate strictly but it should not keep the wrong data to increase the fill rate and keep the order of output fommat of merged records as mentioned 

Return ONLY JSON, no explanatory text.
"""

    try:
        client = genai.Client(api_key="AIzaSyAMU7ObVxqz92nYCTJljHjWumwxfUupnsI")
        
        response = client.models.generate_content(
            model="gemini-2.5-pro",
            contents=merge_prompt,
            config={
                "response_mime_type": "application/json",
                "temperature": temperature,
            },
        )
        
        response_text = getattr(response, 'text', str(response))
        result = safe_json_parse(response_text)
        
        # Ensure required structure
        if 'accepted_profiles' not in result:
            result['accepted_profiles'] = []
        if 'rejected_profiles' not in result:
            result['rejected_profiles'] = []
        if 'merged_profile' not in result:
            result['merged_profile'] = {}
        
        # Add counts and validation
        result['accepted_count'] = len(result['accepted_profiles'])
        result['rejected_count'] = len(result['rejected_profiles'])
        result['total_profiles'] = len(all_profiles)
        
        print(f"🔒 COMPREHENSIVE INDIAN NAME MATCHING: {result['accepted_count']} accepted, {result['rejected_count']} rejected")
        
        return result
        
    except Exception as e:
        print(f"LLM processing failed: {e}")
        return {
            "accepted_profiles": [],
            "rejected_profiles": [{"profile": profile, "reason_for_rejection": "processing_error"} for profile in all_profiles],
            "merged_profile": {"Cleaned_Query": query_string},
            "accepted_count": 0,
            "rejected_count": len(all_profiles),
            "total_profiles": len(all_profiles)
        }




async def process_urls_strict(urls: List[str], query_string: str, temperature: float = 0.1) -> dict:
    """Process URLs with STRICT speciality matching"""
    print(f"🚀 Processing {len(urls)} URLs with STRICT speciality matching...")
    
    # Crawl URLs
    crawled_texts = await asyncio.gather(*[crawl_single_url(url) for url in urls])

    # Extract with clean structure
    all_profiles = []
    successful_extractions = 0
    
    for url, text in zip(urls, crawled_texts):
        if text.strip():
            print(f"📄 Extracting from: {url}")
            try:
                data = extract_with_gemini_clean(text, temperature)
                if data:
                    # Add source info
                    if 'verification_info' not in data:
                        data['verification_info'] = {}
                    data['verification_info']['source'] = url
                    all_profiles.append(data)
                    successful_extractions += 1
                    print(f"✅ Extraction complete: {url}")
                else:
                    print(f"⚠️ No valid data: {url}")
            except Exception as e:
                print(f"⚠️ Error extracting from {url}: {e}")
        else:
            print(f"⚠️ No content: {url}")

    if not all_profiles:
        return {
            "accepted_profiles": [],
            "rejected_profiles": [],
            "merged_profile": {},
            "accepted_count": 0,
            "rejected_count": 0
        }

    print(f"📊 Extracted from {successful_extractions}/{len(urls)} URLs")
    
    # STRICT speciality matching merge
    result = llm_strict_speciality_merge(all_profiles, query_string, temperature)
    return result

# ---- Flask Routes ----
@app.route('/extract-doctor', methods=['POST'])
def extract_doctor_info():
    """Extract doctor information with STRICT speciality matching"""
    try:
        request_data = request.get_json()
        
        if not request_data or 'urls' not in request_data:
            return jsonify({
                "error": "Missing 'urls' field in request body",
                "status": "error"
            }), 400
        
        urls = request_data['urls']
        raw_query = request_data.get('query', '')
        temperature = request_data.get('temperature', 0.1)  # Lower temperature for strict matching
        
        clean_query = clean_query_string(raw_query)
        
        # Validate inputs
        if not isinstance(urls, list) or len(urls) == 0:
            return jsonify({
                "error": "URLs must be a non-empty list",
                "status": "error"
            }), 400
        
        if len(urls) > 10:
            return jsonify({
                "error": "Maximum 10 URLs allowed per request",
                "status": "error"
            }), 400
        
        for url in urls:
            if not isinstance(url, str) or not (url.startswith('http://') or url.startswith('https://')):
                return jsonify({
                    "error": f"Invalid URL: {url}",
                    "status": "error"
                }), 400
        
        print(f"🔒 STRICT SPECIALITY MATCHING")
        print(f"📊 URLs: {len(urls)}")
        print(f"🔍 Query: '{clean_query}'")
        print(f"🌡️ Temperature: {temperature}")
        print("⚠️ STRICT MODE: Only exact speciality matches will be accepted")
        
        # Process with STRICT matching
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(process_urls_strict(urls, clean_query, temperature))
        finally:
            loop.close()
        
        # Calculate fill rate
        merged_profile = result.get("merged_profile", {})
        total_fields = len(merged_profile) if merged_profile else 0
        filled_fields = 0
        
        if merged_profile:
            for v in merged_profile.values():
                if v and str(v) not in ["NA", "Not specified", "", "null"]:
                    filled_fields += 1
        
        fill_rate = (filled_fields / total_fields * 100) if total_fields > 0 else 0
        
        print(f"🔒 STRICT PROCESSING COMPLETED - Fill Rate: {fill_rate:.1f}%")
        
        return jsonify({
            "status": "success",
            "data": merged_profile,
            "accepted_profiles": result.get("accepted_profiles", []),
            "rejected_profiles": result.get("rejected_profiles", []),
            "accepted_count": result.get("accepted_count", 0),
            "rejected_count": result.get("rejected_count", 0),
            "total_profiles": result.get("accepted_count", 0) + result.get("rejected_count", 0),
            "urls_processed": len(urls),
            "original_query": raw_query,
            "cleaned_query": clean_query,
            "temperature_used": temperature,
            "processing_method": "STRICT speciality matching - exact matches only",
            "fill_rate_percentage": round(fill_rate, 1),
            "filled_fields": filled_fields,
            "total_fields": total_fields,
            "matching_mode": "STRICT - only exact speciality matches accepted",
            "message": f"STRICT processing completed - {result.get('accepted_count', 0)} accepted, {result.get('rejected_count', 0)} rejected - {fill_rate:.1f}% fields filled"
        })
        
    except Exception as e:
        print(f"❌ API Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            "error": str(e),
            "status": "error"
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        "status": "healthy", 
        "message": "STRICT Speciality Matching Doctor API",
        "version": "13.0.0-StrictSpeciality"
    })

@app.route('/', methods=['GET'])
def home():
    return jsonify({
        "message": "STRICT Speciality Matching Doctor API",
        "version": "13.0.0-StrictSpeciality", 
        "description": "Doctor information extraction with STRICT speciality matching - only exact matches accepted",
        "strict_matching_rules": {
            "General Physician": ["General Physician", "General Practice", "Family Medicine", "Internal Medicine"],
            "Paediatrics": ["Paediatrics", "Pediatrics", "Child Specialist"],
            "Cardiology": ["Cardiology", "Cardiologist", "Heart Specialist"],
            "Gastroenterology": ["Gastroenterology", "Gastroenterologist", "GI Specialist"],
            "note": "Cross-speciality matches are REJECTED (e.g., Paediatrics for General Physician query)"
        },
        "key_changes": [
            "STRICT speciality matching - no cross-speciality acceptance",
            "Only exact synonyms allowed for each speciality",
            "Lower default temperature (0.1) for precise matching",
            "Clear rejection of different specialities",
            "Paediatrics will NOT be accepted for General Physician queries"
        ]
    })

if __name__ == '__main__':
    print("🚀 Starting STRICT Speciality Matching Doctor API v13.0.0")
    print("🔒 Focus: STRICT speciality matching - only exact matches accepted")
    print("⚠️ CRITICAL CHANGE:")
    print("   • General Physician queries will REJECT Paediatrics profiles")
    print("   • Only exact speciality matches or approved synonyms accepted")
    print("   • Cross-speciality matching is DISABLED")
    print()
    print("🎯 Strict Matching Rules:")
    print("   • General Physician = General Practice = Family Medicine = Internal Medicine")
    print("   • Paediatrics = Pediatrics = Child Specialist")
    print("   • Cardiology = Cardiologist = Heart Specialist")
    print("   • NO cross-speciality acceptance (Paediatrics ≠ General Physician)")
    print()
    print("Available endpoints:")
    print("  POST /extract-doctor - Extract with STRICT speciality matching")
    print("  GET /health - Health check")
    print("  GET / - API documentation")
    
    app.run(debug=True, host='0.0.0.0', port=5559)
