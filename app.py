import streamlit as st
import pandas as pd
import duckdb
import requests
import json
import re
from datetime import datetime
import io
import base64
from typing import Dict, List, Tuple
import pyarrow as pa
import pyarrow.parquet as pq
from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.enum.text import PP_ALIGN
from pptx.dml.color import RGBColor
import asyncio
import aiohttp
from asyncio import Semaphore
import time
from collections import deque

# Page config
st.set_page_config(
    page_title="QA Coaching Intelligence",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');
    
    * {
        font-family: 'Inter', sans-serif;
    }
    
    .main {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        background-attachment: fixed;
    }
    
    .stApp {
        background: transparent;
    }
    
    div[data-testid="stMetricValue"] {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
        background: rgba(255,255,255,0.95);
        padding: 20px;
        border-radius: 15px;
        backdrop-filter: blur(10px);
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 60px;
        padding: 0 30px;
        font-weight: 600;
        font-size: 1.1rem;
        border-radius: 10px;
        transition: all 0.3s ease;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white !important;
    }
    
    .upload-box {
        background: rgba(255,255,255,0.95);
        padding: 40px;
        border-radius: 20px;
        text-align: center;
        border: 3px dashed #667eea;
        transition: all 0.3s ease;
    }
    
    .upload-box:hover {
        border-color: #764ba2;
        transform: translateY(-5px);
        box-shadow: 0 20px 40px rgba(102,126,234,0.3);
    }
    
    .metric-card {
        background: rgba(255,255,255,0.95);
        padding: 30px;
        border-radius: 15px;
        box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        transition: all 0.3s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 20px 40px rgba(102,126,234,0.3);
    }
    
    .agent-card {
        background: white;
        padding: 25px;
        border-radius: 15px;
        margin: 15px 0;
        box-shadow: 0 5px 15px rgba(0,0,0,0.1);
        border-left: 5px solid #667eea;
        transition: all 0.3s ease;
    }
    
    .agent-card:hover {
        transform: translateX(5px);
        box-shadow: 0 10px 25px rgba(102,126,234,0.3);
    }
    
    .theme-badge {
        display: inline-block;
        padding: 8px 16px;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.9rem;
        margin: 5px;
    }
    
    .priority-high {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        color: white;
    }
    
    .priority-medium {
        background: linear-gradient(135deg, #ffd89b 0%, #19547b 100%);
        color: white;
    }
    
    .priority-low {
        background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
        color: #333;
    }
    
    .chat-message {
        padding: 15px 20px;
        border-radius: 15px;
        margin: 10px 0;
        animation: fadeIn 0.3s ease;
    }
    
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(10px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    .user-message {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        margin-left: 20%;
    }
    
    .assistant-message {
        background: rgba(255,255,255,0.95);
        color: #333;
        margin-right: 20%;
        border: 2px solid #667eea;
    }
    
    .stButton>button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        padding: 15px 40px;
        font-size: 1.1rem;
        font-weight: 600;
        border-radius: 10px;
        transition: all 0.3s ease;
    }
    
    .stButton>button:hover {
        transform: translateY(-3px);
        box-shadow: 0 10px 25px rgba(102,126,234,0.4);
    }
    
    .stSelectbox, .stMultiSelect {
        background: white;
        border-radius: 10px;
    }
    
    .success-banner {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        color: white;
        padding: 20px;
        border-radius: 15px;
        font-weight: 600;
        text-align: center;
        animation: slideDown 0.5s ease;
    }
    
    @keyframes slideDown {
        from { transform: translateY(-20px); opacity: 0; }
        to { transform: translateY(0); opacity: 1; }
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'duckdb_conn' not in st.session_state:
    st.session_state.duckdb_conn = duckdb.connect(':memory:')
if 'coaching_insights' not in st.session_state:
    st.session_state.coaching_insights = {}
if 'chat_history' not in st.session_state:
    st.session_state.chat_history = []
if 'processed' not in st.session_state:
    st.session_state.processed = False
if 'processing_stats' not in st.session_state:
    st.session_state.processing_stats = {
        'total_batches': 0,
        'completed_batches': 0,
        'failed_batches': 0,
        'start_time': None,
        'total_tokens': 0
    }
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'pre_analysis_done' not in st.session_state:
    st.session_state.pre_analysis_done = False

# Model configurations
MODELS = {
    "qwen/qwen3-coder:free": {
        "name": "Qwen Coder 3",
        "stars": "⭐⭐⭐⭐⭐",
        "best_for": "Structured coaching analysis",
        "speed": "Very Fast",
        "recommended": True
    },
    "x-ai/grok-4.1-fast:free": {
        "name": "Grok 4.1 Fast",
        "stars": "⭐⭐⭐⭐⭐",
        "best_for": "Complex reasoning",
        "speed": "Fast"
    },
    "meta-llama/llama-3.3-70b-instruct:free": {
        "name": "Llama 3.3 70B",
        "stars": "⭐⭐⭐⭐",
        "best_for": "Balanced performance",
        "speed": "Medium"
    },
    "mistralai/mistral-nemo:free": {
        "name": "Mistral Nemo",
        "stars": "⭐⭐⭐",
        "best_for": "Fast Q&A chat",
        "speed": "Very Fast"
    },
    "mistralai/mistral-small-3.1-24b-instruct:free": {
        "name": "Mistral Small 3.1",
        "stars": "⭐⭐⭐",
        "best_for": "Quick analysis",
        "speed": "Very Fast"
    }
}

# Default coaching themes
DEFAULT_THEMES = [
    "Active Listening & Acknowledgment",
    "Empathy & Emotional Intelligence",
    "Clear Communication & Articulation",
    "Professional Tone & Language",
    "First Call Resolution",
    "Problem Diagnosis",
    "Solution Offering",
    "Response Time Management",
    "Process Adherence",
    "Escalation Judgment",
    "Proactive Communication",
    "Managing Expectations",
    "Handling Difficult Customers",
    "Building Rapport",
    "Product Knowledge",
    "Confidence in Responses"
]

class RateLimiter:
    """Token bucket rate limiter for API calls"""
    def __init__(self, calls_per_minute: int):
        self.calls_per_minute = calls_per_minute
        self.tokens = calls_per_minute
        self.last_update = time.time()
        self.lock = asyncio.Lock()
    
    async def acquire(self):
        async with self.lock:
            now = time.time()
            elapsed = now - self.last_update
            
            # Refill tokens based on elapsed time
            self.tokens = min(
                self.calls_per_minute,
                self.tokens + (elapsed * self.calls_per_minute / 60)
            )
            self.last_update = now
            
            if self.tokens >= 1:
                self.tokens -= 1
                return
            
            # Wait until next token available
            wait_time = (1 - self.tokens) * 60 / self.calls_per_minute
            await asyncio.sleep(wait_time)
            self.tokens = 0
            self.last_update = time.time()

def redact_pii(text: str) -> str:
    """Redact PII from text"""
    # Email
    text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL_REDACTED]', text)
    # Phone
    text = re.sub(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', '[PHONE_REDACTED]', text)
    # SSN
    text = re.sub(r'\b\d{3}-\d{2}-\d{4}\b', '[SSN_REDACTED]', text)
    # Credit card (basic)
    text = re.sub(r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b', '[CARD_REDACTED]', text)
    return text

def normalize_speaker(speaker: str) -> str:
    """Normalize speaker labels"""
    speaker_lower = speaker.lower()
    if any(x in speaker_lower for x in ['agent', 'representative', 'rep']):
        return 'agent'
    elif any(x in speaker_lower for x in ['customer', 'consumer', 'client']):
        return 'customer'
    return speaker_lower

def parse_multiline_transcript(transcript_text: str) -> List[Dict]:
    """Parse multiline transcript from single cell into conversation turns
    
    Handles formats:
    1. Bracket with newline: "[12:30:08 AGENT]:\n message"
    2. Bracket inline: "[12:30:08 AGENT]: message"
    3. Pipe-separated: "2025-02-07 13:17:57 +0000 Consumer: Hi! | 2025-02-07 13:18:01 +0000 Agent: Hello"
    """
    turns = []
    
    # Pattern for bracket format with optional newline: "[12:30:08 AGENT]:\n message" or "[12:30:08 AGENT]: message"
    # Using re.DOTALL to match across newlines
    bracket_pattern = r'\[([\d:]+)\s+([^\]]+)\]:\s*\n?\s*(.*?)(?=\[[\d:]+\s+[^\]]+\]:|$)'
    
    # Pattern for pipe format: "2025-02-07 13:17:57 +0000 Consumer: message"
    pipe_pattern = r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s+[+-]\d{4})\s+([^:]+):\s*(.*)'
    
    # Check if pipe-separated format
    if '|' in transcript_text:
        segments = transcript_text.split('|')
        for segment in segments:
            segment = segment.strip()
            if not segment:
                continue
            match = re.match(pipe_pattern, segment)
            if match:
                timestamp, speaker, message = match.groups()
                turns.append({
                    'timestamp': timestamp,
                    'speaker': normalize_speaker(speaker.strip()),
                    'message': redact_pii(message.strip())
                })
    else:
        # Try bracket format with regex findall (handles newlines)
        matches = re.findall(bracket_pattern, transcript_text, re.DOTALL)
        for match in matches:
            timestamp, speaker, message = match
            # Clean up message (remove extra whitespace/newlines)
            message = ' '.join(message.split())
            if message:
                turns.append({
                    'timestamp': timestamp.strip(),
                    'speaker': normalize_speaker(speaker.strip()),
                    'message': redact_pii(message.strip())
                })
    
    return turns

def convert_to_parquet(df: pd.DataFrame, filename: str) -> bytes:
    """Convert dataframe to parquet bytes"""
    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression='snappy')
    buf.seek(0)
    return buf.getvalue()

def load_file_to_dataframe(uploaded_file) -> pd.DataFrame:
    """Load various file formats to raw dataframe (no parsing)"""
    file_ext = uploaded_file.name.split('.')[-1].lower()
    
    if file_ext == 'csv':
        return pd.read_csv(uploaded_file)
    elif file_ext in ['xlsx', 'xls']:
        return pd.read_excel(uploaded_file)
    elif file_ext == 'parquet':
        return pd.read_parquet(uploaded_file)
    elif file_ext == 'txt':
        # For TXT, create simple dataframe
        content = uploaded_file.read().decode('utf-8')
        return pd.DataFrame([{
            'call_id': 'CALL_0001',
            'agent': 'Unknown',
            'transcript': content
        }])
    else:
        st.error(f"Unsupported file format: {file_ext}")
        return None

async def call_llm_async(
    session: aiohttp.ClientSession,
    model: str,
    messages: List[Dict],
    temperature: float = 0.3,
    is_json: bool = True,
    provider: str = "openrouter",
    api_key: str = None,
    local_url: str = None,
    rate_limiter: RateLimiter = None
) -> Dict:
    """Async LLM call supporting OpenRouter and Local LLM"""
    
    if rate_limiter:
        await rate_limiter.acquire()
    
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": 2000
    }
    
    if is_json and provider == "openrouter":
        payload["response_format"] = {"type": "json_object"}
    
    try:
        if provider == "openrouter":
            url = "https://openrouter.ai/api/v1/chat/completions"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}" if api_key else ""
            }
        else:  # local LLM
            url = local_url or "http://localhost:1234/v1/chat/completions"
            headers = {"Content-Type": "application/json"}
        
        async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=60)) as response:
            if response.status == 200:
                return await response.json()
            else:
                error_text = await response.text()
                return {"error": f"HTTP {response.status}: {error_text}"}
                
    except asyncio.TimeoutError:
        return {"error": "Request timeout"}
    except Exception as e:
        return {"error": str(e)}

def call_llm(model: str, messages: List[Dict], temperature: float = 0.3, is_json: bool = True) -> Dict:
    """Synchronous wrapper for backward compatibility (used in chat)"""
    provider = st.session_state.get('llm_provider', 'openrouter')
    api_key = st.session_state.get('openrouter_api_key')
    local_url = st.session_state.get('local_llm_url')
    
    if provider == "openrouter":
        url = "https://openrouter.ai/api/v1/chat/completions"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}" if api_key else ""
        }
    else:
        url = local_url or "http://localhost:1234/v1/chat/completions"
        headers = {"Content-Type": "application/json"}
    
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": 2000
    }
    
    if is_json and provider == "openrouter":
        payload["response_format"] = {"type": "json_object"}
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        st.error(f"LLM API Error: {str(e)}")
        return None

def process_agent_batch(agent_name: str, calls_df: pd.DataFrame, themes: List[str], model: str) -> Dict:
    """Process batch of calls for an agent (kept for backward compatibility)"""
    
    # Create compressed context
    call_summaries = []
    for idx, call_id in enumerate(calls_df['call_id'].unique()[:10], 1):
        call_data = calls_df[calls_df['call_id'] == call_id]
        
        # Get sentiment if available
        sentiment = call_data['sentiment_score'].mean() if 'sentiment_score' in call_data.columns else None
        
        # Compress conversation
        agent_msgs = call_data[call_data['speaker'] == 'agent']['message'].tolist()
        customer_msgs = call_data[call_data['speaker'] == 'customer']['message'].tolist()
        
        summary = f"Call {idx}:"
        if sentiment:
            summary += f" [Sentiment: {sentiment:.2f}]"
        summary += f"\n- Customer issues: {' | '.join(customer_msgs[:3])}"
        summary += f"\n- Agent responses: {' | '.join(agent_msgs[:3])}"
        
        call_summaries.append(summary)
    
    # Build prompt
    system_prompt = f"""You are an expert contact center QA analyst. Analyze agent performance and identify coaching opportunities.

Focus on these themes: {', '.join(themes)}

Provide response as JSON with this exact structure:
{{
    "agent": "agent_name",
    "calls_analyzed": number,
    "coaching_themes": [
        {{
            "theme": "theme name from provided list",
            "priority": "high|medium|low",
            "frequency": number,
            "examples": ["specific example 1", "specific example 2"],
            "recommendation": "specific actionable advice"
        }}
    ],
    "strengths": ["strength 1", "strength 2"],
    "overall_sentiment_correlation": "insight about sentiment and performance"
}}

CRITICAL: Only use themes from the provided list. Be specific and data-driven."""

    user_prompt = f"""Agent: {agent_name}
Calls analyzed: {len(calls_df['call_id'].unique())}

Call Summaries:
{chr(10).join(call_summaries)}

Identify top 3-5 coaching opportunities with specific examples and actionable recommendations."""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]
    
    response = call_llm(model, messages)
    
    if response and 'choices' in response:
        try:
            content = response['choices'][0]['message']['content']
            # Handle markdown code blocks
            content = re.sub(r'```json\n?', '', content)
            content = re.sub(r'```\n?', '', content)
            return json.loads(content.strip())
        except json.JSONDecodeError as e:
            st.error(f"Failed to parse LLM response for {agent_name}: {str(e)}")
            return None
    
    return None

async def process_agent_batch_async(
    session: aiohttp.ClientSession,
    agent_name: str,
    calls_df: pd.DataFrame,
    themes: List[str],
    model: str,
    provider: str,
    api_key: str,
    local_url: str,
    rate_limiter: RateLimiter,
    semaphore: Semaphore
) -> Tuple[str, Dict]:
    """Async process batch of calls for an agent"""
    
    async with semaphore:
        try:
            # Get unique call IDs for this agent (limit to 5-7 calls max)
            unique_calls = calls_df['call_id'].unique()[:7]  # Reduced to 7 calls max
            
            # Create compressed context
            call_summaries = []
            for idx, call_id in enumerate(unique_calls, 1):
                call_data = calls_df[calls_df['call_id'] == call_id]
                
                sentiment = call_data['sentiment_score'].mean() if 'sentiment_score' in call_data.columns else None
                
                # Get only first 3 messages from each side to keep it concise
                agent_msgs = call_data[call_data['speaker'] == 'agent']['message'].tolist()[:3]
                customer_msgs = call_data[call_data['speaker'] == 'customer']['message'].tolist()[:3]
                
                # Compress messages further (first 100 chars each)
                agent_msgs_short = [msg[:100] + '...' if len(msg) > 100 else msg for msg in agent_msgs]
                customer_msgs_short = [msg[:100] + '...' if len(msg) > 100 else msg for msg in customer_msgs]
                
                summary = f"Call {idx}:"
                if sentiment:
                    summary += f" [Sentiment: {sentiment:.2f}]"
                summary += f"\n- Customer: {' | '.join(customer_msgs_short)}"
                summary += f"\n- Agent: {' | '.join(agent_msgs_short)}"
                
                call_summaries.append(summary)
            
            system_prompt = f"""You are an expert contact center QA analyst. Analyze agent performance and identify coaching opportunities.

Focus on these themes: {', '.join(themes[:10])}

Provide response as JSON with this exact structure:
{{
    "agent": "{agent_name}",
    "calls_analyzed": {len(unique_calls)},
    "coaching_themes": [
        {{
            "theme": "theme name from provided list",
            "priority": "high|medium|low",
            "frequency": 1,
            "examples": ["brief example"],
            "recommendation": "specific actionable advice"
        }}
    ],
    "strengths": ["strength 1", "strength 2"],
    "overall_sentiment_correlation": "brief insight"
}}

CRITICAL: Only use themes from the provided list. Be specific and data-driven. Keep response concise."""

            user_prompt = f"""Agent: {agent_name}
Calls analyzed: {len(unique_calls)}

Call Summaries:
{chr(10).join(call_summaries)}

Identify top 3 coaching opportunities with specific examples and actionable recommendations."""

            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
            
            response = await call_llm_async(
                session, model, messages,
                provider=provider,
                api_key=api_key,
                local_url=local_url,
                rate_limiter=rate_limiter
            )
            
            if not response:
                print(f"[ERROR] {agent_name}: No response from LLM")
                return (agent_name, None)
            
            if 'error' in response:
                print(f"[ERROR] {agent_name}: {response['error']}")
                return (agent_name, None)
            
            if 'choices' not in response:
                print(f"[ERROR] {agent_name}: No 'choices' in response: {response}")
                return (agent_name, None)
            
            try:
                content = response['choices'][0]['message']['content']
                content = re.sub(r'```json\n?', '', content)
                content = re.sub(r'```\n?', '', content)
                result = json.loads(content.strip())
                print(f"[SUCCESS] {agent_name}: Generated {len(result.get('coaching_themes', []))} themes")
                return (agent_name, result)
            except json.JSONDecodeError as e:
                print(f"[ERROR] {agent_name}: JSON parse failed: {str(e)}")
                print(f"[ERROR] Content: {content[:200]}")
                return (agent_name, None)
        
        except Exception as e:
            print(f"[ERROR] {agent_name}: Exception: {str(e)}")
            import traceback
            traceback.print_exc()
            return (agent_name, None)

async def process_all_agents_parallel(
    agents_data: List[Tuple[str, pd.DataFrame]],
    themes: List[str],
    model: str,
    provider: str,
    api_key: str,
    local_url: str,
    max_concurrent: int = 10,
    calls_per_minute: int = 50
) -> Dict:
    """Process all agents in parallel with rate limiting"""
    
    rate_limiter = RateLimiter(calls_per_minute)
    semaphore = Semaphore(max_concurrent)
    
    async with aiohttp.ClientSession() as session:
        tasks = [
            process_agent_batch_async(
                session, agent_name, agent_df, themes, model,
                provider, api_key, local_url, rate_limiter, semaphore
            )
            for agent_name, agent_df in agents_data
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    insights = {}
    for result in results:
        if isinstance(result, tuple) and result[1]:
            agent_name, data = result
            insights[agent_name] = data
    
    return insights

def generate_html_report(insights: Dict, df: pd.DataFrame) -> str:
    """Generate beautiful HTML report"""
    
    total_calls = len(df['call_id'].unique()) if 'call_id' in df.columns else len(df)
    total_agents = len(insights)
    total_themes = sum(len(agent_data.get('coaching_themes', [])) for agent_data in insights.values())
    avg_sentiment = df['sentiment_score'].mean() if 'sentiment_score' in df.columns else 0
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>QA Coaching Intelligence Report</title>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap" rel="stylesheet">
        <style>
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
                font-family: 'Inter', sans-serif;
            }}
            
            body {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 40px;
                min-height: 100vh;
            }}
            
            .container {{
                max-width: 1400px;
                margin: 0 auto;
                background: white;
                border-radius: 30px;
                padding: 50px;
                box-shadow: 0 30px 60px rgba(0,0,0,0.3);
            }}
            
            .header {{
                text-align: center;
                margin-bottom: 50px;
                padding-bottom: 30px;
                border-bottom: 3px solid #667eea;
            }}
            
            h1 {{
                font-size: 3rem;
                font-weight: 700;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                margin-bottom: 10px;
            }}
            
            .subtitle {{
                color: #666;
                font-size: 1.2rem;
            }}
            
            .metrics-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 30px;
                margin-bottom: 50px;
            }}
            
            .metric-card {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                padding: 30px;
                border-radius: 20px;
                color: white;
                box-shadow: 0 10px 30px rgba(102,126,234,0.3);
                transition: transform 0.3s ease;
            }}
            
            .metric-card:hover {{
                transform: translateY(-5px);
            }}
            
            .metric-label {{
                font-size: 0.9rem;
                opacity: 0.9;
                margin-bottom: 10px;
                text-transform: uppercase;
                letter-spacing: 1px;
            }}
            
            .metric-value {{
                font-size: 3rem;
                font-weight: 700;
            }}
            
            .section-title {{
                font-size: 2rem;
                font-weight: 700;
                color: #333;
                margin: 50px 0 30px 0;
                padding-bottom: 15px;
                border-bottom: 3px solid #667eea;
            }}
            
            .chart-container {{
                background: #f8f9fa;
                padding: 30px;
                border-radius: 20px;
                margin-bottom: 40px;
                box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            }}
            
            .agent-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(450px, 1fr));
                gap: 30px;
                margin-bottom: 50px;
            }}
            
            .agent-card {{
                background: white;
                border: 2px solid #e0e0e0;
                border-radius: 20px;
                padding: 30px;
                box-shadow: 0 5px 15px rgba(0,0,0,0.1);
                transition: all 0.3s ease;
            }}
            
            .agent-card:hover {{
                transform: translateY(-5px);
                box-shadow: 0 15px 35px rgba(102,126,234,0.3);
                border-color: #667eea;
            }}
            
            .agent-header {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 20px;
                padding-bottom: 20px;
                border-bottom: 2px solid #f0f0f0;
            }}
            
            .agent-name {{
                font-size: 1.5rem;
                font-weight: 700;
                color: #333;
            }}
            
            .agent-stats {{
                display: flex;
                gap: 15px;
            }}
            
            .stat-badge {{
                background: #f0f0f0;
                padding: 8px 15px;
                border-radius: 10px;
                font-size: 0.85rem;
                font-weight: 600;
            }}
            
            .theme-list {{
                margin: 20px 0;
            }}
            
            .theme-item {{
                background: #f8f9fa;
                padding: 15px;
                border-radius: 10px;
                margin-bottom: 15px;
                border-left: 5px solid #667eea;
            }}
            
            .theme-item.high {{
                border-left-color: #f5576c;
                background: linear-gradient(90deg, rgba(245,87,108,0.1) 0%, transparent 100%);
            }}
            
            .theme-item.medium {{
                border-left-color: #ffa726;
                background: linear-gradient(90deg, rgba(255,167,38,0.1) 0%, transparent 100%);
            }}
            
            .theme-item.low {{
                border-left-color: #66bb6a;
                background: linear-gradient(90deg, rgba(102,187,106,0.1) 0%, transparent 100%);
            }}
            
            .theme-header {{
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 10px;
            }}
            
            .theme-name {{
                font-weight: 700;
                font-size: 1.1rem;
                color: #333;
            }}
            
            .priority-badge {{
                padding: 5px 15px;
                border-radius: 20px;
                font-size: 0.8rem;
                font-weight: 700;
                text-transform: uppercase;
            }}
            
            .priority-high {{
                background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                color: white;
            }}
            
            .priority-medium {{
                background: linear-gradient(135deg, #ffd89b 0%, #ffa726 100%);
                color: white;
            }}
            
            .priority-low {{
                background: linear-gradient(135deg, #a8edea 0%, #66bb6a 100%);
                color: white;
            }}
            
            .theme-examples {{
                margin: 10px 0;
                font-size: 0.9rem;
                color: #666;
                font-style: italic;
            }}
            
            .theme-recommendation {{
                margin-top: 10px;
                padding: 10px;
                background: white;
                border-radius: 8px;
                font-size: 0.95rem;
                color: #333;
            }}
            
            .strengths-section {{
                margin-top: 20px;
                padding: 20px;
                background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
                border-radius: 15px;
                color: white;
            }}
            
            .strengths-title {{
                font-weight: 700;
                font-size: 1.1rem;
                margin-bottom: 10px;
            }}
            
            .strengths-list {{
                list-style: none;
            }}
            
            .strengths-list li {{
                padding: 8px 0;
                padding-left: 25px;
                position: relative;
            }}
            
            .strengths-list li:before {{
                content: "✓";
                position: absolute;
                left: 0;
                font-weight: bold;
                font-size: 1.2rem;
            }}
            
            .footer {{
                text-align: center;
                margin-top: 60px;
                padding-top: 30px;
                border-top: 2px solid #e0e0e0;
                color: #666;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🎯 QA Coaching Intelligence Report</h1>
                <p class="subtitle">Generated on {datetime.now().strftime('%B %d, %Y at %I:%M %p')}</p>
            </div>
            
            <div class="metrics-grid">
                <div class="metric-card">
                    <div class="metric-label">Total Calls Analyzed</div>
                    <div class="metric-value">{total_calls}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Agents Reviewed</div>
                    <div class="metric-value">{total_agents}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Coaching Opportunities</div>
                    <div class="metric-value">{total_themes}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">Average Sentiment</div>
                    <div class="metric-value">{"😊 " + f"{avg_sentiment:.2f}" if avg_sentiment > 0 else "N/A"}</div>
                </div>
            </div>
            
            <h2 class="section-title">📊 Coaching Theme Distribution</h2>
            <div class="chart-container">
                <div style="display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 25px; padding: 20px;">
    """
    
    # Prepare theme data with icons
    theme_icons = {
        "Active Listening": "👂",
        "Empathy": "❤️",
        "Communication": "💬",
        "Professional": "👔",
        "Resolution": "✅",
        "Problem": "🔍",
        "Solution": "💡",
        "Response Time": "⏱️",
        "Process": "📋",
        "Escalation": "⬆️",
        "Proactive": "🎯",
        "Expectations": "📊",
        "Difficult": "😤",
        "Rapport": "🤝",
        "Knowledge": "📚",
        "Confidence": "💪"
    }
    
    theme_counts = {}
    for agent_data in insights.values():
        for theme in agent_data.get('coaching_themes', []):
            theme_name = theme.get('theme', '')
            theme_counts[theme_name] = theme_counts.get(theme_name, 0) + 1
    
    sorted_themes = sorted(theme_counts.items(), key=lambda x: x[1], reverse=True)[:8]
    max_count = max([c for _, c in sorted_themes]) if sorted_themes else 1
    
    for theme_name, count in sorted_themes:
        # Find matching icon
        icon = "🎯"
        for key, emoji in theme_icons.items():
            if key.lower() in theme_name.lower():
                icon = emoji
                break
        
        percentage = (count / max_count) * 100
        
        html += f"""
                    <div style="background: white; border-radius: 20px; padding: 25px; box-shadow: 0 8px 20px rgba(0,0,0,0.08); transition: all 0.3s ease; border-left: 5px solid #667eea;">
                        <div style="display: flex; align-items: center; gap: 15px; margin-bottom: 15px;">
                            <div style="font-size: 2.5rem;">{icon}</div>
                            <div style="flex: 1;">
                                <div style="font-weight: 700; font-size: 1.1rem; color: #333; margin-bottom: 5px;">{theme_name}</div>
                                <div style="font-size: 0.9rem; color: #666;">Frequency: {count} agents</div>
                            </div>
                        </div>
                        <div style="background: #f0f0f0; height: 12px; border-radius: 10px; overflow: hidden;">
                            <div style="background: linear-gradient(90deg, #667eea 0%, #764ba2 100%); height: 100%; width: {percentage}%; border-radius: 10px; transition: width 0.5s ease;"></div>
                        </div>
                    </div>
        """
    
    html += """
                </div>
            </div>
            
            <h2 class="section-title">📋 Agent Performance Summary</h2>
            <div style="overflow-x: auto; margin: 30px 0;">
                <table style="width: 100%; border-collapse: separate; border-spacing: 0 15px;">
                    <thead>
                        <tr style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);">
                            <th style="padding: 20px; text-align: left; color: white; font-weight: 700; font-size: 1.1rem; border-radius: 10px 0 0 10px;">👤 Agent</th>
                            <th style="padding: 20px; text-align: left; color: white; font-weight: 700; font-size: 1.1rem;">📊 Calls</th>
                            <th style="padding: 20px; text-align: left; color: white; font-weight: 700; font-size: 1.1rem;">🎯 Top Area of Improvement</th>
                            <th style="padding: 20px; text-align: left; color: white; font-weight: 700; font-size: 1.1rem; border-radius: 0 10px 10px 0;">📈 Priority</th>
                        </tr>
                    </thead>
                    <tbody>
    """
    
    # Add agent rows
    for agent_name, agent_data in insights.items():
        themes = agent_data.get('coaching_themes', [])
        calls = agent_data.get('calls_analyzed', 0)
        
        if themes:
            top_theme = themes[0]
            theme_name = top_theme.get('theme', 'N/A')
            priority = top_theme.get('priority', 'low')
            
            # Get icon for theme
            icon = "🎯"
            for key, emoji in theme_icons.items():
                if key.lower() in theme_name.lower():
                    icon = emoji
                    break
            
            # Priority colors
            if priority == 'high':
                priority_color = "linear-gradient(135deg, #f093fb 0%, #f5576c 100%)"
                priority_icon = "🔴"
            elif priority == 'medium':
                priority_color = "linear-gradient(135deg, #ffd89b 0%, #ffa726 100%)"
                priority_icon = "🟡"
            else:
                priority_color = "linear-gradient(135deg, #a8edea 0%, #66bb6a 100%)"
                priority_icon = "🟢"
            
            html += f"""
                        <tr style="background: white; box-shadow: 0 4px 12px rgba(0,0,0,0.05); transition: all 0.3s ease;">
                            <td style="padding: 20px; font-weight: 700; font-size: 1.05rem; color: #333; border-radius: 10px 0 0 10px;">
                                <div style="display: flex; align-items: center; gap: 10px;">
                                    <div style="width: 45px; height: 45px; border-radius: 50%; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); display: flex; align-items: center; justify-content: center; color: white; font-weight: 700; font-size: 1.2rem;">
                                        {agent_name[0].upper()}
                                    </div>
                                    {agent_name}
                                </div>
                            </td>
                            <td style="padding: 20px;">
                                <div style="display: inline-block; background: #f0f0f0; padding: 8px 16px; border-radius: 20px; font-weight: 600; color: #666;">
                                    {calls} calls
                                </div>
                            </td>
                            <td style="padding: 20px;">
                                <div style="display: inline-flex; align-items: center; gap: 10px; background: linear-gradient(135deg, #667eea10 0%, #764ba210 100%); padding: 10px 20px; border-radius: 25px; border: 2px solid #667eea30;">
                                    <span style="font-size: 1.5rem;">{icon}</span>
                                    <span style="font-weight: 600; color: #333;">{theme_name}</span>
                                </div>
                            </td>
                            <td style="padding: 20px; border-radius: 0 10px 10px 0;">
                                <div style="display: inline-flex; align-items: center; gap: 8px; background: {priority_color}; padding: 10px 20px; border-radius: 25px; color: white; font-weight: 700; text-transform: uppercase; font-size: 0.9rem;">
                                    <span>{priority_icon}</span>
                                    <span>{priority}</span>
                                </div>
                            </td>
                        </tr>
            """
        else:
            html += f"""
                        <tr style="background: white; box-shadow: 0 4px 12px rgba(0,0,0,0.05);">
                            <td style="padding: 20px; font-weight: 700; font-size: 1.05rem; color: #333; border-radius: 10px 0 0 10px;">
                                <div style="display: flex; align-items: center; gap: 10px;">
                                    <div style="width: 45px; height: 45px; border-radius: 50%; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); display: flex; align-items: center; justify-content: center; color: white; font-weight: 700; font-size: 1.2rem;">
                                        {agent_name[0].upper()}
                                    </div>
                                    {agent_name}
                                </div>
                            </td>
                            <td style="padding: 20px;">
                                <div style="display: inline-block; background: #f0f0f0; padding: 8px 16px; border-radius: 20px; font-weight: 600; color: #666;">
                                    {calls} calls
                                </div>
                            </td>
                            <td style="padding: 20px;" colspan="2">
                                <div style="color: #999; font-style: italic;">No coaching themes identified</div>
                            </td>
                        </tr>
            """
    
    html += """
                    </tbody>
                </table>
            </div>
            
            <h2 class="section-title">👥 Agent Coaching Details</h2>
            <div class="agent-grid">
    """
    
    # Add agent cards
    for agent_name, agent_data in insights.items():
        themes = agent_data.get('coaching_themes', [])
        strengths = agent_data.get('strengths', [])
        calls_analyzed = agent_data.get('calls_analyzed', 0)
        
        html += f"""
                <div class="agent-card">
                    <div class="agent-header">
                        <div class="agent-name">👤 {agent_name}</div>
                        <div class="agent-stats">
                            <div class="stat-badge">{calls_analyzed} calls</div>
                            <div class="stat-badge">{len(themes)} themes</div>
                        </div>
                    </div>
                    
                    <div class="theme-list">
        """
        
        for theme in themes:
            priority = theme.get('priority', 'low')
            html += f"""
                        <div class="theme-item {priority}">
                            <div class="theme-header">
                                <div class="theme-name">{theme.get('theme', '')}</div>
                                <div class="priority-badge priority-{priority}">{priority}</div>
                            </div>
                            <div class="theme-examples">
                                Frequency: {theme.get('frequency', 0)} instances<br>
                                Examples: {' | '.join(theme.get('examples', [])[:2])}
                            </div>
                            <div class="theme-recommendation">
                                <strong>💡 Recommendation:</strong> {theme.get('recommendation', '')}
                            </div>
                        </div>
            """
        
        if strengths:
            html += f"""
                    </div>
                    <div class="strengths-section">
                        <div class="strengths-title">⭐ Strengths</div>
                        <ul class="strengths-list">
            """
            for strength in strengths:
                html += f"<li>{strength}</li>"
            html += """
                        </ul>
                    </div>
            """
        else:
            html += "</div>"
        
        html += "</div>"
    
    # Close HTML
    html += """
            </div>
            
            <div class="footer">
                <p>QA Coaching Intelligence Platform | Powered by AI Analytics</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html

def generate_powerpoint(insights: Dict, df: pd.DataFrame) -> bytes:
    """Generate PowerPoint presentation"""
    prs = Presentation()
    prs.slide_width = Inches(10)
    prs.slide_height = Inches(7.5)
    
    # Title slide
    slide = prs.slides.add_slide(prs.slide_layouts[6])  # Blank
    
    # Add gradient background
    background = slide.background
    fill = background.fill
    fill.solid()
    fill.fore_color.rgb = RGBColor(102, 126, 234)
    
    # Title
    title_box = slide.shapes.add_textbox(Inches(1), Inches(2.5), Inches(8), Inches(1))
    title_frame = title_box.text_frame
    title_frame.text = "🎯 QA Coaching Intelligence Report"
    title_para = title_frame.paragraphs[0]
    title_para.font.size = Pt(48)
    title_para.font.bold = True
    title_para.font.color.rgb = RGBColor(255, 255, 255)
    title_para.alignment = PP_ALIGN.CENTER
    
    # Subtitle
    subtitle_box = slide.shapes.add_textbox(Inches(1), Inches(4), Inches(8), Inches(0.5))
    subtitle_frame = subtitle_box.text_frame
    subtitle_frame.text = f"Generated on {datetime.now().strftime('%B %d, %Y')}"
    subtitle_para = subtitle_frame.paragraphs[0]
    subtitle_para.font.size = Pt(20)
    subtitle_para.font.color.rgb = RGBColor(255, 255, 255)
    subtitle_para.alignment = PP_ALIGN.CENTER
    
    # Key findings slide
    slide = prs.slides.add_slide(prs.slide_layouts[5])  # Title only
    title = slide.shapes.title
    title.text = "📊 Key Findings"
    
    total_calls = len(df['call_id'].unique()) if 'call_id' in df.columns else len(df)
    total_agents = len(insights)
    total_themes = sum(len(agent_data.get('coaching_themes', [])) for agent_data in insights.values())
    
    # Add metrics
    metrics_text = f"""
    ✓ Analyzed {total_calls} calls
    ✓ Reviewed {total_agents} agents
    ✓ Identified {total_themes} coaching opportunities
    """
    
    text_box = slide.shapes.add_textbox(Inches(2), Inches(2), Inches(6), Inches(3))
    text_frame = text_box.text_frame
    text_frame.text = metrics_text
    for para in text_frame.paragraphs:
        para.font.size = Pt(28)
        para.space_before = Pt(20)
    
    # Agent slides
    for agent_name, agent_data in list(insights.items())[:10]:  # Limit to 10 agents
        slide = prs.slides.add_slide(prs.slide_layouts[5])
        title = slide.shapes.title
        title.text = f"👤 {agent_name}"
        
        themes = agent_data.get('coaching_themes', [])[:3]  # Top 3
        
        y_pos = 2
        for idx, theme in enumerate(themes, 1):
            # Theme box
            theme_box = slide.shapes.add_textbox(Inches(1), Inches(y_pos), Inches(8), Inches(1.2))
            theme_frame = theme_box.text_frame
            
            # Theme name
            theme_frame.text = f"{idx}. {theme.get('theme', '')}"
            theme_para = theme_frame.paragraphs[0]
            theme_para.font.size = Pt(20)
            theme_para.font.bold = True
            
            # Priority
            priority = theme.get('priority', 'low')
            priority_text = theme_frame.add_paragraph()
            priority_text.text = f"Priority: {priority.upper()}"
            priority_text.font.size = Pt(16)
            if priority == 'high':
                priority_text.font.color.rgb = RGBColor(245, 87, 108)
            elif priority == 'medium':
                priority_text.font.color.rgb = RGBColor(255, 167, 38)
            else:
                priority_text.font.color.rgb = RGBColor(102, 187, 106)
            
            # Recommendation
            rec_text = theme_frame.add_paragraph()
            rec_text.text = f"💡 {theme.get('recommendation', '')[:100]}..."
            rec_text.font.size = Pt(14)
            rec_text.font.italic = True
            
            y_pos += 1.5
    
    # Save to bytes
    ppt_bytes = io.BytesIO()
    prs.save(ppt_bytes)
    ppt_bytes.seek(0)
    return ppt_bytes.getvalue()


# Sidebar
with st.sidebar:
    st.markdown("### 🎯 QA Coaching Intelligence")
    st.markdown("---")
    
    st.markdown("#### LLM Provider")
    llm_provider = st.radio(
        "Select provider:",
        ["OpenRouter", "Local LLM (LM Studio/Ollama)"],
        key="llm_provider_radio"
    )
    
    if llm_provider == "OpenRouter":
        st.session_state.llm_provider = "openrouter"
        
        # Try to get API key from secrets first
        try:
            api_key = st.secrets.get("OPENROUTER_API_KEY", "")
            if api_key:
                st.session_state.openrouter_api_key = api_key
                st.success("✅ API key loaded from secrets")
            else:
                # Fallback to manual input
                api_key = st.text_input("OpenRouter API Key:", type="password", key="api_key_input")
                st.session_state.openrouter_api_key = api_key
                if not api_key:
                    st.warning("⚠️ API key required")
        except:
            # No secrets available, use manual input
            api_key = st.text_input("OpenRouter API Key:", type="password", key="api_key_input")
            st.session_state.openrouter_api_key = api_key
            if not api_key:
                st.warning("⚠️ API key required")
    else:
        st.session_state.llm_provider = "local"
        local_url = st.text_input(
            "Local LLM URL:",
            value="http://localhost:1234/v1/chat/completions",
            key="local_llm_url_input"
        )
        st.session_state.local_llm_url = local_url
        st.info("💡 Make sure LM Studio or Ollama is running")
    
    st.markdown("---")
    st.markdown("#### Analysis Model")
    
    if st.session_state.llm_provider == "openrouter":
        analysis_model = st.selectbox(
            "Select model:",
            options=list(MODELS.keys()),
            format_func=lambda x: f"{MODELS[x]['stars']} {MODELS[x]['name']}" + (" ✨" if MODELS[x].get('recommended') else ""),
            index=0
        )
        st.info(f"**Best for:** {MODELS[analysis_model]['best_for']}\n\n**Speed:** {MODELS[analysis_model]['speed']}")
    else:
        analysis_model = st.text_input(
            "Model name:",
            value="llama3",
            help="Enter the model name from LM Studio/Ollama"
        )
    
    st.markdown("---")
    st.markdown("#### Processing Settings")
    
    max_concurrent = st.slider(
        "Concurrent requests:",
        min_value=5,
        max_value=20,
        value=10,
        help="More = faster but may hit rate limits"
    )
    st.session_state.max_concurrent = max_concurrent
    
    calls_per_minute = st.number_input(
        "Rate limit (calls/min):",
        min_value=10,
        max_value=100,
        value=50,
        help="Your OpenRouter/API rate limit"
    )
    st.session_state.calls_per_minute = calls_per_minute
    
    st.markdown("---")
    st.markdown("#### Coaching Themes")
    theme_option = st.radio("Theme source:", ["Pre-loaded", "Custom", "Both"])
    
    if theme_option == "Custom":
        custom_themes = st.text_area("Enter themes (one per line):", height=150)
        coaching_themes = [t.strip() for t in custom_themes.split('\n') if t.strip()]
    elif theme_option == "Both":
        custom_themes = st.text_area("Add custom themes:", height=100)
        additional = [t.strip() for t in custom_themes.split('\n') if t.strip()]
        coaching_themes = DEFAULT_THEMES + additional
    else:
        coaching_themes = DEFAULT_THEMES
    
    st.session_state.coaching_themes = coaching_themes
    st.session_state.analysis_model = analysis_model if st.session_state.llm_provider == "openrouter" else analysis_model
    
    st.caption(f"{len(coaching_themes)} themes active")

# Main content
st.markdown("<div style='text-align: center; padding: 20px;'>", unsafe_allow_html=True)
st.markdown("<h1 style='font-size: 3.5rem; font-weight: 700; color: #2986cc;'>🎯 QA Coaching Intelligence</h1>", unsafe_allow_html=True)
st.markdown("<p style='font-size: 1.3rem; color: white; opacity: 0.9;'>Transform Every Call into Coaching Excellence</p>", unsafe_allow_html=True)
st.markdown("</div>", unsafe_allow_html=True)

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["📤 Upload & Process", "📊 Dashboard", "💬 Q&A Chat", "💾 Export & Session"])

with tab1:
    st.markdown("<div style='background: rgba(255,255,255,0.95); padding: 40px; border-radius: 20px; margin: 20px 0;'>", unsafe_allow_html=True)
    
    st.markdown("### 📤 Step 1: Upload Files")
    uploaded_files = st.file_uploader(
        "Supported: CSV, XLSX, XLS, TXT, Parquet",
        type=['csv', 'xlsx', 'xls', 'txt', 'parquet'],
        accept_multiple_files=True
    )
    
    if uploaded_files:
        st.success(f"✅ {len(uploaded_files)} file(s) uploaded")
        
        if st.button("🔄 Load & Convert to Parquet", use_container_width=True):
            with st.spinner("Loading and converting files..."):
                all_data = []
                
                for uploaded_file in uploaded_files:
                    df = load_file_to_dataframe(uploaded_file)
                    if df is not None:
                        all_data.append(df)
                
                if all_data:
                    combined_df = pd.concat(all_data, ignore_index=True)
                    
                    # Convert to parquet
                    parquet_bytes = convert_to_parquet(combined_df, 'transcripts.parquet')
                    st.session_state.transcripts_parquet = parquet_bytes
                    st.session_state.raw_df = combined_df
                    
                    # Load into DuckDB
                    conn = st.session_state.duckdb_conn
                    conn.execute("DROP TABLE IF EXISTS transcripts")
                    conn.execute("CREATE TABLE transcripts AS SELECT * FROM combined_df")
                    
                    st.success(f"✅ Loaded {len(combined_df):,} rows | Size: {len(parquet_bytes) / 1024 / 1024:.2f} MB (Parquet)")
                    
                    st.session_state.data_loaded = True
    
    if st.session_state.get('data_loaded'):
        st.markdown("---")
        st.markdown("### 🗂️ Step 2: Map Columns")
        
        df = st.session_state.raw_df
        available_columns = list(df.columns)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("#### Required Fields")
            call_id_col = st.selectbox("Call ID column:", [""] + available_columns, key="call_id_col")
            agent_col = st.selectbox("Agent column:", [""] + available_columns, key="agent_col")
            transcript_col = st.selectbox("Transcript column:", [""] + available_columns, key="transcript_col")
        
        with col2:
            st.markdown("#### Optional Fields")
            sentiment_col = st.selectbox("Sentiment Score:", ["None"] + available_columns, key="sentiment_col")
            timestamp_col = st.selectbox("Timestamp:", ["None"] + available_columns, key="timestamp_col")
            duration_col = st.selectbox("Call Duration:", ["None"] + available_columns, key="duration_col")
        
        with col3:
            st.markdown("#### Additional Metrics")
            custom_cols = st.multiselect(
                "Other columns to include:",
                [c for c in available_columns if c not in [call_id_col, agent_col, transcript_col, sentiment_col, timestamp_col, duration_col]],
                key="custom_cols"
            )
        
        # Validate required fields
        if call_id_col and agent_col and transcript_col:
            st.success("✅ Required columns mapped")
            
            if st.button("📊 Run Pre-Analysis (DuckDB)", use_container_width=True):
                with st.spinner("Running analytics..."):
                    conn = st.session_state.duckdb_conn
                    
                    # Store column mapping
                    st.session_state.column_mapping = {
                        'call_id': call_id_col,
                        'agent': agent_col,
                        'transcript': transcript_col,
                        'sentiment': sentiment_col if sentiment_col != "None" else None,
                        'timestamp': timestamp_col if timestamp_col != "None" else None,
                        'duration': duration_col if duration_col != "None" else None,
                        'custom': custom_cols
                    }
                    
                    # Parse transcripts and expand
                    expanded_rows = []
                    parse_failures = []
                    
                    for idx, row in df.iterrows():
                        call_id = row[call_id_col]
                        agent_name = row[agent_col]
                        transcript_text = row[transcript_col]
                        sentiment = row[sentiment_col] if sentiment_col != "None" and sentiment_col in row else None
                        
                        # Parse transcript
                        turns = parse_multiline_transcript(str(transcript_text))
                        
                        if not turns:
                            parse_failures.append({
                                'call_id': call_id,
                                'agent': agent_name,
                                'transcript_preview': str(transcript_text)[:200]
                            })
                            continue
                        
                        for turn in turns:
                            expanded_rows.append({
                                'call_id': call_id,
                                'agent': agent_name,
                                'timestamp': turn['timestamp'],
                                'speaker': turn['speaker'],
                                'message': turn['message'],
                                'sentiment_score': sentiment,
                                'original_transcript': transcript_text
                            })
                    
                    if parse_failures:
                        st.warning(f"⚠️ Failed to parse {len(parse_failures)} transcripts. Check format.")
                        with st.expander("Show failed transcripts"):
                            st.write(pd.DataFrame(parse_failures))
                    
                    if not expanded_rows:
                        st.error("❌ No transcripts could be parsed. Please check your data format.")
                        st.info("Expected formats:\n- `[12:30:08 AGENT]: message`\n- `2025-02-07 13:17:57 +0000 Agent: message | 2025-02-07 13:18:01 +0000 Customer: response`")
                        st.stop()
                    
                    expanded_df = pd.DataFrame(expanded_rows)
                    
                    # Store in session state first
                    st.session_state.processed_df = expanded_df
                    
                    # Reload into DuckDB using direct DataFrame reference
                    conn.execute("DROP TABLE IF EXISTS transcripts")
                    conn.execute("CREATE TABLE transcripts AS SELECT * FROM expanded_df")
                    
                    # Run DuckDB analytics
                    analytics = {}
                    
                    # 1. Call volumes
                    analytics['total_calls'] = conn.execute("SELECT COUNT(DISTINCT call_id) as count FROM transcripts").fetchone()[0]
                    analytics['total_agents'] = conn.execute("SELECT COUNT(DISTINCT agent) as count FROM transcripts").fetchone()[0]
                    
                    # 2. Per-agent stats
                    agent_stats = conn.execute("""
                        SELECT 
                            agent,
                            COUNT(DISTINCT call_id) as total_calls,
                            COUNT(*) as total_messages,
                            SUM(CASE WHEN speaker = 'agent' THEN 1 ELSE 0 END) as agent_messages,
                            SUM(CASE WHEN speaker = 'customer' THEN 1 ELSE 0 END) as customer_messages,
                            AVG(LENGTH(message)) as avg_message_length
                        FROM transcripts
                        GROUP BY agent
                        ORDER BY total_calls DESC
                    """).fetchdf()
                    analytics['agent_stats'] = agent_stats
                    
                    # 3. Sentiment analysis (if available)
                    if sentiment_col != "None":
                        sentiment_stats = conn.execute("""
                            SELECT 
                                agent,
                                AVG(sentiment_score) as avg_sentiment,
                                MIN(sentiment_score) as min_sentiment,
                                MAX(sentiment_score) as max_sentiment,
                                COUNT(CASE WHEN sentiment_score < 0.5 THEN 1 END) as low_sentiment_calls
                            FROM (
                                SELECT DISTINCT call_id, agent, sentiment_score 
                                FROM transcripts 
                                WHERE sentiment_score IS NOT NULL
                            )
                            GROUP BY agent
                        """).fetchdf()
                        analytics['sentiment_stats'] = sentiment_stats
                    
                    # 4. Message flow analysis
                    flow_stats = conn.execute("""
                        SELECT 
                            agent,
                            AVG(turns_per_call) as avg_turns,
                            AVG(agent_response_ratio) as avg_response_ratio
                        FROM (
                            SELECT 
                                call_id,
                                agent,
                                COUNT(*) as turns_per_call,
                                SUM(CASE WHEN speaker = 'agent' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as agent_response_ratio
                            FROM transcripts
                            GROUP BY call_id, agent
                        )
                        GROUP BY agent
                    """).fetchdf()
                    analytics['flow_stats'] = flow_stats
                    
                    st.session_state.pre_analytics = analytics
                    st.session_state.pre_analysis_done = True
                    
                    st.success("✅ Pre-analysis complete!")
                    st.rerun()
        else:
            st.warning("⚠️ Please map all required columns (Call ID, Agent, Transcript)")
    
    st.markdown("</div>", unsafe_allow_html=True)

with tab2:
    if st.session_state.get('pre_analysis_done'):
        analytics = st.session_state.pre_analytics
        
        st.markdown("### 📊 Pre-Analysis Dashboard (DuckDB)")
        
        # Key metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Calls", f"{analytics['total_calls']:,}")
        with col2:
            st.metric("Total Agents", analytics['total_agents'])
        with col3:
            if 'sentiment_stats' in analytics:
                avg_sentiment = analytics['sentiment_stats']['avg_sentiment'].mean()
                st.metric("Avg Sentiment", f"{avg_sentiment:.2f}")
            else:
                st.metric("Avg Sentiment", "N/A")
        with col4:
            total_messages = analytics['agent_stats']['total_messages'].sum()
            st.metric("Total Messages", f"{total_messages:,}")
        
        st.markdown("---")
        
        # Agent performance table
        st.markdown("#### 👥 Agent Statistics")
        
        # Merge stats
        display_df = analytics['agent_stats'].copy()
        
        if 'sentiment_stats' in analytics:
            display_df = display_df.merge(
                analytics['sentiment_stats'][['agent', 'avg_sentiment', 'low_sentiment_calls']],
                on='agent',
                how='left'
            )
        
        if 'flow_stats' in analytics:
            display_df = display_df.merge(
                analytics['flow_stats'][['agent', 'avg_turns', 'avg_response_ratio']],
                on='agent',
                how='left'
            )
        
        # Format for display
        display_df['avg_message_length'] = display_df['avg_message_length'].round(1)
        if 'avg_sentiment' in display_df.columns:
            display_df['avg_sentiment'] = display_df['avg_sentiment'].round(2)
        if 'avg_turns' in display_df.columns:
            display_df['avg_turns'] = display_df['avg_turns'].round(1)
        if 'avg_response_ratio' in display_df.columns:
            display_df['avg_response_ratio'] = (display_df['avg_response_ratio'] * 100).round(1)
        
        st.dataframe(display_df, use_container_width=True, height=400)
        
        st.markdown("---")
        
        # Coaching insights section
        if not st.session_state.get('processed'):
            st.markdown("### 🎯 Generate Coaching Insights")
            st.info("📌 Pre-analysis complete! Now optionally generate AI-powered coaching themes.")
            
            # Validation
            if st.session_state.llm_provider == "openrouter" and not st.session_state.get('openrouter_api_key'):
                st.error("❌ Please provide OpenRouter API key in sidebar")
            else:
                if st.button("🚀 Generate Coaching Themes (LLM)", use_container_width=True, type="primary"):
                    with st.spinner("Generating coaching insights..."):
                        # Get agents data
                        df = st.session_state.processed_df
                        agents = df.groupby('agent')
                        agents_data = [(agent, group) for agent, group in agents]
                        total_agents = len(agents_data)
                        
                        # Get coaching themes from session state (set in sidebar)
                        themes = st.session_state.get('coaching_themes', DEFAULT_THEMES)
                        
                        st.info(f"Processing {total_agents} agents with {len(themes)} coaching themes")
                        
                        # Progress tracking
                        progress_bar = st.progress(0.0)
                        status_text = st.empty()
                        log_area = st.empty()
                        
                        start_time = time.time()
                        
                        # Run parallel processing
                        async def run_processing():
                            insights = await process_all_agents_parallel(
                                agents_data,
                                themes,
                                st.session_state.get('analysis_model', 'qwen/qwen3-coder:free'),
                                st.session_state.llm_provider,
                                st.session_state.get('openrouter_api_key'),
                                st.session_state.get('local_llm_url'),
                                max_concurrent=st.session_state.get('max_concurrent', 10),
                                calls_per_minute=st.session_state.get('calls_per_minute', 50)
                            )
                            return insights
                        
                        import nest_asyncio
                        nest_asyncio.apply()
                        
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        
                        try:
                            status_text.text("Processing agents in parallel...")
                            
                            # Create log container
                            with st.expander("📋 Processing Logs", expanded=True):
                                log_container = st.empty()
                                logs = []
                                
                                # Monkey patch print to capture logs
                                import sys
                                from io import StringIO
                                old_stdout = sys.stdout
                                sys.stdout = log_buffer = StringIO()
                            
                            insights = loop.run_until_complete(run_processing())
                            
                            # Restore stdout and get logs
                            sys.stdout = old_stdout
                            log_text = log_buffer.getvalue()
                            if log_text:
                                with st.expander("📋 Processing Logs", expanded=True):
                                    st.code(log_text)
                            
                            elapsed = time.time() - start_time
                            
                            progress_bar.progress(1.0)
                            status_text.text(f"✅ Processed {len(insights)} agents in {elapsed:.1f}s")
                            
                            if not insights:
                                log_area.error("⚠️ No insights generated. Check logs above.")
                            
                            st.session_state.coaching_insights = insights
                            st.session_state.processed = True
                            
                            time.sleep(1)
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"Processing failed: {str(e)}")
                            import traceback
                            log_area.code(traceback.format_exc())
                        finally:
                            loop.close()
        
        # Show coaching insights if available
        if st.session_state.get('processed'):
            st.markdown("---")
            st.markdown("### 🎯 AI-Powered Coaching Insights")
            
            insights = st.session_state.coaching_insights
            df = st.session_state.processed_df
            
            # Generate HTML report
            html_report = generate_html_report(insights, df)
            st.components.v1.html(html_report, height=2000, scrolling=True)
            
            st.session_state.html_report = html_report
    
    else:
        st.info("👆 Upload files and run pre-analysis first!")

with tab3:
    if st.session_state.processed:
        st.markdown("### 💬 Ask Questions About Your Data")
        
        # Chat model selector
        with st.expander("⚙️ Chat Settings"):
            chat_model = st.selectbox(
                "Chat model:",
                options=list(MODELS.keys()),
                format_func=lambda x: f"{MODELS[x]['stars']} {MODELS[x]['name']}",
                index=list(MODELS.keys()).index("mistralai/mistral-nemo:free")
            )
        
        # Display chat history
        for msg in st.session_state.chat_history:
            if msg['role'] == 'user':
                st.markdown(f"<div class='chat-message user-message'><strong>You:</strong> {msg['content']}</div>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div class='chat-message assistant-message'><strong>Assistant:</strong> {msg['content']}</div>", unsafe_allow_html=True)
        
        # Chat input
        user_question = st.text_input("Ask a question:", key="chat_input")
        
        col1, col2 = st.columns([1, 5])
        with col1:
            send_btn = st.button("Send", use_container_width=True)
        with col2:
            clear_btn = st.button("Clear History", use_container_width=True)
        
        if clear_btn:
            st.session_state.chat_history = []
            st.rerun()
        
        if send_btn and user_question:
            # Add to history
            st.session_state.chat_history.append({"role": "user", "content": user_question})
            
            # Prepare context
            insights = st.session_state.coaching_insights
            df = st.session_state.processed_df
            
            # Simple query routing
            question_lower = user_question.lower()
            
            # Check if it's a SQL-like question
            if any(kw in question_lower for kw in ['how many', 'count', 'average', 'total', 'list all']):
                # Try to answer with DuckDB
                try:
                    conn = st.session_state.duckdb_conn
                    
                    if 'how many calls' in question_lower:
                        result = conn.execute("SELECT COUNT(DISTINCT call_id) as count FROM transcripts").fetchone()
                        answer = f"There are {result[0]} calls in the dataset."
                    elif 'how many agents' in question_lower:
                        result = conn.execute("SELECT COUNT(DISTINCT agent) as count FROM transcripts WHERE agent IS NOT NULL").fetchone()
                        answer = f"There are {result[0]} agents in the dataset."
                    elif 'average sentiment' in question_lower:
                        if 'sentiment_score' in df.columns:
                            result = conn.execute("SELECT AVG(sentiment_score) as avg FROM transcripts").fetchone()
                            answer = f"The average sentiment score is {result[0]:.2f}."
                        else:
                            answer = "Sentiment data not available in the transcripts."
                    else:
                        answer = "I can answer questions about call counts, agent counts, and sentiment averages. Try rephrasing!"
                        
                    st.session_state.chat_history.append({"role": "assistant", "content": answer})
                    st.rerun()
                    
                except Exception as e:
                    answer = f"I encountered an error: {str(e)}"
                    st.session_state.chat_history.append({"role": "assistant", "content": answer})
                    st.rerun()
            
            else:
                # Use LLM for coaching questions
                context = f"Coaching insights for {len(insights)} agents:\n\n"
                for agent, data in insights.items():
                    context += f"{agent}: {len(data.get('coaching_themes', []))} coaching themes\n"
                    for theme in data.get('coaching_themes', [])[:2]:
                        context += f"  - {theme.get('theme', '')} ({theme.get('priority', '')})\n"
                
                messages = [
                    {"role": "system", "content": "You are a helpful QA coaching assistant. Answer questions based on the coaching data provided. Be specific and cite agent names when relevant."},
                    {"role": "user", "content": f"Context:\n{context}\n\nQuestion: {user_question}"}
                ]
                
                response = call_llm(chat_model, messages, temperature=0.5, is_json=False)
                
                if response and 'choices' in response:
                    answer = response['choices'][0]['message']['content']
                    st.session_state.chat_history.append({"role": "assistant", "content": answer})
                    st.rerun()
                else:
                    st.error("Failed to get response from chat model")
    else:
        st.info("👆 Process transcripts first to enable Q&A!")

with tab4:
    if st.session_state.processed:
        st.markdown("### 📥 Download Reports")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # HTML Report
            if 'html_report' in st.session_state:
                st.download_button(
                    "📊 HTML Report",
                    data=st.session_state.html_report,
                    file_name=f"coaching_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
                    mime="text/html",
                    use_container_width=True
                )
        
        with col2:
            # CSV Export
            if st.button("📄 Export CSV", use_container_width=True):
                insights = st.session_state.coaching_insights
                rows = []
                for agent, data in insights.items():
                    for theme in data.get('coaching_themes', []):
                        rows.append({
                            'agent': agent,
                            'theme': theme.get('theme', ''),
                            'priority': theme.get('priority', ''),
                            'frequency': theme.get('frequency', 0),
                            'examples': ' | '.join(theme.get('examples', [])),
                            'recommendation': theme.get('recommendation', '')
                        })
                
                export_df = pd.DataFrame(rows)
                csv = export_df.to_csv(index=False)
                
                st.download_button(
                    "Download CSV",
                    data=csv,
                    file_name=f"coaching_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
        
        with col3:
            # Excel Export
            if st.button("📗 Export Excel", use_container_width=True):
                insights = st.session_state.coaching_insights
                
                # Create Excel file
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    # Summary sheet
                    summary_data = []
                    for agent, data in insights.items():
                        summary_data.append({
                            'Agent': agent,
                            'Calls': data.get('calls_analyzed', 0),
                            'Coaching Themes': len(data.get('coaching_themes', [])),
                            'High Priority': sum(1 for t in data.get('coaching_themes', []) if t.get('priority') == 'high')
                        })
                    pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
                    
                    # Detail sheet
                    rows = []
                    for agent, data in insights.items():
                        for theme in data.get('coaching_themes', []):
                            rows.append({
                                'Agent': agent,
                                'Theme': theme.get('theme', ''),
                                'Priority': theme.get('priority', ''),
                                'Frequency': theme.get('frequency', 0),
                                'Recommendation': theme.get('recommendation', '')
                            })
                    pd.DataFrame(rows).to_excel(writer, sheet_name='Coaching Details', index=False)
                
                excel_data = output.getvalue()
                
                st.download_button(
                    "Download Excel",
                    data=excel_data,
                    file_name=f"coaching_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
        
        # PowerPoint export
        col4, col5, col6 = st.columns(3)
        with col4:
            if st.button("📽️ Export PowerPoint", use_container_width=True):
                with st.spinner("Generating PowerPoint..."):
                    ppt_data = generate_powerpoint(st.session_state.coaching_insights, st.session_state.processed_df)
                    st.download_button(
                        "Download PowerPoint",
                        data=ppt_data,
                        file_name=f"coaching_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pptx",
                        mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        use_container_width=True
                    )
        
        st.markdown("---")
        st.markdown("### 💾 Session Management")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("💾 Save Session", use_container_width=True):
                # Save all session data to parquet
                session_data = {
                    'transcripts': st.session_state.processed_df,
                    'insights': pd.DataFrame([
                        {'agent': agent, 'insights_json': json.dumps(data)}
                        for agent, data in st.session_state.coaching_insights.items()
                    ]),
                    'metadata': pd.DataFrame([{
                        'processed_at': datetime.now().isoformat(),
                        'model_used': analysis_model,
                        'total_calls': len(st.session_state.processed_df['call_id'].unique()),
                        'total_agents': len(st.session_state.coaching_insights)
                    }])
                }
                
                # Convert each to parquet
                output = io.BytesIO()
                
                # For now, save insights as JSON in parquet
                session_df = pd.DataFrame([{
                    'data_type': 'insights',
                    'content': json.dumps(st.session_state.coaching_insights)
                }])
                
                parquet_bytes = convert_to_parquet(session_df, 'session.parquet')
                
                st.download_button(
                    "📥 Download Session File",
                    data=parquet_bytes,
                    file_name=f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet",
                    mime="application/octet-stream",
                    use_container_width=True
                )
        
        with col2:
            session_file = st.file_uploader("📂 Load Session", type=['parquet'])
            if session_file:
                # Load session
                session_df = pd.read_parquet(session_file)
                if not session_df.empty:
                    content = json.loads(session_df.iloc[0]['content'])
                    st.session_state.coaching_insights = content
                    st.session_state.processed = True
                    st.success("✅ Session loaded!")
                    st.rerun()
    
    else:
        st.info("👆 Process transcripts first!")

# Footer
st.markdown("<br><br>", unsafe_allow_html=True)
st.markdown("<div style='text-align: center; color: white; opacity: 0.7; padding: 20px;'>", unsafe_allow_html=True)
st.markdown("QA Coaching Intelligence Platform | Powered by AI Analytics", unsafe_allow_html=True)
st.markdown("</div>", unsafe_allow_html=True)
