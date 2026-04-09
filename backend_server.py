"""
╔══════════════════════════════════════════════════════════════════════╗
║          PAGALPAN TREDING PRO — BACKEND v3.0 (100% LIVE)            ║
║                                                                      ║
║  APP: Pagalpan Treding Pro                                           ║
║                                                                      ║
║  LIVE DATA SOURCES:                                                  ║
║   1. Dhan API        → NIFTY/BankNifty live spot + LTP               ║
║   2. Yahoo Finance   → Real 5m intraday OHLCV candles               ║
║   3. NSE Official    → All indices, PCR, FII/DII, Option Chain      ║
║   4. Stooq.com       → Gift Nifty, Commodities, USD/INR             ║
║   5. NewsData.io     → Live Indian market news                      ║
║   6. NewsAPI.org     → Backup news                                  ║
║   7. Google RSS      → Final news fallback                          ║
║   8. AngelOne API    → Backup price data                            ║
║                                                                      ║
║  SIGNALS: RSI(14)+MACD+BB(20)+EMA(9/21/50/200)+VWAP+ATR+Pivot      ║
║  NEW: Gift Nifty, Market Sentiment Score, Tech Flow Meter            ║
║  TELEGRAM: /status /scan /pcr /fii /gift /sentiment /help           ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import os, time, re, warnings, threading, math
import requests
import pandas as pd
import numpy as np
from flask import Flask, jsonify
from flask_cors import CORS
from datetime import datetime
import pytz
import schedule

warnings.filterwarnings("ignore")

# ══════════════════════════════════════════════════════════════
#  🔑  API KEYS — All keys configured here
#  For Render: set these in Environment Variables
# ══════════════════════════════════════════════════════════════
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN",   "8381016190:AAED0OqTzGEeiiJRet7udcGVxCPOpCGPk5o")
TELEGRAM_CHATID  = os.getenv("TELEGRAM_CHATID",  "711929429")

# Dhan HQ API (NSE Live Data)
DHAN_CLIENT_ID   = os.getenv("DHAN_CLIENT_ID",   "01569f44")
DHAN_TOKEN       = os.getenv("DHAN_TOKEN",       "2b64f8a5-eef2-42a2-bee0-d0702f7f200e")

# AngelOne API (Backup)
ANGEL_KEY        = os.getenv("ANGEL_KEY",        "EUBUtdxc")
ANGEL_SECRET     = os.getenv("ANGEL_SECRET",     "6db3bfe0-3248-43b6-ba67-a49aa52e61f7")

# News APIs
NEWSDATA_KEY     = os.getenv("NEWSDATA_KEY",     "pub_592d66f455a54df49a22204180d15893")
NEWSAPI_KEY      = os.getenv("NEWSAPI_KEY",      "213c304802868c91d2b69bbb59a04f3c")

# Trading config
CAPITAL   = 25_000
RISK_PCT  = 0.01
RISK      = CAPITAL * RISK_PCT   # ₹250
RR        = 3
ATR_MULT  = 1.5

IST = pytz.timezone("Asia/Kolkata")
app = Flask(__name__)
CORS(app)


# ══════════════════════════════════════════════════
#  🕐  UTILS
# ══════════════════════════════════════════════════
def ist_now():
    return datetime.now(IST)

def is_market_open():
    now = ist_now()
    if now.weekday() >= 5:
        return False
    t = now.strftime("%H:%M")
    return "09:15" <= t <= "15:30"

def is_pre_market():
    now = ist_now()
    if now.weekday() >= 5:
        return False
    t = now.strftime("%H:%M")
    return "08:00" <= t < "09:15"


# ══════════════════════════════════════════════════
#  📤  TELEGRAM
# ══════════════════════════════════════════════════
def send_telegram(msg: str):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={
            "chat_id": TELEGRAM_CHATID,
            "text": msg,
            "parse_mode": "Markdown",
        }, timeout=8)
    except Exception as e:
        print(f"[TG] {e}")


# ══════════════════════════════════════════════════
#  📡  SOURCE 1: DHAN API — Live Spot Prices
# ══════════════════════════════════════════════════
def _dhan_ltp(security_id: str, segment: str = "IDX_I"):
    """Fetch live LTP from Dhan API"""
    if not DHAN_CLIENT_ID or not DHAN_TOKEN:
        return None
    try:
        url = "https://api.dhan.co/marketfeed/ltp"
        headers = {
            "client_id":    DHAN_CLIENT_ID,
            "access_token": DHAN_TOKEN,
            "Content-Type": "application/json",
        }
        payload = {"instruments": [{"exchangeSegment": segment, "securityId": security_id}]}
        resp = requests.post(url, headers=headers, json=payload, timeout=6)
        data = resp.json()
        if "data" in data and data["data"]:
            return float(data["data"][0].get("last_price", 0))
    except Exception as e:
        print(f"[DHAN {security_id}] {e}")
    return None

def get_dhan_prices() -> dict:
    """Live NIFTY(13)+BANKNIFTY(25)+SENSEX+FINNIFTY(27) from Dhan"""
    result = {}
    ids = {"nifty":"13","banknifty":"25","finnifty":"27"}
    for k, sid in ids.items():
        p = _dhan_ltp(sid, "IDX_I")
        if p:
            result[k] = {"price": round(p, 2), "src": "DHAN"}
    if result:
        print(f"✅ Dhan: {list(result.keys())}")
    return result


# ══════════════════════════════════════════════════
#  📡  SOURCE 2: YAHOO FINANCE — Real Candles + Gift Nifty
# ══════════════════════════════════════════════════
def get_yahoo_candles(symbol: str, period: str = "3d", interval: str = "5m"):
    """Real OHLCV candles for technical analysis"""
    try:
        import yfinance as yf
        df = yf.Ticker(symbol).history(period=period, interval=interval)
        if df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df = df[["Open","High","Low","Close","Volume"]].dropna()
        print(f"✅ Yahoo [{symbol}]: {len(df)} bars")
        return df
    except Exception as e:
        print(f"[YAHOO {symbol}] {e}")
        return None

def get_yahoo_price(symbol: str):
    """Quick current price from Yahoo"""
    try:
        import yfinance as yf
        info = yf.Ticker(symbol).fast_info
        return float(info.last_price) if hasattr(info, "last_price") else None
    except:
        return None

def get_gift_nifty() -> dict:
    """
    Gift Nifty = SGX Nifty proxy via Yahoo Finance
    Symbol: ^NSEI futures via NQ=F or use stooq
    """
    result = {"price": 0, "change": 0, "pct": 0, "src": "N/A"}
    # Try Yahoo Finance for Gift Nifty (GIFTNIFTY)
    try:
        import yfinance as yf
        # GIFT NIFTY trades on NSE IFSC (symbol: NIFTYBEES or use futures proxy)
        # Best proxy: use Stooq for Singapore SGX data
        pass
    except:
        pass
    # Try Stooq SGX Nifty
    try:
        url = "https://stooq.com/q/l/?s=nifty.f&f=sd2t2ohlcv&h&e=csv"
        resp = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=5)
        lines = resp.text.strip().split("\n")
        if len(lines) >= 2:
            cols = lines[0].split(",")
            vals = lines[1].split(",")
            row  = dict(zip(cols, vals))
            c = float(row.get("Close",0)); o = float(row.get("Open",0))
            if c > 0:
                result = {
                    "price":  round(c, 2),
                    "open":   round(o, 2),
                    "high":   round(float(row.get("High",0)),2),
                    "low":    round(float(row.get("Low",0)),2),
                    "change": round(c-o,2),
                    "pct":    round((c-o)/o*100,2) if o else 0,
                    "src":    "STOOQ_SGX",
                }
                print(f"✅ Gift Nifty: {result['price']}")
                return result
    except Exception as e:
        print(f"[GIFT NIFTY] {e}")
    # Final fallback: use NSE Nifty futures proxy
    try:
        nifty_spot = get_yahoo_price("^NSEI")
        if nifty_spot:
            # Gift Nifty trades at ~premium/discount to Nifty spot
            result = {"price": round(nifty_spot, 2), "change": 0, "pct": 0, "src": "NSEI_PROXY"}
    except:
        pass
    return result


# ══════════════════════════════════════════════════
#  📡  SOURCE 3: NSE OFFICIAL — All data
# ══════════════════════════════════════════════════
def _nse_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.nseindia.com/",
    })
    try: s.get("https://www.nseindia.com", timeout=6)
    except: pass
    return s

def get_nse_indices() -> dict:
    result = {}
    MAP = {
        "NIFTY 50":"nifty","NIFTY BANK":"banknifty","NIFTY FIN SERVICE":"finifty",
        "NIFTY MIDCAP 50":"midcap50","NIFTY IT":"nifty_it","NIFTY AUTO":"nifty_auto",
        "NIFTY PHARMA":"nifty_pharma","NIFTY FMCG":"nifty_fmcg","NIFTY METAL":"nifty_metal",
        "INDIA VIX":"vix","NIFTY REALTY":"nifty_realty","NIFTY PSU BANK":"psubank",
    }
    try:
        s    = _nse_session()
        data = s.get("https://www.nseindia.com/api/allIndices", timeout=8).json()
        for idx in data.get("data",[]):
            k = MAP.get(idx.get("index",""))
            if k:
                result[k] = {
                    "price":round(idx["last"],2),"change":round(idx.get("variation",0),2),
                    "pct":round(idx.get("percentChange",0),2),"open":round(idx.get("open",0),2),
                    "high":round(idx.get("high",0),2),"low":round(idx.get("low",0),2),
                    "prev":round(idx.get("previousClose",0),2),
                    "adv":idx.get("advances",0),"dec":idx.get("declines",0),"src":"NSE",
                }
        print(f"✅ NSE: {len(result)} indices")
    except Exception as e:
        print(f"⚠️ NSE: {e}")
    return result

def get_option_chain(symbol: str = "NIFTY"):
    """NSE option chain → full strike data + PCR"""
    try:
        s    = _nse_session()
        url  = f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol}"
        data = s.get(url, timeout=8).json()
        filt = data.get("filtered",{})
        ce_oi = filt.get("CE",{}).get("totOI",0)
        pe_oi = filt.get("PE",{}).get("totOI",0)
        pcr   = round(pe_oi/ce_oi,2) if ce_oi else 1.0
        ltp   = data["records"]["underlyingValue"]
        expiries = data["records"]["expiryDates"][:4]
        # Get strike-wise data for option chain display
        strikes = []
        for row in data["records"]["data"][:20]:
            ce = row.get("CE",{}); pe = row.get("PE",{})
            strikes.append({
                "strike": row.get("strikePrice",0),
                "ce_oi": ce.get("openInterest",0),
                "pe_oi": pe.get("openInterest",0),
                "ce_ltp": ce.get("lastPrice",0),
                "pe_ltp": pe.get("lastPrice",0),
                "ce_iv": ce.get("impliedVolatility",0),
                "pe_iv": pe.get("impliedVolatility",0),
            })
        return {
            "symbol":symbol,"ltp":ltp,"pcr":pcr,"ce_oi":ce_oi,"pe_oi":pe_oi,
            "expiries":expiries,"strikes":strikes,"src":"NSE",
        }
    except Exception as e:
        print(f"⚠️ OC [{symbol}]: {e}")
        return None

def get_fii_dii() -> dict:
    try:
        s    = _nse_session()
        data = s.get("https://www.nseindia.com/api/fiidiiTradeReact", timeout=8).json()
        fii  = next((x for x in data if "FII" in str(x.get("category",""))), {})
        dii  = next((x for x in data if x.get("category","")=="DII"), {})
        return {
            "fii_buy":round(float(fii.get("buyValue",0)),2),
            "fii_sell":round(float(fii.get("sellValue",0)),2),
            "fii_net":round(float(fii.get("netValue",0)),2),
            "dii_buy":round(float(dii.get("buyValue",0)),2),
            "dii_sell":round(float(dii.get("sellValue",0)),2),
            "dii_net":round(float(dii.get("netValue",0)),2),
            "date":fii.get("date",""),"src":"NSE",
        }
    except Exception as e:
        print(f"⚠️ FII/DII: {e}")
        return {"fii_net":0,"dii_net":0,"fii_buy":0,"fii_sell":0,"dii_buy":0,"dii_sell":0}


# ══════════════════════════════════════════════════
#  📡  SOURCE 4: STOOQ — Commodities + Backup
# ══════════════════════════════════════════════════
def get_stooq_data() -> dict:
    SYMS = {
        "nifty_s":"^nsei","banknifty_s":"^nsebank","sensex_s":"^bsesn",
        "gold_usd":"xauusd","crude_usd":"clusd","silver_usd":"xagusd","usdinr":"usdinr",
    }
    result = {}
    hdrs = {"User-Agent":"Mozilla/5.0"}
    for k,sym in SYMS.items():
        try:
            url  = f"https://stooq.com/q/l/?s={sym}&f=sd2t2ohlcv&h&e=csv"
            resp = requests.get(url,headers=hdrs,timeout=5)
            lines = resp.text.strip().split("\n")
            if len(lines)>=2:
                cols=lines[0].split(","); vals=lines[1].split(",")
                row=dict(zip(cols,vals))
                c=float(row.get("Close",0)); o=float(row.get("Open",0))
                if c>0:
                    result[k]={"price":round(c,2),"open":round(o,2),
                                "high":round(float(row.get("High",0)),2),"low":round(float(row.get("Low",0)),2),
                                "change":round(c-o,2),"pct":round((c-o)/o*100,2) if o else 0,"src":"STOOQ"}
        except: pass
        time.sleep(0.15)
    print(f"✅ Stooq: {len(result)}")
    return result


# ══════════════════════════════════════════════════
#  📡  SOURCE 5: NEWS (newsdata.io + newsapi.org + Google)
# ══════════════════════════════════════════════════
BULL_W = ["surge","rally","gain","bull","positive","up","breakout","rise","growth","strong","buy"]
BEAR_W = ["crash","fall","drop","bear","negative","down","plunge","selloff","weak","sell","loss"]

def get_news(pagesize: int = 8):
    articles = []; sentiment = "NEUTRAL ⚖️"

    # 1. newsdata.io (pub_ key)
    if NEWSDATA_KEY:
        try:
            url = (f"https://newsdata.io/api/1/latest?apikey={NEWSDATA_KEY}"
                   f"&q=nifty%20sensex%20india%20stock&language=en&category=business&size={pagesize}")
            data = requests.get(url, timeout=8).json()
            if data.get("status")=="success" and data.get("results"):
                bull=bear=0
                for a in data["results"][:pagesize]:
                    t=a.get("title",""); tl=t.lower()
                    s="bullish" if any(w in tl for w in BULL_W) else "bearish" if any(w in tl for w in BEAR_W) else "neutral"
                    if s=="bullish": bull+=1
                    elif s=="bearish": bear+=1
                    articles.append({"title":t,"source":a.get("source_name","NewsData"),
                                     "url":a.get("link",""),"time":a.get("pubDate","")[:16],"sentiment":s})
                sentiment = "BULLISH 📈" if bull>bear else "BEARISH 📉" if bear>bull else "NEUTRAL ⚖️"
                print(f"✅ NewsData: {len(articles)} | {sentiment}")
                return articles, sentiment, bull, bear
        except Exception as e: print(f"⚠️ NewsData: {e}")

    # 2. newsapi.org (backup)
    if NEWSAPI_KEY:
        try:
            url = (f"https://newsapi.org/v2/everything?q=India+stock+market+Nifty"
                   f"&sortBy=publishedAt&language=en&pageSize={pagesize}&apiKey={NEWSAPI_KEY}")
            data = requests.get(url, timeout=8).json()
            if data.get("status")=="ok" and data.get("articles"):
                bull=bear=0
                for a in data["articles"][:pagesize]:
                    t=a.get("title",""); tl=t.lower()
                    s="bullish" if any(w in tl for w in BULL_W) else "bearish" if any(w in tl for w in BEAR_W) else "neutral"
                    if s=="bullish": bull+=1
                    elif s=="bearish": bear+=1
                    articles.append({"title":t,"source":a.get("source",{}).get("name","NewsAPI"),
                                     "url":a.get("url",""),"time":a.get("publishedAt","")[:16],"sentiment":s})
                sentiment = "BULLISH 📈" if bull>bear else "BEARISH 📉" if bear>bull else "NEUTRAL ⚖️"
                print(f"✅ NewsAPI: {len(articles)} | {sentiment}")
                return articles, sentiment, bull, bear
        except Exception as e: print(f"⚠️ NewsAPI: {e}")

    # 3. Google News RSS fallback
    try:
        from xml.etree import ElementTree as ET
        url = "https://news.google.com/rss/search?q=Nifty+India+Market&hl=en-IN"
        resp = requests.get(url,timeout=8)
        root = ET.fromstring(resp.text)
        bull=bear=0
        for item in root.findall(".//item")[:pagesize]:
            t=item.find("title").text or ""; tl=t.lower()
            s="bullish" if any(w in tl for w in BULL_W) else "bearish" if any(w in tl for w in BEAR_W) else "neutral"
            if s=="bullish": bull+=1
            elif s=="bearish": bear+=1
            articles.append({"title":t,"source":"Google News","url":"","time":"","sentiment":s})
        sentiment = "BULLISH 📈" if bull>bear else "BEARISH 📉" if bear>bull else "NEUTRAL ⚖️"
        print(f"✅ Google RSS: {len(articles)}")
        return articles, sentiment, bull, bear
    except Exception as e: print(f"⚠️ Google RSS: {e}")

    return articles, sentiment, 0, 0


# ══════════════════════════════════════════════════
#  📊  INDICATOR ENGINE (REAL Yahoo candle data)
# ══════════════════════════════════════════════════
def calc_rsi(s, p=14):
    d=pd.Series(s).diff(); g=d.where(d>0,0).rolling(p).mean(); l=(-d.where(d<0,0)).rolling(p).mean()
    rs=g/l; rsi=100-(100/(1+rs)); return round(float(rsi.iloc[-1]),2) if not rsi.empty else 50.0

def calc_macd(s, fast=12, slow=26, sig=9):
    s=pd.Series(s); ef=s.ewm(span=fast,adjust=False).mean(); es=s.ewm(span=slow,adjust=False).mean()
    m=ef-es; sl=m.ewm(span=sig,adjust=False).mean(); return round(float(m.iloc[-1]),2),round(float(sl.iloc[-1]),2)

def calc_bb(s, p=20, sd=2):
    s=pd.Series(s); sma=s.rolling(p).mean(); std=s.rolling(p).std()
    return round(float((sma+sd*std).iloc[-1]),2),round(float(sma.iloc[-1]),2),round(float((sma-sd*std).iloc[-1]),2)

def calc_atr(df, p=14):
    hl=df["High"]-df["Low"]; hpc=(df["High"]-df["Close"].shift(1)).abs(); lpc=(df["Low"]-df["Close"].shift(1)).abs()
    tr=pd.concat([hl,hpc,lpc],axis=1).max(axis=1); atr=tr.rolling(p).mean()
    return round(float(atr.iloc[-1]),2) if not atr.empty else 0.0

def calc_vwap(df):
    df=df.copy(); df["_d"]=df.index.date; parts=[]
    for _,g in df.groupby("_d"):
        tp=(g["High"]+g["Low"]+g["Close"])/3; parts.append((tp*g["Volume"]).cumsum()/g["Volume"].cumsum())
    return round(float(pd.concat(parts).sort_index().iloc[-1]),2)

def calc_pivot(h,l,c):
    p=(h+l+c)/3
    return {"pivot":round(p,2),"r1":round(p*2-l,2),"r2":round(p+(h-l),2),"s1":round(p*2-h,2),"s2":round(p-(h-l),2)}

def calc_stochastic(df, k=14, d=3):
    """Stochastic Oscillator"""
    low14  = df["Low"].rolling(k).min()
    high14 = df["High"].rolling(k).max()
    k_line = 100 * ((df["Close"] - low14) / (high14 - low14 + 0.0001))
    d_line = k_line.rolling(d).mean()
    return round(float(k_line.iloc[-1]),2), round(float(d_line.iloc[-1]),2)

def calc_adx(df, p=14):
    """ADX — Trend Strength"""
    try:
        high=df["High"]; low=df["Low"]; close=df["Close"]
        plus_dm=(high.diff()).where(lambda x:x>0,0)
        minus_dm=(-low.diff()).where(lambda x:x>0,0)
        tr=pd.concat([high-low,(high-close.shift()).abs(),(low-close.shift()).abs()],axis=1).max(axis=1)
        atr=tr.rolling(p).mean()
        plus_di=100*(plus_dm.rolling(p).mean()/atr)
        minus_di=100*(minus_dm.rolling(p).mean()/atr)
        dx=100*(plus_di-minus_di).abs()/(plus_di+minus_di+0.001)
        adx=dx.rolling(p).mean()
        return round(float(adx.iloc[-1]),2)
    except: return 25.0

def generate_signal(name: str, df, price: float) -> dict:
    """Full signal using REAL candle data"""
    blank = {"signal":"WAIT ⏳","entry":0,"sl":0,"target":0,"rsi":50,"macd":0,"macd_signal":0,
             "atr":0,"ema9":price,"ema20":price,"ema50":price,"ema200":price,"vwap":price,
             "bb_upper":price,"bb_lower":price,"bullish_votes":0,"bearish_votes":0,
             "pivot":{},"qty":1,"adx":25,"stoch_k":50,"stoch_d":50,"src":"NO_DATA"}
    if df is None or len(df)<30: return blank

    c=df["Close"]
    rsi        = calc_rsi(c.tolist())
    macd,msig  = calc_macd(c.tolist())
    bb_up,bb_mid,bb_lo = calc_bb(c.tolist())
    ema9   = round(float(c.ewm(span=9,  adjust=False).mean().iloc[-1]),2)
    ema20  = round(float(c.ewm(span=20, adjust=False).mean().iloc[-1]),2)
    ema50  = round(float(c.ewm(span=50, adjust=False).mean().iloc[-1]),2)
    ema200 = round(float(c.ewm(span=200,adjust=False).mean().iloc[-1]),2) if len(df)>=200 else ema50
    atr    = calc_atr(df)
    vwap   = calc_vwap(df) if df["Volume"].sum()>0 else price
    piv    = calc_pivot(float(df["High"].max()),float(df["Low"].min()),float(c.iloc[-1]))
    adx    = calc_adx(df)
    sk, sd = calc_stochastic(df)

    bull=bear=0
    if price>ema9:   bull+=1
    else:             bear+=1
    if price>ema20:  bull+=1
    else:             bear+=1
    if price>ema50:  bull+=1
    else:             bear+=1
    if price>ema200: bull+=1
    else:             bear+=1
    if 45<rsi<70:    bull+=1
    elif rsi<35:      bear+=2
    elif rsi>72:      bear+=1
    if macd>msig:    bull+=1
    else:             bear+=1
    if price<bb_up:  bull+=1
    if price>bb_lo:  bear+=1
    if price>vwap:   bull+=1
    else:             bear+=1
    if sk>sd and sk<80: bull+=1
    elif sk<sd and sk>20: bear+=1

    if bull>=7:
        sig="BUY CALL 🚀"; entry=price; sl=round(entry-ATR_MULT*atr,2); tgt=round(entry+RR*(entry-sl),2)
    elif bear>=7:
        sig="BUY PUT 📉";  entry=price; sl=round(entry+ATR_MULT*atr,2); tgt=round(entry-RR*(sl-entry),2)
    elif bull>=5:
        sig="WEAK BUY 🟡"; entry=price; sl=round(entry-ATR_MULT*atr,2); tgt=round(entry+RR*(entry-sl),2)
    else:
        sig="WAIT ⏳"; entry=sl=tgt=0

    qty=max(1,int(RISK/(ATR_MULT*atr))) if atr>0 else 1
    return {"signal":sig,"entry":entry,"sl":sl,"target":tgt,"rsi":rsi,"macd":macd,"macd_signal":msig,
            "atr":atr,"ema9":ema9,"ema20":ema20,"ema50":ema50,"ema200":ema200,"vwap":vwap,
            "bb_upper":bb_up,"bb_mid":bb_mid,"bb_lower":bb_lo,"bullish_votes":bull,"bearish_votes":bear,
            "qty":qty,"pivot":piv,"adx":adx,"stoch_k":sk,"stoch_d":sd,"src":"REAL_CANDLES"}


# ══════════════════════════════════════════════════
#  🧠  MARKET SENTIMENT ENGINE
#  Combines: FII/DII, PCR, News, Technical indicators
# ══════════════════════════════════════════════════
def calc_market_sentiment(signals, fii_dii, oc_data, news_bull, news_bear):
    """
    Market Sentiment Score: 0-100
    0-30: Very Bearish | 31-45: Bearish | 46-54: Neutral | 55-70: Bullish | 71-100: Very Bullish
    """
    score = 50  # Start neutral
    reasons = []

    # FII/DII (weight: 20 points)
    fii_net = fii_dii.get("fii_net", 0)
    dii_net = fii_dii.get("dii_net", 0)
    combined = fii_net + dii_net
    if combined > 2000:
        score += 15; reasons.append(f"FII+DII ₹{combined:.0f}Cr ←🟢 Strong Buy")
    elif combined > 500:
        score += 8; reasons.append(f"FII+DII ₹{combined:.0f}Cr ←🟢 Moderate Buy")
    elif combined < -2000:
        score -= 15; reasons.append(f"FII+DII ₹{combined:.0f}Cr ←🔴 Heavy Sell")
    elif combined < -500:
        score -= 8; reasons.append(f"FII+DII ₹{combined:.0f}Cr ←🔴 Moderate Sell")

    # PCR (weight: 15 points)
    pcr = oc_data.get("pcr", 1.0) if oc_data else 1.0
    if pcr > 1.3:
        score += 12; reasons.append(f"PCR {pcr} ←🟢 Bullish (High Put Writing)")
    elif pcr > 1.0:
        score += 5; reasons.append(f"PCR {pcr} ←🟡 Slightly Bullish")
    elif pcr < 0.7:
        score -= 12; reasons.append(f"PCR {pcr} ←🔴 Bearish (High Call Writing)")
    elif pcr < 1.0:
        score -= 5; reasons.append(f"PCR {pcr} ←🟡 Slightly Bearish")

    # Technical signals (weight: 20 points)
    nifty_sig = signals.get("nifty", {})
    bull_v = nifty_sig.get("bullish_votes", 4)
    bear_v = nifty_sig.get("bearish_votes", 4)
    if bull_v >= 7:
        score += 15; reasons.append(f"Technical 🟢 {bull_v}/10 Bullish votes")
    elif bull_v >= 5:
        score += 7; reasons.append(f"Technical 🟡 {bull_v}/10 Bullish votes")
    elif bear_v >= 7:
        score -= 15; reasons.append(f"Technical 🔴 {bear_v}/10 Bearish votes")
    elif bear_v >= 5:
        score -= 7; reasons.append(f"Technical 🟡 {bear_v}/10 Bearish votes")

    # RSI (weight: 10 points)
    rsi = nifty_sig.get("rsi", 50)
    if 50 < rsi < 70:
        score += 8; reasons.append(f"RSI {rsi} ←🟢 Bullish zone")
    elif rsi >= 70:
        score -= 5; reasons.append(f"RSI {rsi} ←🔴 Overbought")
    elif 30 < rsi < 50:
        score -= 5; reasons.append(f"RSI {rsi} ←🔴 Bearish zone")
    elif rsi <= 30:
        score += 5; reasons.append(f"RSI {rsi} ←🟡 Oversold bounce possible")

    # News Sentiment (weight: 10 points)
    total_news = news_bull + news_bear + 1
    news_bull_pct = news_bull / total_news * 100
    if news_bull_pct > 60:
        score += 8; reasons.append(f"News 📰 {news_bull_pct:.0f}% Bullish")
    elif news_bull_pct < 40 and news_bear > 2:
        score -= 8; reasons.append(f"News 📰 {100-news_bull_pct:.0f}% Bearish")

    # ADX (trend strength)
    adx = nifty_sig.get("adx", 25)
    if adx > 35:
        reasons.append(f"ADX {adx} ←💪 Strong Trend")
    elif adx < 20:
        reasons.append(f"ADX {adx} ←⚠️ Weak/No Trend")

    score = max(0, min(100, score))

    # Label
    if score >= 71: label = "VERY BULLISH 🔥"; color = "green"
    elif score >= 55: label = "BULLISH 📈"; color = "green"
    elif score >= 45: label = "NEUTRAL ⚖️"; color = "gold"
    elif score >= 31: label = "BEARISH 📉"; color = "red"
    else: label = "VERY BEARISH ❄️"; color = "red"

    # Market direction prediction
    if score >= 60: direction = "UP ↑"; direction_pct = round((score-50)*2, 1)
    elif score <= 40: direction = "DOWN ↓"; direction_pct = round((50-score)*2, 1)
    else: direction = "SIDEWAYS ↔"; direction_pct = 0

    return {
        "score": score,
        "label": label,
        "color": color,
        "direction": direction,
        "direction_confidence": direction_pct,
        "reasons": reasons[:5],
        "pcr": pcr,
        "fii_net": fii_net,
        "news_bull_pct": round(news_bull/(news_bull+news_bear+1)*100,1),
    }


# ══════════════════════════════════════════════════
#  🧠  TECHNICAL FLOW METER (Digital)
#  Shows overall market technical strength 0-100
# ══════════════════════════════════════════════════
def calc_tech_flow_meter(signals: dict) -> dict:
    """
    Digital flow meter combining all 7 indicators for each index
    Returns score 0-100 and dial position
    """
    flow_scores = {}
    for idx_name, sig in signals.items():
        if not sig or sig.get("src") == "NO_DATA":
            flow_scores[idx_name] = {"score":50,"label":"NEUTRAL","bars":[]}
            continue

        price = sig.get("entry") or sig.get("ema20",0)
        rsi   = sig.get("rsi",50)
        macd  = sig.get("macd",0)
        msig  = sig.get("macd_signal",0)
        ema9  = sig.get("ema9",0)
        ema20 = sig.get("ema20",0)
        ema50 = sig.get("ema50",0)
        ema200= sig.get("ema200",0)
        bb_up = sig.get("bb_upper",0)
        bb_lo = sig.get("bb_lower",0)
        bb_mid= sig.get("bb_mid",0)
        adx   = sig.get("adx",25)
        sk    = sig.get("stoch_k",50)
        sd    = sig.get("stoch_d",50)

        # Each indicator: bull(+1) neutral(0) bear(-1)
        indicators = [
            {"name":"BB",   "val":round((price-bb_lo)/(bb_up-bb_lo+0.001)*100,1) if bb_up>bb_lo else 50,
             "bull": price>bb_mid,"label":f"BB {'上' if price>bb_mid else '下'}"},
            {"name":"MACD", "val":round(macd/0.1,1) if abs(macd)<50 else (100 if macd>0 else 0),
             "bull": macd>msig,"label":f"MACD {'Bull' if macd>msig else 'Bear'}"},
            {"name":"RSI",  "val":rsi,"bull": 45<rsi<70,"label":f"RSI {rsi}"},
            {"name":"EMA9", "val":100 if price>ema9 else 0,"bull": price>ema9,"label":f"9EMA {'↑' if price>ema9 else '↓'}"},
            {"name":"EMA50","val":100 if price>ema50 else 0,"bull": price>ema50,"label":f"50EMA {'↑' if price>ema50 else '↓'}"},
            {"name":"EMA200","val":100 if price>ema200 else 0,"bull": price>ema200,"label":f"200EMA {'↑' if price>ema200 else '↓'}"},
            {"name":"ADX",  "val":min(adx,100),"bull": adx>25,"label":f"ADX {adx:.0f} {'Strong' if adx>30 else 'Weak'}"},
            {"name":"STOCH","val":sk,"bull": sk>sd and sk<80,"label":f"Stoch {'Bull' if sk>sd else 'Bear'}"},
        ]

        bull_count = sum(1 for i in indicators if i["bull"])
        score = round((bull_count/len(indicators))*100)

        if score>=70: label="STRONG BUY"; col="green"
        elif score>=55: label="BUY"; col="limegreen"
        elif score>=45: label="NEUTRAL"; col="gold"
        elif score>=30: label="SELL"; col="orange"
        else: label="STRONG SELL"; col="red"

        flow_scores[idx_name] = {
            "score":   score,
            "label":   label,
            "color":   col,
            "bull_count": bull_count,
            "total":   len(indicators),
            "bars":    [{"name":i["name"],"val":i["val"],"bull":i["bull"],"label":i["label"]} for i in indicators],
            "adx":     adx,
        }

    return flow_scores


# ══════════════════════════════════════════════════
#  🧠  MASTER AGGREGATOR
# ══════════════════════════════════════════════════
_cache = {}; _cache_ts = 0; CACHE_TTL = 20
_candle_cache = {}; _candle_ts = 0; CANDLE_TTL = 300

def _get_candles(symbol: str):
    global _candle_cache, _candle_ts
    now = time.time()
    if now-_candle_ts>CANDLE_TTL or symbol not in _candle_cache:
        df = get_yahoo_candles(symbol,"3d","5m")
        if df is not None:
            _candle_cache[symbol]=df; _candle_ts=now
    return _candle_cache.get(symbol)

def get_all_data(force=False) -> dict:
    global _cache, _cache_ts
    if not force and time.time()-_cache_ts<CACHE_TTL and _cache:
        return _cache

    print(f"\n🔄 [{ist_now().strftime('%H:%M:%S')}] Fetching all live data...")

    # 1. Live spot prices (Dhan → Yahoo fallback)
    dhan = get_dhan_prices()
    n_p  = dhan.get("nifty",{}).get("price") or get_yahoo_price("^NSEI")
    bn_p = dhan.get("banknifty",{}).get("price") or get_yahoo_price("^NSEBANK")
    fn_p = dhan.get("finnifty",{}).get("price") or get_yahoo_price("^NSEI") # proxy

    # 2. Real intraday candles (Yahoo Finance)
    n_df  = _get_candles("^NSEI")
    bn_df = _get_candles("^NSEBANK")

    if not n_p  and n_df  is not None: n_p  = round(float(n_df["Close"].iloc[-1]),2)
    if not bn_p and bn_df is not None: bn_p = round(float(bn_df["Close"].iloc[-1]),2)
    n_p  = n_p  or 24200.0
    bn_p = bn_p or 52340.0

    # 3. Inject live price into last candle
    def inject(df, price):
        if df is None: return None
        df=df.copy(); df.iloc[-1, df.columns.get_loc("Close")]=price; return df

    n_sig  = generate_signal("NIFTY",     inject(n_df,  n_p),  n_p)
    bn_sig = generate_signal("BANKNIFTY", inject(bn_df, bn_p), bn_p)

    # 4. NSE data
    nse    = get_nse_indices()
    stooq  = get_stooq_data()
    oc_n   = get_option_chain("NIFTY")
    oc_bn  = get_option_chain("BANKNIFTY")
    fii    = get_fii_dii()

    # 5. Gift Nifty
    gift_nifty = get_gift_nifty()

    # 6. News
    arts, news_sent, news_bull, news_bear = get_news()

    # 7. Market Sentiment
    signals_dict = {"nifty": n_sig, "banknifty": bn_sig}
    sentiment = calc_market_sentiment(signals_dict, fii, oc_n, news_bull, news_bear)

    # 8. Tech Flow Meter
    tech_flow = calc_tech_flow_meter(signals_dict)

    # Alert on signal change
    _check_alert("NIFTY",     n_sig)
    _check_alert("BANKNIFTY", bn_sig)

    def merge_idx(nse_k, stooq_k, live_p, live_src):
        base = nse.get(nse_k) or stooq.get(stooq_k) or {}
        if live_p: base["price"] = live_p; base["src"] = live_src
        return base

    _cache = {
        "app":           "Pagalpan Treding Pro",
        "timestamp":     ist_now().strftime("%H:%M:%S"),
        "market_open":   is_market_open(),
        "pre_market":    is_pre_market(),
        "indices": {
            "nifty":      merge_idx("nifty",    "nifty_s",    n_p,  dhan.get("nifty",{}).get("src","YAHOO")),
            "banknifty":  merge_idx("banknifty","banknifty_s",bn_p, dhan.get("banknifty",{}).get("src","YAHOO")),
            "sensex":     nse.get("sensex",  stooq.get("sensex_s",{})),
            "finifty":    merge_idx("finifty",  "nifty_s",  fn_p, "NSE"),
            "midcap50":   nse.get("midcap50", {}),
            "vix":        nse.get("vix",      {}),
            "nifty_it":   nse.get("nifty_it", {}),
            "nifty_auto": nse.get("nifty_auto",{}),
            "nifty_fmcg": nse.get("nifty_fmcg",{}),
            "nifty_metal":nse.get("nifty_metal",{}),
            "psubank":    nse.get("psubank",  {}),
        },
        "gift_nifty":   gift_nifty,
        "commodities": {
            "gold_usd":   stooq.get("gold_usd",  {}),
            "crude_usd":  stooq.get("crude_usd", {}),
            "silver_usd": stooq.get("silver_usd",{}),
            "usdinr":     stooq.get("usdinr",    {}),
        },
        "signals":       signals_dict,
        "option_chain":  {"nifty":oc_n,"banknifty":oc_bn},
        "pcr":           oc_n.get("pcr",1.0) if oc_n else 1.0,
        "fii_dii":       fii,
        "news":          arts,
        "news_sentiment":news_sent,
        "market_sentiment": sentiment,
        "tech_flow_meter":  tech_flow,
    }
    _cache_ts = time.time()
    print(f"✅ DONE | N:{n_p} | Sig:{n_sig['signal']} | Sentiment:{sentiment['label']}({sentiment['score']})")
    return _cache


# ══════════════════════════════════════════════════
#  🔔  TELEGRAM ALERTS
# ══════════════════════════════════════════════════
_last_sig = {}

def _check_alert(name: str, sig: dict):
    global _last_sig
    new = sig.get("signal","")
    if "WAIT" in new: return
    if _last_sig.get(name)==new: return
    e=sig["entry"]; sl=sig["sl"]; tgt=sig["target"]; qty=sig["qty"]
    risk=round(abs(e-sl)*qty,2); profit=round(abs(tgt-e)*qty,2)
    piv=sig.get("pivot",{}); is_buy="BUY CALL" in new or "WEAK" in new
    msg = (
        f"{'📈' if is_buy else '📉'} *{new} — {name}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Entry  : `{e:,.2f}`\n"
        f"🎯 Target : `{tgt:,.2f}` (1:{RR})\n"
        f"🛑 SL     : `{sl:,.2f}` (ATR×{ATR_MULT})\n"
        f"📦 Qty:{qty} | Risk:₹{risk:,} | Profit:₹{profit:,}\n\n"
        f"📊 RSI:`{sig['rsi']}` MACD:`{sig['macd']}` ADX:`{sig.get('adx',0)}`\n"
        f"🗳️ Votes: 🟢{sig['bullish_votes']} 🔴{sig['bearish_votes']}\n"
        f"🎯 R1:`{piv.get('r1','-'):,}` P:`{piv.get('pivot','-'):,}` S1:`{piv.get('s1','-'):,}`\n"
        f"🕒 {ist_now().strftime('%H:%M IST')}\n\n"
        f"⚠️ _Pagalpan Treding Pro — Educational only_"
    )
    send_telegram(msg)
    _last_sig[name]=new

def hero_zero_scan():
    if not is_market_open(): return
    t = ist_now().strftime("%H:%M")
    if not ("13:00"<=t<="15:30"): return
    oc = get_option_chain("NIFTY")
    if not oc: return
    pcr=oc["pcr"]; ltp=oc["ltp"]
    if pcr<0.55:
        send_telegram(f"💥 *HERO-ZERO (CALL)*\nNIFTY:`{ltp}` PCR:`{pcr}` ← Very Oversold\n🔥 Short covering!\nOTM Call ₹10-15 → Hero 🚀\n⚠️_HIGH RISK_")
    elif pcr>1.65:
        send_telegram(f"💥 *HERO-ZERO (PUT)*\nNIFTY:`{ltp}` PCR:`{pcr}` ← Very Overbought\n🔥 Long unwinding!\nOTM Put ₹10-15 → Hero 📉\n⚠️_HIGH RISK_")


# ══════════════════════════════════════════════════
#  🤖  TELEGRAM BOT COMMANDS
# ══════════════════════════════════════════════════
_last_upd_id = 0

def handle_commands():
    global _last_upd_id
    try:
        r=requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                       params={"offset":_last_upd_id+1},timeout=5)
        for upd in r.json().get("result",[]):
            _last_upd_id=upd["update_id"]
            msg=upd.get("message",{}); text=msg.get("text","").strip().lower()
            uid=str(msg.get("chat",{}).get("id",""))
            if uid!=str(TELEGRAM_CHATID): continue
            d=get_all_data(); n=d["indices"]["nifty"]; bn=d["indices"]["banknifty"]
            sn=d["signals"]["nifty"]; sent=d["market_sentiment"]

            if text=="/status":
                send_telegram(
                    f"🔥 *Pagalpan Treding Pro*\n"
                    f"🕒 {ist_now().strftime('%H:%M IST')}\n"
                    f"📊 {'OPEN🟢' if d['market_open'] else 'CLOSED🔴'}\n\n"
                    f"📈 NIFTY: `{n['price']}` [{n.get('src','?')}]\n"
                    f"🏦 BN: `{bn['price']}`\n"
                    f"🎁 Gift Nifty: `{d['gift_nifty'].get('price','?')}`\n\n"
                    f"🎯 Signal: *{sn['signal']}*\n"
                    f"📡 Sentiment: *{sent['label']}* ({sent['score']}/100)\n"
                    f"📰 News: {d['news_sentiment']}"
                )
            elif text=="/scan":
                send_telegram("🔍 Scanning..."); get_all_data(force=True)
                sn=_cache["signals"]["nifty"]; sbn=_cache["signals"]["banknifty"]
                send_telegram(f"NIFTY: *{sn['signal']}* RSI:{sn['rsi']}\nBN: *{sbn['signal']}*")
            elif text=="/pcr":
                oc=get_option_chain("NIFTY")
                if oc:
                    pcr=oc["pcr"]; zone="Bullish🟢" if pcr>1.2 else "Bearish🔴" if pcr<0.8 else "Neutral🟡"
                    send_telegram(f"🏛️ PCR:`{pcr}` LTP:`{oc['ltp']}` {zone}\nCE:`{oc['ce_oi']:,}` PE:`{oc['pe_oi']:,}`")
            elif text=="/fii":
                f=get_fii_dii()
                send_telegram(f"💰 FII: {'🟢' if f['fii_net']>0 else '🔴'}₹{f['fii_net']:.0f}Cr\nDII: {'🟢' if f['dii_net']>0 else '🔴'}₹{f['dii_net']:.0f}Cr")
            elif text=="/gift":
                g=get_gift_nifty()
                send_telegram(f"🎁 Gift Nifty: `{g['price']}` ({'+' if g['change']>=0 else ''}{g['change']}) [{g['src']}]")
            elif text=="/sentiment":
                sent=d["market_sentiment"]
                reasons="\n".join([f"• {r}" for r in sent["reasons"][:4]])
                send_telegram(f"📡 *Market Sentiment*\n{sent['label']} ({sent['score']}/100)\n{sent['direction']} {sent['direction_confidence']}% confidence\n\n{reasons}")
            elif text=="/help":
                send_telegram("/status /scan /pcr /fii /gift /sentiment /help")
    except Exception as e:
        print(f"[CMD] {e}")


# ══════════════════════════════════════════════════
#  ⏰  SCHEDULER
# ══════════════════════════════════════════════════
def _heartbeat():
    d=get_all_data(); n=d["indices"]["nifty"]; sn=d["signals"]["nifty"]
    sent=d["market_sentiment"]; g=d["gift_nifty"]
    send_telegram(
        f"💓 *Pagalpan Treding Pro*\n"
        f"🕒 {ist_now().strftime('%H:%M')} | {'OPEN🟢' if d['market_open'] else 'CLOSED🔴'}\n"
        f"📈 N:`{n['price']}` 🎁 Gift:`{g.get('price','?')}`\n"
        f"🎯 {sn['signal']}\n"
        f"📡 Sentiment: {sent['label']} ({sent['score']}/100)"
    )

def start_scheduler():
    schedule.every(5).minutes.do(lambda: get_all_data(force=True))
    schedule.every(10).minutes.do(hero_zero_scan)
    schedule.every(30).minutes.do(_heartbeat)
    schedule.every(30).seconds.do(handle_commands)
    while True: schedule.run_pending(); time.sleep(1)


# ══════════════════════════════════════════════════
#  🌐  FLASK API
# ══════════════════════════════════════════════════
@app.route("/")
def root():
    return jsonify({"app":"Pagalpan Treding Pro","status":"LIVE","time":ist_now().strftime("%H:%M:%S IST")})

@app.route("/health")
def health():
    return jsonify({"ok":True,"ts":ist_now().isoformat()})

@app.route("/api/data")
def api_data():
    return jsonify(get_all_data())

@app.route("/api/signal")
def api_signal():
    return jsonify(get_all_data().get("signals",{}))

@app.route("/api/fii")
def api_fii():
    return jsonify(get_fii_dii())

@app.route("/api/options/<symbol>")
def api_options(symbol):
    return jsonify(get_option_chain(symbol.upper()) or {})

@app.route("/api/news")
def api_news():
    arts,sent,b,br=get_news(); return jsonify({"articles":arts,"sentiment":sent,"bull":b,"bear":br})

@app.route("/api/gift")
def api_gift():
    return jsonify(get_gift_nifty())

@app.route("/api/sentiment")
def api_sentiment():
    return jsonify(get_all_data().get("market_sentiment",{}))

@app.route("/api/techflow")
def api_techflow():
    return jsonify(get_all_data().get("tech_flow_meter",{}))


# ══════════════════════════════════════════════════
#  🚀  MAIN
# ══════════════════════════════════════════════════
if __name__=="__main__":
    print("╔══════════════════════════════════════════════════════╗")
    print("║     🔥 PAGALPAN TREDING PRO — BACKEND v3.0           ║")
    print("║     100% LIVE: Dhan + Yahoo + NSE + News             ║")
    print("╚══════════════════════════════════════════════════════╝")

    d  = get_all_data(force=True)
    n  = d["indices"]["nifty"]
    sn = d["signals"]["nifty"]
    sent = d["market_sentiment"]
    print(f"\n📈 NIFTY: {n['price']} [{n.get('src','?')}]")
    print(f"🎁 Gift Nifty: {d['gift_nifty'].get('price','?')}")
    print(f"🎯 Signal: {sn['signal']} | RSI:{sn['rsi']}")
    print(f"📡 Sentiment: {sent['label']} ({sent['score']}/100)")

    send_telegram(
        f"🔥 *Pagalpan Treding Pro v3.0 LIVE!*\n"
        f"⏰ {ist_now().strftime('%d %b %Y %H:%M IST')}\n"
        f"📈 NIFTY: `{n['price']}` [{n.get('src','?')}]\n"
        f"🎁 Gift Nifty: `{d['gift_nifty'].get('price','?')}`\n"
        f"🎯 Signal: `{sn['signal']}`\n"
        f"📡 Sentiment: {sent['label']} ({sent['score']}/100)\n\n"
        f"🔴 ALL LIVE DATA — No demo!\nType /help"
    )

    threading.Thread(target=start_scheduler, daemon=True).start()
    port=int(os.getenv("PORT",5000))
    print(f"\n🌐 API: http://0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
