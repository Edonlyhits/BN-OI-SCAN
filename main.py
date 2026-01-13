import time
import requests
import logging
import asyncio
import aiohttp
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timezone, timedelta
import pandas as pd
import os
import re

# --- 設定 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = TimedRotatingFileHandler("binance_oi_log.log", when="midnight", interval=1, backupCount=30)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.addHandler(logging.StreamHandler())

BASE_URL = "https://fapi.binance.com"
# 請確保環境變數中有 DISCORD_WEBHOOK_URL，或者直接填入
DISCORD_WEBHOOK_URL = os.getenv("https://discord.com/api/webhooks/1460100896942854265/y8P743R0sZpgR06D3WcLvfcnsAroCzyWCzEvwnmu4JBzk5k_6C_O9089oOMQdotHumVo") 
DATA_FILE = "binance_oi_data_history.csv"
TAIWAN_TZ = timezone(timedelta(hours=8))
CHECK_INTERVAL = 45 
OI_THRESHOLD = 5  
PRICE_THRESHOLD = 1.2  

# --- 核心函數 ---

def get_all_symbols():
    url = f"{BASE_URL}/fapi/v1/exchangeInfo"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return [
                s['symbol'] for s in response.json()['symbols']
                if s['status'] == 'TRADING'
                and not s['symbol'].upper().endswith('USDC')
                and s['contractType'] == 'PERPETUAL'
                and not re.search(r'\d{6}', s['symbol'])
            ]
    except Exception as e:
        logger.error(f"獲取交易對失敗: {e}")
    return []

def get_all_prices():
    url = f"{BASE_URL}/fapi/v1/ticker/price"
    try:
        return {item['symbol']: float(item['price']) for item in requests.get(url).json()}
    except: return {}

def get_funding_rates():
    url = f"{BASE_URL}/fapi/v1/premiumIndex"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return {item['symbol']: float(item['lastFundingRate']) for item in response.json() if 'lastFundingRate' in item}
    except: return {}

async def fetch_open_interest(session, symbol, semaphore):
    async with semaphore:
        url = f"{BASE_URL}/fapi/v1/openInterest?symbol={symbol}"
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    return symbol, float(data['openInterest'])
        except: pass
        return symbol, None

async def get_all_open_interest(symbols):
    semaphore = asyncio.Semaphore(15)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_open_interest(session, symbol, semaphore) for symbol in symbols]
        results = await asyncio.gather(*tasks)
        return dict(results)

def save_to_csv(data, timestamp):
    df = pd.DataFrame(data)
    df['timestamp'] = timestamp
    if not os.path.exists(DATA_FILE):
        df.to_csv(DATA_FILE, index=False)
    else:
        df.to_csv(DATA_FILE, mode='a', header=False, index=False)

# --- 主程式 ---

async def main():
    logger.info("監控程式啟動...")
    while True:
        try:
            symbols = get_all_symbols()
            prev_prices = get_all_prices()
            prev_oi = await get_all_open_interest(symbols)
            funding_rates = get_funding_rates()
            
            start_time = datetime.now(TAIWAN_TZ).strftime('%Y/%m/%d %H:%M:%S')
            
            # 儲存起始點數據
            data_start = [
                {'symbol': s, 'price': prev_prices[s], 'oi': prev_oi[s], 'funding': funding_rates.get(s, 0), 'phase': 'start'}
                for s in symbols if s in prev_prices and prev_oi.get(s)
            ]
            save_to_csv(data_start, start_time)
            
            await asyncio.sleep(CHECK_INTERVAL)
            
            curr_prices = get_all_prices()
            curr_oi = await get_all_open_interest(symbols)
            
            embeds = []
            for s in symbols:
                if s not in prev_prices or s not in curr_prices or not prev_oi.get(s) or not curr_oi.get(s):
                    continue
                
                oi_chg = (curr_oi[s] - prev_oi[s]) / prev_oi[s] * 100
                prc_chg = (curr_prices[s] - prev_prices[s]) / prev_prices[s] * 100
                funding = funding_rates.get(s, 0)
                
                if oi_chg > OI_THRESHOLD and abs(prc_chg) < PRICE_THRESHOLD:
                    # 方向判定
                    direction = "📈 資金建多" if prc_chg > 0.3 else "📉 資金建空" if prc_chg < -0.3 else "🔄 橫盤吸籌"
                    color = 0x00FF00 if prc_chg > 0.3 else 0xFF0000 if prc_chg < -0.3 else 0xFFFF00
                    
                    embeds.append({
                        "title": f"⚠️ {s} 持倉異常",
                        "color": color,
                        "fields": [
                            {"name": "參考方向", "value": f"**{direction}**", "inline": False},
                            {"name": "OI 變化", "value": f"+{oi_chg:.2f}%", "inline": True},
                            {"name": "價格變化", "value": f"{prc_chg:.2f}%", "inline": True},
                            {"name": "資金費率", "value": f"{funding*100:.4f}%", "inline": True}
                        ]
                    })
            
            if embeds:
                async with aiohttp.ClientSession() as session:
                    await session.post(DISCORD_WEBHOOK_URL, json={"embeds": embeds[:10]})
            
            logger.info(f"檢查完成，發現 {len(embeds)} 個異常")
            
        except Exception as e:
            logger.error(f"運行錯誤: {e}")
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())


