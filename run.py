import requests
import json
import traceback
import sqlite3
from datetime import datetime, timedelta, timezone
import warnings  # 추가된 부분

# InsecureRequestWarning 경고 무시
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

with open("/home/arang/web/lostark/lostark-auction-noti/config.json", "rb") as f:
    config = json.loads(f.read())

token = config["token"]
webhook_url = config["webhook_url"]

# KST 시간대 정의
KST = timezone(timedelta(hours=9))

def parse_endDate(endDate_str):
    try:
        dt = datetime.strptime(endDate_str, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        try:
            dt = datetime.strptime(endDate_str, '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            return None
    # 서버에서 받은 시간에 UTC 시간대를 설정하고 KST로 변환
    dt = dt.replace().astimezone(KST)
    return dt

def send_discord_message(condition_name, previous_lowest_price, current_lowest_price, item_details):
    global webhook_url
    # 남은 시간 계산
    endDate_datetime = parse_endDate(item_details['endDate'])
    current_time = datetime.now(KST)
    if endDate_datetime:
        time_diff = endDate_datetime - current_time
        total_seconds = int(time_diff.total_seconds())
        if total_seconds > 0:
            hours, remainder = divmod(total_seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            time_remaining = f"{hours}시간 {minutes}분 남음"
        else:
            time_remaining = "경매 종료"
    else:
        time_remaining = "종료 시간 파싱 불가"

    # Discord 임베드 메시지 구성
    embed = {
        "title": f"[{condition_name}] 최저가 갱신",
        "description": f"{previous_lowest_price} → {current_lowest_price}",
        "color": 0x00ff00,  # Green color
        "fields": [
            {
                "name": "아이템 이름",
                "value": item_details['itemName'],
                "inline": False
            },
            {
                "name": "옵션 정보",
                "value": item_details['optionInfo'],
                "inline": False
            },
            {
                "name": "거래 가능 횟수",
                "value": str(item_details['tradeAllowCount']),
                "inline": True
            },
            {
                "name": "품질",
                "value": str(item_details['gradeQuality']),
                "inline": True
            },
            {
                "name": "남은 시간",
                "value": time_remaining,
                "inline": False
            }
        ],
        "thumbnail": {
            "url": item_details['icon']
        },
        "timestamp": datetime.now(KST).isoformat()
    }
    data = {
        "embeds": [embed]
    }
    headers = {
        "Content-Type": "application/json"
    }
    response = requests.post(webhook_url, data=json.dumps(data), headers=headers, verify=False)
    if response.status_code == 204:
        print(f"[{current_time}] {condition_name} - 메시지가 성공적으로 전송되었습니다.")
    else:
        print(f"메시지 전송 실패. 상태 코드: {response.status_code} - {response.text}")

def log(message):
    print(message)
    # send_discord_message(message)  # 현재는 사용하지 않음

u = "https://developer-lostark.game.onstove.com/auctions/items"
s = requests.session()
s.verify = False
s.headers = {
    "Content-Type": "application/json",
    "Authorization": f"bearer {token}"
}
# s.proxies = {"https": "http://localhost:8888"}  # 필요 시 사용

with open("/home/arang/web/lostark/lostark-auction-noti/conditions.json", "rb") as f:
    try:
        conditions = json.loads(f.read())
    except:
        print(f"[x] condition load error..")
        log(traceback.format_exc())
        exit(1)

print(f"[+] condition load success")

# 데이터베이스 설정
conn = sqlite3.connect('items.db')
cursor = conn.cursor()

# 테이블 생성
cursor.execute('''
CREATE TABLE IF NOT EXISTS items (
    condition_name TEXT,
    itemName TEXT,
    optionInfo TEXT,
    endDate TEXT,
    price REAL,
    tradeAllowCount INTEGER,
    gradeQuality INTEGER,
    icon TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS lowest_prices (
    condition_name TEXT PRIMARY KEY,
    lowest_price REAL
)
''')

# items 테이블 초기화
cursor.execute('DELETE FROM items')
conn.commit()

for condition in conditions:
    cnt = 0
    conditions[condition]["PageNo"] = 1  # 페이지 번호 초기화
    while True:
        try:
            response = s.post(u, json=conditions[condition])
            response.raise_for_status()
            r = response.json()
            totalCount = r.get("TotalCount", 0)
            pageSize = r.get("PageSize", 0)
            cnt += pageSize

            for item in r.get("Items", []):
                auctionInfo = item.get("AuctionInfo", {})
                options = item.get("Options", [])

                optionInfos = []
                for option in options:
                    if option.get("Type") == "ACCESSORY_UPGRADE":
                        optionName = option.get("OptionName", "Unknown Option")
                        value = option.get("Value", 0)
                        optionInfos.append(f'{optionName} - {value}%')

                optionInfo = '\n'.join(optionInfos)
                price = auctionInfo.get("BuyPrice")
                tradeAllowCount = auctionInfo.get("TradeAllowCount")
                gradeQuality = item.get("GradeQuality")
                itemName = item.get("Name")
                icon = item.get("Icon")
                endDate = auctionInfo.get("EndDate")

                # price 또는 endDate가 None이면 건너뜀
                if price is None or endDate is None:
                    continue

                # 데이터베이스에 삽입
                cursor.execute('''
                INSERT INTO items (condition_name, itemName, optionInfo, endDate, price, tradeAllowCount, gradeQuality, icon)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (condition, itemName, optionInfo, endDate, price, tradeAllowCount, gradeQuality, icon))
                conn.commit()

            if cnt < totalCount:
                conditions[condition]["PageNo"] += 1
                continue
            else:
                break
        except Exception as e:
            log(f"[x] search error.. - {condition}")
            log(traceback.format_exc())
            break  # 에러 발생 시 루프 종료

# 크롤링 후 각 조건에 대해 처리
for condition in conditions:
    # 현재 KST 시간
    current_time = datetime.now(KST)
    # 해당 조건의 아이템 가져오기
    cursor.execute('''
    SELECT price, endDate, itemName, optionInfo, tradeAllowCount, gradeQuality, icon
    FROM items WHERE condition_name = ?
    ''', (condition,))
    rows = cursor.fetchall()

    items = []
    for row in rows:
        price, endDate, itemName, optionInfo, tradeAllowCount, gradeQuality, icon = row
        # price가 None이면 건너뜀
        if price is None:
            continue
        endDate_datetime = parse_endDate(endDate)
        if endDate_datetime and endDate_datetime > current_time:
            items.append({
                'price': price,
                'endDate': endDate,
                'itemName': itemName,
                'optionInfo': optionInfo,
                'tradeAllowCount': tradeAllowCount,
                'gradeQuality': gradeQuality,
                'icon': icon
            })

    if items:
        # 가격이 숫자인 아이템만 필터링
        prices = [item['price'] for item in items if isinstance(item['price'], (int, float))]

        if prices:
            current_lowest_price = min(prices)
            # 최저가 아이템 가져오기
            lowest_price_item = next(item for item in items if item['price'] == current_lowest_price)
            # 이전 최저가 가져오기
            cursor.execute('SELECT lowest_price FROM lowest_prices WHERE condition_name = ?', (condition,))
            result = cursor.fetchone()
            if result:
                previous_lowest_price = result[0]
            else:
                previous_lowest_price = None

            # 가격 비교
            if previous_lowest_price is None or current_lowest_price < previous_lowest_price:
                # Discord 웹훅으로 메시지 전송
                send_discord_message(condition, previous_lowest_price, current_lowest_price, lowest_price_item)
                # 최저가 업데이트
                cursor.execute('''
                INSERT OR REPLACE INTO lowest_prices (condition_name, lowest_price)
                VALUES (?, ?)
                ''', (condition, current_lowest_price))
                conn.commit()
        else:
            print(f"No valid numeric prices found for condition {condition}")
    else:
        print(f"No valid items found for condition {condition}")

# 데이터베이스 연결 종료
conn.close()
