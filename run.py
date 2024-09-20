import requests
import json
import traceback
import sqlite3
import hashlib
from datetime import datetime, timedelta, timezone
import warnings

# InsecureRequestWarning 경고 무시
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

with open("/home/arang/web/lostark/lostark-auction-noti/config.json", "rb") as f:
    config = json.loads(f.read())

token = config["token"]
webhook_url = config["webhook_url"]
webhook_url2 = config["webhook_url2"]  # 최저가 알림을 위한 웹훅 URL

# KST 시간대 정의
KST = timezone(timedelta(hours=9))

def parse_endDate(endDate_str):
    try:
        dt = datetime.strptime(endDate_str, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        try:
            dt = datetime.strptime(endDate_str, '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            print(f"[parse_endDate] 종료 시간 파싱 실패: {endDate_str}")
            return None
    # 서버에서 받은 시간에 KST 시간대를 설정
    dt = dt.replace(tzinfo=KST)
    return dt

def generate_item_id(item):
    # 아이템의 고유 ID 생성 (아이템 이름, 옵션, 가격, 종료 시간 등을 조합)
    unique_string = f"{item['itemName']}_{item['optionInfo']}_{item['price']}_{item['endDate']}"
    return hashlib.md5(unique_string.encode('utf-8')).hexdigest()

def send_discord_message(condition_name, item_details, lowest_price, is_lowest_price=False):
    global webhook_url, webhook_url2
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

    current_price = item_details['price']

    # 가격 차이 계산
    price_difference = current_price - lowest_price
    price_difference_percentage = (price_difference / lowest_price) * 100 if lowest_price != 0 else 0
    price_difference_display = f"{price_difference} ({price_difference_percentage:.2f}%)"

    # 메시지 제목 및 색상 설정
    if is_lowest_price:
        title = f"[{condition_name}] 🏆 최저가 갱신!"
        description = f"현재 최저가: {current_price}"
        color = 0xff0000  # 빨간색으로 강조
        webhook_to_use = webhook_url2  # 최저가 알림은 webhook_url2 사용
    else:
        title = f"[{condition_name}] 새로운 아이템 등록"
        description = f"가격: {current_price} (최저가 대비 {price_difference_display} 차이)"
        color = 0x00ff00  # 녹색
        webhook_to_use = webhook_url  # 신규 아이템 알림은 기존 webhook_url 사용

    # Discord 임베드 메시지 구성
    embed = {
        "title": title,
        "description": description,
        "color": color,
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
    try:
        response = requests.post(webhook_to_use, data=json.dumps(data), headers=headers, verify=False)
        if response.status_code == 204 or response.status_code == 200:
            print(f"[{current_time}] {condition_name} - 메시지가 성공적으로 전송되었습니다.")
        else:
            print(f"메시지 전송 실패. 상태 코드: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[send_discord_message] 메시지 전송 중 오류 발생: {e}")
        traceback.print_exc()

def log(message):
    print(message)
    # 필요 시 로그를 파일에 저장하거나 추가적인 처리 가능

u = "https://developer-lostark.game.onstove.com/auctions/items"
s = requests.session()
s.verify = False
s.headers = {
    "Content-Type": "application/json",
    "Authorization": f"bearer {token}"
}

with open("/home/arang/web/lostark/lostark-auction-noti/conditions.json", "rb") as f:
    try:
        conditions = json.loads(f.read())
    except Exception as e:
        print(f"[x] condition load error: {e}")
        log(traceback.format_exc())
        exit(1)

print(f"[+] {datetime.now(KST)} - 조건 로드 성공")

# 데이터베이스 설정
try:
    conn = sqlite3.connect('/home/arang/web/lostark/lostark-auction-noti/items.db')  # 절대 경로로 변경
    cursor = conn.cursor()
except Exception as e:
    print(f"[x] 데이터베이스 연결 실패: {e}")
    log(traceback.format_exc())
    exit(1)

# 테이블 생성
try:
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

    # 이미 알림을 보낸 아이템을 추적하기 위한 테이블 생성
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS notified_items (
        item_id TEXT PRIMARY KEY
    )
    ''')

    conn.commit()
    print("[+] 데이터베이스 테이블 확인 완료")
except Exception as e:
    print(f"[x] 테이블 생성 중 오류 발생: {e}")
    log(traceback.format_exc())
    conn.close()
    exit(1)

# items 테이블 초기화
try:
    cursor.execute('DELETE FROM items')
    conn.commit()
    print("[+] items 테이블 초기화 완료")
except Exception as e:
    print(f"[x] items 테이블 초기화 중 오류 발생: {e}")
    log(traceback.format_exc())
    conn.close()
    exit(1)

for condition in conditions:
    print(f"[{datetime.now(KST)}] '{condition}' 조건에 대한 데이터 수집 시작")
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

            items_list = r.get("Items", [])
            if not items_list:
                print(f"[{condition}] 아이템이 없습니다.")
                break

            for item in items_list:
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
                try:
                    cursor.execute('''
                    INSERT INTO items (condition_name, itemName, optionInfo, endDate, price, tradeAllowCount, gradeQuality, icon)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (condition, itemName, optionInfo, endDate, price, tradeAllowCount, gradeQuality, icon))
                    conn.commit()
                except Exception as e:
                    print(f"[x] 데이터베이스 삽입 중 오류 발생: {e}")
                    log(traceback.format_exc())

            if cnt < totalCount:
                conditions[condition]["PageNo"] += 1
                continue
            else:
                break
        except Exception as e:
            print(f"[x] 검색 오류 발생 - '{condition}': {e}")
            log(traceback.format_exc())
            break  # 에러 발생 시 루프 종료

print("[+] 데이터 수집 완료")

# 크롤링 후 각 조건에 대해 처리
for condition in conditions:
    print(f"[{datetime.now(KST)}] '{condition}' 조건에 대한 아이템 처리 시작")
    # 현재 KST 시간
    current_time = datetime.now(KST)
    # 해당 조건의 아이템 가져오기
    try:
        cursor.execute('''
        SELECT price, endDate, itemName, optionInfo, tradeAllowCount, gradeQuality, icon
        FROM items WHERE condition_name = ?
        ''', (condition,))
        rows = cursor.fetchall()
    except Exception as e:
        print(f"[x] 데이터베이스 조회 중 오류 발생: {e}")
        log(traceback.format_exc())
        continue

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
        else:
            # 경매가 종료된 경우 건너뜀
            continue

    if items:
        # 가격이 숫자인 아이템만 필터링
        prices = [item['price'] for item in items if isinstance(item['price'], (int, float))]

        if prices:
            current_lowest_price = min(prices)
            # 가격 차이가 20% 이하인 아이템만 필터링
            threshold_price = current_lowest_price * 1.2
            filtered_items = [item for item in items if item['price'] <= threshold_price]

            # 최저가 아이템 가져오기
            lowest_price_item = next(item for item in filtered_items if item['price'] == current_lowest_price)
            # 이전 최저가 가져오기
            cursor.execute('SELECT lowest_price FROM lowest_prices WHERE condition_name = ?', (condition,))
            result = cursor.fetchone()
            if result:
                previous_lowest_price = result[0]
            else:
                previous_lowest_price = None

            # 가격 비교
            if previous_lowest_price is None or current_lowest_price < previous_lowest_price:
                # 최저가 아이템에 대해 알림 전송 (webhook_url2 사용)
                send_discord_message(condition, lowest_price_item, current_lowest_price, is_lowest_price=True)
                # 최저가 업데이트
                try:
                    cursor.execute('''
                    INSERT OR REPLACE INTO lowest_prices (condition_name, lowest_price)
                    VALUES (?, ?)
                    ''', (condition, current_lowest_price))
                    conn.commit()
                except Exception as e:
                    print(f"[x] 최저가 업데이트 중 오류 발생: {e}")
                    log(traceback.format_exc())
            else:
                pass  # 최저가 변동 없음

            # 모든 아이템에 대해 알림 전송 (이미 알림을 보낸 아이템은 제외, webhook_url 사용)
            for item in filtered_items:
                item_id = generate_item_id(item)
                cursor.execute('SELECT item_id FROM notified_items WHERE item_id = ?', (item_id,))
                if cursor.fetchone():
                    # 이미 알림을 보낸 아이템
                    continue
                # 아이템에 대한 알림 전송
                send_discord_message(condition, item, current_lowest_price, is_lowest_price=False)
                # 알림 보낸 아이템 기록
                cursor.execute('INSERT INTO notified_items (item_id) VALUES (?)', (item_id,))
                conn.commit()
        else:
            print(f"[{datetime.now(KST)}] '{condition}' 유효한 가격 정보가 있는 아이템이 없습니다.")
    else:
        print(f"[{datetime.now(KST)}] '{condition}' 유효한 아이템을 찾을 수 없습니다.")

# 데이터베이스 연결 종료
conn.close()
print(f"[{datetime.now(KST)}] 프로그램 실행 완료")

