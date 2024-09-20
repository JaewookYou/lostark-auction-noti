import requests
import json
import traceback
import sqlite3
import hashlib
import os
import re
import urllib.parse
import warnings
from datetime import datetime, timedelta, timezone
from collections import Counter
from bs4 import BeautifulSoup

# InsecureRequestWarning 경고 무시
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

current_file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file_path)

def flatten_dict(d, parent_key=''):
    items = []
    if isinstance(d, list):
        for index, value in enumerate(d):
            new_key = f"{parent_key}[{index}]"
            if isinstance(value, (dict, list)):
                items.extend(flatten_dict(value, new_key))
            else:
                items.append((new_key, value))
    elif isinstance(d, dict):
        for key, value in d.items():
            if parent_key:
                new_key = f"{parent_key}[{key}]"
            else:
                new_key = key
            if isinstance(value, (dict, list)):
                items.extend(flatten_dict(value, new_key))
            else:
                items.append((new_key, value))
    else:
        items.append((parent_key, d))
    return items

def generate_query_params(data):
    flat_items = flatten_dict(data)
    return urllib.parse.urlencode(flat_items, doseq=True)

def normalize_path(path):
    # 먼저 모든 구분자를 표준 유닉스 형식(/)으로 변환
    normalized = path.replace('\\', '/')
    
    # os.path.normpath를 사용하여 현재 OS에 맞는 형식으로 변환
    return os.path.normpath(normalized)

with open(normalize_path(f"{current_dir}/config.json"), "rb") as f:
    config = json.loads(f.read())

token = config["token"]
webhook_url = config["webhook_url"]
webhook_url2 = config["webhook_url2"]  # 최저가 알림을 위한 웹훅 URL
secondpass = config.get("secondpass", "")  # Optional field

# KST 시간대 정의
KST = timezone(timedelta(hours=9))

def clean_html(raw_html):
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext.strip()

def parse_time(time_str):
    if '시간' in time_str and '분' in time_str:
        hours, minutes = re.search(r'(\d+)시간\s*(\d+)분', time_str).groups()
        return timedelta(hours=int(hours), minutes=int(minutes))
    elif '시간' in time_str:
        hours = re.search(r'(\d+)시간', time_str).group(1)
        return timedelta(hours=int(hours))
    elif '분' in time_str:
        minutes = re.search(r'(\d+)분', time_str).group(1)
        return timedelta(minutes=int(minutes))
    return timedelta()

def parse_trade_count(text):
    if '불가' in text:
        return 0
    match = re.search(r'(\d+)회', text)
    if match:
        return int(match.group(1))
    return 0  # 기본값으로 0 반환

def parse_auction_items(html):
    soup = BeautifulSoup(html, 'html.parser')
    items = []
    for row in soup.select('table.auctionListTable tbody tr'):
        item = {}
        
        # 아이템 이름
        item['Name'] = row.select_one('span.name').text.strip()
        
        # 등급, 티어, 레벨, 아이콘
        button = row.select_one('button.button--deal-history')
        item['Grade'] = button['data-grade']
        item['Tier'] = int(button['data-tier'])
        item['Level'] = int(button['data-itemlevel'])
        item['Icon'] = button['data-itempath']
        
        # 품질
        quality = row.select_one('div.quality span.txt')
        if quality:
            item['GradeQuality'] = int(quality.text)
        
        # 경매 정보
        item['AuctionInfo'] = {}
        price_row = row.select_one('div.price-row')
        buy_price = row.select_one('div.price-buy em')
        
        if price_row:
            start_price = int(price_row.select_one('em').text.replace(',', ''))
            current_bid = int(price_row.select_one('span.tooltip em').text.replace(',', ''))
            item['AuctionInfo']['StartPrice'] = start_price
            item['AuctionInfo']['BidPrice'] = current_bid
            item['AuctionInfo']['BidStartPrice'] = start_price
        
        if buy_price:
            item['AuctionInfo']['BuyPrice'] = int(buy_price.text.replace(',', ''))
        
        # 남은 시간
        time_left = row.select_one('div.time')
        if time_left:
            item['AuctionInfo']['EndDate'] = (datetime.now() + parse_time(time_left.text.strip())).isoformat()
        
        # 거래 가능 횟수
        trade_count_elem = row.select_one('span.count')
        if trade_count_elem:
            trade_count_text = trade_count_elem.text.strip()
            item['AuctionInfo']['TradeAllowCount'] = parse_trade_count(trade_count_text)
        else:
            item['AuctionInfo']['TradeAllowCount'] = 0  # 정보가 없는 경우 기본값 0
        
        # 아이템 옵션
        option_json = button['data-optionjson']
        options = json.loads(option_json)
        item['Options'] = []
        for option in options:
            option_name = clean_html(option['secondOptionText'])
            option_value = option['optionValue']
            option_type = 'ARK_PASSIVE' if option['optionType'] == 8 else 'ACCESSORY_UPGRADE'
            item['Options'].append({
                'Type': option_type,
                'OptionName': option_name,
                'OptionNameTripod': '',
                'Value': float(option_value),
                'IsPenalty': False,
                'ClassName': None,
                'IsValuePercentage': option_type == 'ACCESSORY_UPGRADE'
            })
        
        # 아이템 고유 번호
        product_id = row.select_one('button.button--deal-buy')['data-productid']
        item['ProductId'] = product_id
        
        items.append(item)

    return items

def search_item(infos):
    u = "https://lostark.game.onstove.com/Auction"
    r = requests.post(u,data=infos,headers=config["headers"],verify=False)
    return r.content.decode()

def items_match(item1, item2, time_tolerance=timedelta(minutes=3)):
    # Compare basic attributes
    if item1['itemName'] != item2['Name']:
        print(f"Item name mismatch: {item1['itemName']} != {item2['Name']}")
        return False
    if item1.get('gradeQuality') != item2.get('GradeQuality'):
        print(f"Grade quality mismatch: {item1.get('gradeQuality')} != {item2.get('GradeQuality')}")
        return False
    if item1.get('buyPrice') != item2["AuctionInfo"]['BuyPrice']:
        print(f"Buy price mismatch: {item1.get('buyPrice')} != {item2['AuctionInfo']['BuyPrice']}")
        return False
    if item1.get('tradeAllowCount') != item2["AuctionInfo"]['TradeAllowCount']:
        print(f"Trade allow count mismatch: {item1.get('tradeAllowCount')} != {item2['AuctionInfo']['TradeAllowCount']}")
        return False
        
    # Compare auction prices
    if item1.get('startPrice') != item2["AuctionInfo"]['StartPrice']:
        print(f"Start price mismatch: {item1.get('startPrice')} != {item2['AuctionInfo']['StartPrice']}")
        return False
    if item1.get('bidPrice') != item2["AuctionInfo"]['BidPrice']:
        print(f"Bid price mismatch: {item1.get('bidPrice')} != {item2['AuctionInfo']['BidPrice']}")
        return False
    
    # Compare end dates with tolerance
    endDate1 = parse_endDate(item1['endDate'])
    endDate2 = parse_endDate(item2["AuctionInfo"]['EndDate'])
    if endDate1 and endDate2:
        if abs(endDate1 - endDate2) > time_tolerance:
            print(f"End date mismatch beyond tolerance: {endDate1} != {endDate2}")
            return False
    else:
        print("Cannot compare items without valid end dates")
        return False  # Cannot compare items without valid end dates
    
    # Compare options
    options1 = Counter(x.replace("%","").strip() for x in item1['optionInfo'].split('\n') if x.strip())
    options2 = Counter(f"{opt['OptionName']} - {opt['Value']}".strip() for opt in item2['Options'])
    
    if options1 != options2:
        print("Options mismatch:")
        print("Item1 options:", dict(options1))
        print("Item2 options:", dict(options2))
        
        # 세부적인 차이점 출력
        diff1 = options1 - options2
        diff2 = options2 - options1
        if diff1:
            print("Options in Item1 but not in Item2:", dict(diff1))
        if diff2:
            print("Options in Item2 but not in Item1:", dict(diff2))
        
        return False

    # All checks passed
    return True

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
    dt = dt.replace(tzinfo=timezone.utc).astimezone(KST)
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

    # 옵션 정보 구성
    option_info = item_details['optionInfo']

    # 구매 링크 생성
    product_id = item_details.get('ProductId', 'N/A')
    buy_link = f"http://43.201.250.186:50000/buy?itemno={product_id}&price={current_price}"

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
                "value": option_info,
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
            },
            {
                "name": "구매하기",
                "value": f"[여기를 클릭하여 구매하기]({buy_link})",
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

# 테이블에 컬럼이 있는지 확인하고 없으면 추가
def add_column_if_not_exists(cursor, table_name, column_name, column_type):
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [column[1] for column in cursor.fetchall()]
    if column_name not in columns:
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
        print(f"[+] '{column_name}' 컬럼이 '{table_name}' 테이블에 추가되었습니다.")

s = requests.session()
s.verify = False
s.headers = {
    "Content-Type": "application/json",
    "Authorization": f"bearer {token}"
}

with open(normalize_path(f"{current_dir}/conditions.json"), "rb") as f:
    try:
        conditions = json.loads(f.read())
    except Exception as e:
        print(f"[x] condition load error: {e}")
        log(traceback.format_exc())
        exit(1)

print(f"[+] {datetime.now(KST)} - 조건 로드 성공")

# 데이터베이스 설정
try:
    conn = sqlite3.connect(normalize_path(f'{current_dir}/items.db'))  # 절대 경로로 변경
    cursor = conn.cursor()
except Exception as e:
    print(f"[x] 데이터베이스 연결 실패: {e}")
    log(traceback.format_exc())
    exit(1)

# 테이블 생성 및 'infos' 컬럼 추가 확인
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

    # 테이블 수정
    add_column_if_not_exists(cursor, 'items', 'infos', 'TEXT')
    add_column_if_not_exists(cursor, 'items', 'startPrice', 'REAL')
    add_column_if_not_exists(cursor, 'items', 'bidPrice', 'REAL')
    add_column_if_not_exists(cursor, 'items', 'buyPrice', 'REAL')

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
    cnt = 0
    conditions[condition]["PageNo"] = 1  # 페이지 번호 초기화
    while True:
        try:
            response = s.post("https://developer-lostark.game.onstove.com/auctions/items", json=conditions[condition])
            response.raise_for_status()
            r = response.json()
            totalCount = r.get("TotalCount", 0)
            pageSize = r.get("PageSize", 0)
            print(f"[{datetime.now(KST)}] '{condition}' 조건에 대한 데이터 수집 시작({cnt}/{totalCount})")
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
                    optionName = option.get("OptionName", "Unknown Option").strip()
                    value = option.get("Value", 0)
                    if option.get("IsValuePercentage", False):
                        value_str = f"{value}%"
                    else:
                        value_str = str(value)
                    optionInfos.append(f'{optionName} - {value_str}')

                optionInfo = '\n'.join(optionInfos)
                price = auctionInfo.get("BuyPrice")
                tradeAllowCount = auctionInfo.get("TradeAllowCount")
                gradeQuality = item.get("GradeQuality")
                itemName = item.get("Name")
                icon = item.get("Icon")
                endDate = auctionInfo.get("EndDate")

                buy_options = []
                for option in conditions[condition]["EtcOptions"]:
                    t = {}
                    t["firstOption"] = option["FirstOption"]
                    t["secondOption"] = option["SecondOption"]
                    t["minValue"] = option["MinValue"]
                    t["maxValue"] = option["MaxValue"]
                    buy_options.append(t)

                skill_options = []
                for option in conditions[condition]["SkillOptions"]:
                    t = {}
                    t["firstOption"] = option["FirstOption"]
                    t["secondOption"] = option["SecondOption"]
                    t["minValue"] = option["MinValue"]
                    t["maxValue"] = option["MaxValue"]
                    skill_options.append(t)

                # infos dictionary를 json dump해서 db에 삽입
                infos = {
                    "firstCategory": conditions[condition]["FirstCategory"],
                    "secondCategory": conditions[condition]["CategoryCode"],
                    "classNo": "",
                    "itemTier": conditions[condition]["ItemTier"],
                    "itemGrade": conditions[condition]["ItemGrade2"],
                    "itemLevelMin": 0,
                    "itemLevelMax": 1800,
                    "itemName": itemName,
                    "pageNo": conditions[condition]["PageNo"],
                    "sortOption": {
                        "Sort": "BUY_PRICE",
                        "IsDesc": False
                    },
                    "gradeQuality": gradeQuality,
                    "skillOptionList": skill_options,
                    "etcOptionList": buy_options
                }
                infos_json = json.dumps(infos)

                # price 또는 endDate가 None이면 건너뜀
                if price is None or endDate is None:
                    continue

                # 데이터베이스에 삽입
                try:
                    cursor.execute('''
                    INSERT INTO items (condition_name, itemName, optionInfo, endDate, price, tradeAllowCount, gradeQuality, icon, infos, startPrice, bidPrice, buyPrice)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (condition, itemName, optionInfo, endDate, price, tradeAllowCount, gradeQuality, icon, infos_json, auctionInfo.get('StartPrice'), auctionInfo.get('BidPrice'), auctionInfo.get('BuyPrice')))
                    conn.commit()
                except Exception as e:
                    print(f"[x] 데이터베이스 삽입 중 오류 발생: {e}")
                    log(traceback.format_exc())

            if cnt < totalCount and cnt <= 30:
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
        SELECT price, endDate, itemName, optionInfo, tradeAllowCount, gradeQuality, icon, infos, startPrice, bidPrice, buyPrice
        FROM items WHERE condition_name = ?
        ''', (condition,))

        rows = cursor.fetchall()
    except Exception as e:
        print(f"[x] 데이터베이스 조회 중 오류 발생: {e}")
        log(traceback.format_exc())
        continue

    items = []
    for row in rows:
        price, endDate, itemName, optionInfo, tradeAllowCount, gradeQuality, icon, infos_json, startPrice, bidPrice, buyPrice = row
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
                'icon': icon,
                'infos': infos_json,
                'startPrice': startPrice,
                'bidPrice': bidPrice,
                'buyPrice': buyPrice
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
                # 최저가일 경우 디비에 넣어놨던 json dumps (infos) 꺼내와서 search_item의 인자로 넘기기
                infos_json = lowest_price_item.get('infos')
                if infos_json:
                    infos = json.loads(infos_json)
                    
                    # search_item의 인자로 infos를 넘겨서 검색 수행
                    search_result = search_item(generate_query_params(infos))
                    result_items = parse_auction_items(search_result)

                    # 최저가 아이템에 해당하는 정보를 찾기
                    matched_product_id = None
                    
                    for item in result_items:
                        if items_match(lowest_price_item, item):
                            matched_product_id = item['ProductId']
                            print(f"[+] Matched Product ID: {matched_product_id}")
                            # 최저가 아이템에 ProductId 추가
                            lowest_price_item['ProductId'] = matched_product_id
                            break
                    if matched_product_id:
                        # 최저가 아이템에 대한 알림 전송 (webhook_url2 사용)
                        send_discord_message(condition, lowest_price_item, current_lowest_price, is_lowest_price=True)
                    else:
                        print("[!] Matching item not found in result_items.")
                else:
                    print("[x] 'infos' 데이터가 없습니다.")

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

        else:
            print(f"[{datetime.now(KST)}] '{condition}' 유효한 가격 정보가 있는 아이템이 없습니다.")
    else:
        print(f"[{datetime.now(KST)}] '{condition}' 유효한 아이템을 찾을 수 없습니다.")

# 데이터베이스 연결 종료
conn.close()
print(f"[{datetime.now(KST)}] 프로그램 실행 완료")
