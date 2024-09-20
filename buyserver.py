from flask import Flask, request, jsonify
from functools import wraps
import os
import binascii
import requests
import re
import json
from bs4 import BeautifulSoup
import urllib.parse
from datetime import datetime, timedelta


def normalize_path(path):
    # 먼저 모든 구분자를 표준 유닉스 형식(/)으로 변환
    normalized = path.replace('\\', '/')
    
    # os.path.normpath를 사용하여 현재 OS에 맞는 형식으로 변환
    return os.path.normpath(normalized)


app = Flask(__name__)

# 랜덤 시크릿 키 생성
SECRET_KEY = binascii.hexlify(os.urandom(64)).decode()

def buy(itemno,price):
    with open("config.json","rb") as f:
        config = json.loads(f.read())

    secondpass = config["secondpass"]
    s = requests.session()
    s.headers = config["headers"]
    s.verify=False

    u = "https://lostark.game.onstove.com/SecondPassword/GetSecondPasswordForm?type=auction&status=1"
    r = s.get(u)

    html_content = r.content.decode()

    # Parse the HTML content
    soup = BeautifulSoup(html_content, 'html.parser')

    # Extract the keypad buttons
    buttons = soup.find_all('button', {'name': 'btnRandompad'})

    # Create the mapping
    displayed_to_value = {}
    for button in buttons:
        displayed_digit = button.get_text()
        value = button['value']
        displayed_to_value[displayed_digit] = value

    # Your actual password
    actual_password = secondpass

    # Generate the encrypted password
    encrypted_password = ''
    for digit in actual_password:
        encrypted_password += displayed_to_value[digit]

    print('Encrypted Password:', encrypted_password)

    # Extract the randompadkey
    randompadkey_button = soup.find('button', {'class': 'button--password-confirm'})
    randompadkey = randompadkey_button['data-randompadkey']

    print('RandomPadKey:', randompadkey)

    u = "https://lostark.game.onstove.com/Auction/SetAuctionBuy"
    data = f"productId={itemno}&worldId=1&pcName=%EC%86%8C%EC%84%9C%EB%9F%AC%EB%8B%AC%EC%9D%B4&price={int(float(price))}&pheon=0&password={encrypted_password}%7C{randompadkey}"
    s.headers["Content-Type"] = "application/x-www-form-urlencoded"

    r = s.post(u, data=data)
    return r.content

@app.route('/buy', methods=['GET'])
def execute_buy():
    itemno = request.args.get('itemno')
    price = request.args.get('price')

    if not itemno or not price:
        return jsonify({"error": "Missing itemno or price"}), 400

    try:
        # buy 함수 실행
        return buy(itemno, price), 200
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=50000)